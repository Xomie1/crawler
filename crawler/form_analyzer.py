# -*- coding: utf-8 -*-
"""
Form Analyzer Module
Analyzes form DOM structure and generates submission strategies
"""

import re
import logging
import json
from typing import Dict, List, Optional, Tuple, Set
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


class FormField:
    """Represents a form field."""
    
    def __init__(self, name: str, field_type: str, required: bool = False, 
                 placeholder: str = None, selector: str = None):
        self.name = name
        self.field_type = field_type  # text, email, textarea, select, hidden, etc.
        self.required = required
        self.placeholder = placeholder
        self.selector = selector
        self.value = None
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'type': self.field_type,
            'required': self.required,
            'placeholder': self.placeholder,
            'selector': self.selector
        }


class FormAnalysis:
    """Represents complete analysis of a form."""
    
    def __init__(self, url: str, form_html: str):
        self.url = url
        self.form_html = form_html
        
        # Form properties
        self.form_action = None
        self.form_method = 'POST'  # default
        self.form_id = None
        self.form_class = None
        self.form_name = None
        
        # Fields
        self.fields: List[FormField] = []
        self.field_map: Dict[str, FormField] = {}  # name -> field
        
        # CAPTCHA detection
        self.has_captcha = False
        self.captcha_type = None  # 'recaptcha_v2', 'recaptcha_v3', 'hcaptcha', etc.
        
        # Form submission type
        self.submission_type = 'standard'  # 'standard', 'ajax', 'javascript'
        self.is_ajax = False
        
        # Hidden fields
        self.hidden_fields: Dict[str, str] = {}
        
        # Confidence score
        self.confidence = 0.0
        
        # Raw form element
        self.form_element = None
        
        # Issues
        self.issues: List[str] = []
    
    def to_dict(self) -> Dict:
        return {
            'url': self.url,
            'form_action': self.form_action,
            'form_method': self.form_method,
            'form_id': self.form_id,
            'fields': [f.to_dict() for f in self.fields],
            'has_captcha': self.has_captcha,
            'captcha_type': self.captcha_type,
            'submission_type': self.submission_type,
            'is_ajax': self.is_ajax,
            'hidden_fields_count': len(self.hidden_fields),
            'confidence': self.confidence,
            'issues': self.issues
        }


class FormAnalyzer:
    """Analyzes forms in HTML and generates submission strategies."""
    
    # CAPTCHA detection patterns
    RECAPTCHA_V2_PATTERNS = [
        r'recaptcha.*v2',
        r'g-recaptcha(?!-responsive)',
        r'grecaptcha\.render',
    ]
    
    RECAPTCHA_V3_PATTERNS = [
        r'google\.recaptcha\.execute',  # v3 specific method
        r'grecaptcha\.execute\(',  # v3 execute
        r'recaptcha/api\.js\?render=',  # v3 script with render
        r'recaptcha.*v3',  # Explicit version
        r'action:\s*[\'"][\w\-\.]+[\'"]',  # Action parameter (v3 specific)
        r'google\.recaptcha(?!.*v2)',  # Google recaptcha not v2
    ]
    
    HCAPTCHA_PATTERNS = [
        r'hcaptcha',
        r'h-captcha',
    ]
    
    # Field type patterns
    FIELD_PATTERNS = {
        'email': [
            r'email', r'mail', r'e-mail', r'メール',
            r'ｅメール', r'contact.*email'
        ],
        'name': [
            r'name', r'名前', r'なまえ', r'fullname', r'full.?name',
            r'お名前', r'contact.*name'
        ],
        'company': [
            r'company', r'会社', r'企業', r'organization',
            r'事業者', r'organization.*name'
        ],
        'phone': [
            r'phone', r'tel', r'telephone', r'mobile', r'電話',
            r'TEL', r'携帯'
        ],
        'message': [
            r'message', r'content', r'inquiry', r'question', r'comment',
            r'メッセージ', r'内容', r'お問い合わせ', r'詳細'
        ],
        'subject': [
            r'subject', r'title', r'件名', r'表題', r'topic'
        ],
    }
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.parsed_base = urlparse(base_url)
    
    def analyze(self, form_url: str, html_content: str) -> Optional[FormAnalysis]:
        """
        Analyze a form in HTML content.
        
        Args:
            form_url: URL where form was found
            html_content: HTML content containing the form
            
        Returns:
            FormAnalysis object or None if no form found
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            form = soup.find('form')
            
            if not form:
                logger.warning(f"No form found via HTML parsing on {form_url}")
                
                # Try browser fallback for JavaScript-rendered forms
                try:
                    from crawler.submit_form.browser_form_submitter import BrowserFormSubmitter
                    logger.info("Attempting to find form via browser...")
                    
                    browser = BrowserFormSubmitter(timeout=self.timeout)
                    browser._ensure_browser()
                    
                    page = browser._context.new_page()
                    page.goto(form_url, wait_until='networkidle', timeout=self.timeout * 1000)
                    time.sleep(1)
                    
                    browser_html = page.content()
                    page.close()
                    
                    soup_browser = BeautifulSoup(browser_html, 'html.parser')
                    form = soup_browser.find('form')
                    
                    if form:
                        logger.info("✅ Form found via browser rendering")
                        html_content = browser_html
                    else:
                        logger.error(f"No form found on {form_url} (even with browser)")
                        return None
                except ImportError:
                    logger.warning("Browser not available for form detection")
                    return None
                except Exception as e:
                    logger.warning(f"Browser detection failed: {e}")
                    return None

            
            analysis = FormAnalysis(form_url, str(form))
            analysis.form_element = form
            
            # Extract form properties
            self._extract_form_properties(form, analysis)
            
            # Extract fields
            self._extract_fields(form, analysis)
            
            # Detect CAPTCHA
            self._detect_captcha(html_content, analysis)
            
            # Detect submission type
            self._detect_submission_type(form, html_content, analysis)
            
            # Calculate confidence
            self._calculate_confidence(analysis)
            
            logger.info(f"Form analysis complete: {analysis.confidence:.2f} confidence")
            logger.info(f"  - Fields: {len(analysis.fields)}")
            logger.info(f"  - CAPTCHA: {analysis.captcha_type or 'None'}")
            logger.info(f"  - Submission: {analysis.submission_type}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing form: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _extract_form_properties(self, form, analysis: FormAnalysis):
        """Extract form tag properties."""
        analysis.form_action = form.get('action', '')
        
        # Make action absolute URL
        if analysis.form_action:
            analysis.form_action = urljoin(analysis.url, analysis.form_action)
        
        analysis.form_method = (form.get('method') or 'POST').upper()
        analysis.form_id = form.get('id', '')
        analysis.form_name = form.get('name', '')
        analysis.form_class = ' '.join(form.get('class', []))
        
        logger.debug(f"Form properties:")
        logger.debug(f"  - Action: {analysis.form_action}")
        logger.debug(f"  - Method: {analysis.form_method}")
        logger.debug(f"  - ID: {analysis.form_id}")
    
    def _extract_fields(self, form, analysis: FormAnalysis):
        """Extract all form fields."""
        inputs = form.find_all(['input', 'textarea', 'select'])
        
        for field_elem in inputs:
            field_type = field_elem.get('type', 'text').lower()
            field_name = field_elem.get('name', '')
            
            # Skip if no name
            if not field_name:
                continue
            
            # Skip submit/button/reset
            if field_type in ['submit', 'button', 'reset', 'hidden', 'image']:
                if field_type != 'hidden':
                    # Might be submit button - skip for now
                    logger.debug(f"Skipping {field_type} field: {field_name}")
                    continue
                else:
                    # Hidden field - store value
                    analysis.hidden_fields[field_name] = field_elem.get('value', '')
                    logger.debug(f"Found hidden field: {field_name}")
                    continue
            
            # Determine field purpose
            field_purpose = self._detect_field_purpose(field_elem)
            
            # Create field
            field = FormField(
                name=field_name,
                field_type=field_type if field_type in ['text', 'email', 'tel', 'url', 'number', 'textarea', 'select'] else 'text',
                required=field_elem.has_attr('required') or field_elem.has_attr('aria-required'),
                placeholder=field_elem.get('placeholder', ''),
                selector=self._generate_css_selector(field_elem)
            )
            
            analysis.fields.append(field)
            analysis.field_map[field_name] = field
            
            logger.debug(f"Field: {field_name} ({field_purpose}) - Required: {field.required}")
    
    def _detect_field_purpose(self, field_elem) -> str:
        """Detect what a field is for (email, name, message, etc.)."""
        field_name = field_elem.get('name', '').lower()
        field_id = field_elem.get('id', '').lower()
        field_label = field_elem.get('placeholder', '').lower()
        
        # Also check associated label
        parent = field_elem.parent
        label_text = ''
        if parent:
            label = parent.find('label')
            if label:
                label_text = label.get_text().lower()
        
        combined = f"{field_name} {field_id} {field_label} {label_text}"
        
        # Check patterns
        for purpose, patterns in self.FIELD_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, combined, re.IGNORECASE):
                    return purpose
        
        return 'unknown'
    
    def _generate_css_selector(self, elem) -> str:
        """Generate CSS selector for element."""
        if elem.get('id'):
            return f"#{elem.get('id')}"
        
        name = elem.get('name')
        if name:
            return f"input[name='{name}']"
        
        return str(elem.name)
    
    def _detect_captcha(self, html_content: str, analysis: FormAnalysis):
        """Detect CAPTCHA in page."""
        html_lower = html_content.lower()
        
        # Check for reCAPTCHA v2 (checkbox)
        if any(re.search(pattern, html_content, re.IGNORECASE) 
            for pattern in self.RECAPTCHA_V2_PATTERNS):
            analysis.has_captcha = True
            analysis.captcha_type = 'recaptcha_v2'
            analysis.issues.append('Has reCAPTCHA v2 - manual completion required')
            logger.warning("✅ Detected: reCAPTCHA v2")

        # Check for reCAPTCHA v3 (invisible)
        elif any(re.search(pattern, html_content, re.IGNORECASE) 
                for pattern in self.RECAPTCHA_V3_PATTERNS):
            analysis.has_captcha = True
            analysis.captcha_type = 'recaptcha_v3'
            analysis.issues.append('Has reCAPTCHA v3 - requires API key')
            logger.warning("✅ Detected: reCAPTCHA v3")
        
        # Check for hCaptcha
        elif any(re.search(pattern, html_content, re.IGNORECASE) 
                 for pattern in self.HCAPTCHA_PATTERNS):
            analysis.has_captcha = True
            analysis.captcha_type = 'hcaptcha'
            analysis.issues.append('Has hCaptcha - manual completion required')
            logger.warning("Detected hCaptcha")
        
        # Check for image CAPTCHA
        elif 'captcha' in html_lower and ('<img' in html_lower or '.jpg' in html_lower or '.png' in html_lower):
            analysis.has_captcha = True
            analysis.captcha_type = 'image'
            analysis.issues.append('Has image CAPTCHA - requires OCR')
            logger.warning("Detected image CAPTCHA")
    
    def _detect_submission_type(self, form, html_content: str, analysis: FormAnalysis):
        """Detect form submission type."""
        form_html_str = str(form).lower()
        
        # Check for AJAX
        if 'ajax' in form_html_str or 'submit' in form_html_str:
            if re.search(r'onclick|onsubmit|fetch|axios|xhr', form_html_str, re.IGNORECASE):
                analysis.is_ajax = True
                analysis.submission_type = 'ajax'
                logger.info("Detected AJAX submission")
        
        # Check for JavaScript
        if 'javascript' in form_html_str or re.search(r'onsubmit.*javascript:', form_html_str, re.IGNORECASE):
            analysis.submission_type = 'javascript'
            logger.info("Detected JavaScript submission")
        
        # Standard form submission (default)
        if analysis.submission_type == 'standard':
            logger.info("Using standard form submission (POST/GET)")
    
    def _calculate_confidence(self, analysis: FormAnalysis):
        """Calculate form analysis confidence score."""
        score = 0.0
        
        # Has form action (important)
        if analysis.form_action:
            score += 30
        else:
            score += 10  # Assume self-submission
        
        # Has required fields
        required_field_count = sum(1 for f in analysis.fields if f.required)
        if required_field_count > 0:
            score += min(20, required_field_count * 5)
        
        # Has key fields
        field_names = set(f.name.lower() for f in analysis.fields)
        if any(keyword in ' '.join(field_names) for keyword in ['email', 'mail', 'message', 'content']):
            score += 20
        
        # No CAPTCHA (positive)
        if not analysis.has_captcha:
            score += 20
        else:
            score -= 10  # CAPTCHA lowers confidence
        
        # Standard submission (easier)
        if analysis.submission_type == 'standard':
            score += 10
        elif analysis.submission_type == 'ajax':
            score += 5
        else:
            score -= 5
        
        # Has hidden fields (might be CSRF tokens)
        if analysis.hidden_fields:
            score += 10
        
        # Normalize to 0-1
        analysis.confidence = max(0.0, min(1.0, score / 100.0))