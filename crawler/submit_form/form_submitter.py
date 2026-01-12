# -*- coding: utf-8 -*-
"""
Complete Form Submitter - ALL Phase 3 Priorities in One File
Handles: Retry, CAPTCHA, Success Verification, AJAX, CSRF, Multi-step
"""

import re
import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import os
from crawler.submit_form.form_analyzer import FormAnalysis, FormAnalyzer
from crawler.submit_form.browser_form_submitter import BrowserFormSubmitter
from crawler.submit_form.form_data_validator import FormDataValidator


logger = logging.getLogger(__name__)


# ============================================================================
# PRIORITY 3: SUCCESS VERIFICATION
# ============================================================================

class SubmissionVerifier:
    """Enhanced verification of form submission success."""
    
    SUCCESS_KEYWORDS = [
        # English
        'thank you', 'thanks', 'success', 'successful', 'submitted',
        'received', 'sent', 'confirmed', 'completed', 'appreciate',
        # Japanese - Original
        'ありがとうございました', 'ありがとうございます', '完了', '送信完了',
        # Japanese - NEW
        '確認', '確認画面', '送信確認', '確認いたしました',
        '受け付け', '受け取り', '受信', '受け付けました',
        'お問い合わせ', 'ご送信', '送信する', 'ご送信ありがとう',
        '登録完了', 'ご登録ありがとう', '登録されました',
        '予約完了', 'ご予約ありがとう', '予約されました',
        '処理完了', '処理しました', '手続き完了',
        '申し込み', '申し込み完了', 'お申し込みありがとう',
        'メール送信', 'メッセージ送信', '送信しました',
    ]
    
    ERROR_KEYWORDS = [
        'error', 'fail', 'failed', 'invalid', 'incorrect', 'problem',
        'エラー', '失敗', '不正', '無効', '問題', '入力してください',
    ]
    
    VALIDATION_ERROR_PATTERNS = [
        r'required', r'必須.*入力', r'please\s+enter', r'invalid\s+email',
    ]
    
    SUCCESS_URL_PATTERNS = [
        r'/thanks?', r'/thank[-_]you', r'/success', r'/complete',
        r'/confirmation', r'/完了', r'/送信完了',
    ]
    
    def __init__(self, save_responses: bool = False, response_dir: str = "submission_responses"):
        self.save_responses = save_responses
        self.response_dir = response_dir
        
        if save_responses:
            os.makedirs(response_dir, exist_ok=True)
    
    def verify_submission(
        self,
        response: requests.Response,
        form_data: Dict,
        original_url: str
    ) -> Dict:
        """Comprehensive verification of submission success."""
        result = {
            'success': False,
            'confidence': 0.0,
            'indicators': [],
            'warnings': [],
        }
        
        try:
            # Get response content
            try:
                content = response.text
                content_lower = content.lower()
            except:
                content = response.content.decode('utf-8', errors='ignore')
                content_lower = content.lower()
            
            # Strategy 1: HTTP status
            if 200 <= response.status_code < 300:
                result['indicators'].append(f"HTTP {response.status_code}")
                result['confidence'] += 30
            elif 300 <= response.status_code < 400:
                result['indicators'].append(f"HTTP {response.status_code} (redirect)")
                result['confidence'] += 20
            else:
                result['indicators'].append(f"HTTP {response.status_code} (error)")
                result['confidence'] -= 40
            
            # Strategy 2: URL change
            if response.url != original_url:
                result['indicators'].append("URL changed")
                result['confidence'] += 10
                
                for pattern in self.SUCCESS_URL_PATTERNS:
                    if re.search(pattern, response.url, re.IGNORECASE):
                        result['indicators'].append(f"Success URL pattern: {pattern}")
                        result['confidence'] += 25
                        break
            
            # Strategy 3: Success keywords
            found_success = [kw for kw in self.SUCCESS_KEYWORDS if kw.lower() in content_lower]
            if found_success:
                result['confidence'] += min(40, len(found_success) * 10)
                result['indicators'].append(f"Success keywords: {', '.join(found_success[:3])}")
            
            # Strategy 4: Error keywords
            found_errors = [kw for kw in self.ERROR_KEYWORDS if kw.lower() in content_lower]
            if found_errors:
                result['confidence'] -= min(40, len(found_errors) * 10)
                result['warnings'].append(f"Error keywords: {', '.join(found_errors[:3])}")
            
            # Strategy 5: Validation errors
            for pattern in self.VALIDATION_ERROR_PATTERNS:
                if re.search(pattern, content, re.IGNORECASE):
                    result['confidence'] -= 30
                    result['warnings'].append("Validation error detected")
                    break
            
            # Strategy 6: Form still present
            soup = BeautifulSoup(content, 'html.parser')
            form_still_present = bool(soup.find('form'))

            if form_still_present:
                # Case 1: On success/thank-you page = OK
                if any(kw in content_lower for kw in ['thank', 'complete', 'success', 'confirm', '確認', '完了']):
                    result['indicators'].append("Form on confirmation page (expected)")
                    result['confidence'] += 10
                # Case 2: Still on same form page = BAD
                elif response.url == original_url:
                    result['confidence'] -= 25
                    result['warnings'].append("Form still present on same page")
                # Case 3: On error page = BAD
                elif 'error' in response.url.lower():
                    result['confidence'] -= 30
                    result['warnings'].append("Form on error page")
                else:
                    result['confidence'] -= 5
                    result['warnings'].append("Form still present")
            
            # Normalize confidence
            result['confidence'] = max(0.0, min(1.0, result['confidence'] / 100.0))

            # IMPROVED: Lower threshold from 0.4 to 0.30
            result['success'] = result['confidence'] >= 0.30

            # Save response if enabled
            if self.save_responses:
                self._save_response(response, original_url, result['success'])
            
            logger.info(f"Verification: {'✅ SUCCESS' if result['success'] else '❌ FAILED'} (confidence: {result['confidence']:.2f})")
            
        except Exception as e:
            logger.error(f"Verification error: {e}")
            result['confidence'] = 0.5
        
        return result
    
    def _save_response(self, response, url, success):
        """Save response HTML."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            url_slug = re.sub(r'[^\w\-]', '_', urlparse(url).netloc)[:50]
            status = 'success' if success else 'failed'
            filename = f"{timestamp}_{url_slug}_{status}.html"
            filepath = os.path.join(self.response_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                try:
                    f.write(response.text)
                except:
                    f.write(response.content.decode('utf-8', errors='ignore'))
            
            logger.debug(f"Saved response: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save response: {e}")


# ============================================================================
# PRIORITY 4: AJAX/JAVASCRIPT HANDLING
# ============================================================================

class AjaxFormHandler:
    """Handles AJAX and JavaScript-based form submissions."""
    
    AJAX_INDICATORS = [
        r'\.ajax\s*\(', r'fetch\s*\(', r'XMLHttpRequest',
        r'axios\.', r'\$\.post', r'preventDefault\(\)',
    ]
    
    AJAX_ENDPOINT_PATTERNS = [
        r'url\s*:\s*[\'"]([^\'"]+)[\'"]',
        r'fetch\s*\([\'"]([^\'"]+)[\'"]',
        r'\.post\s*\([\'"]([^\'"]+)[\'"]',
    ]
    
    def __init__(self, session: requests.Session):
        self.session = session
    
    def detect_submission_type(self, html_content: str, form_url: str) -> Dict:
        """Detect how the form should be submitted."""
        result = {
            'type': 'standard',
            'endpoint': None,
            'method': 'POST',
            'ajax_detected': False,
        }
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            form = soup.find('form')
            
            if not form:
                return result
            
            # Get form action and method
            form_action = form.get('action', '')
            result['endpoint'] = urljoin(form_url, form_action) if form_action else form_url
            result['method'] = form.get('method', 'POST').upper()
            
            # Check for AJAX indicators
            scripts = soup.find_all('script')
            script_content = '\n'.join([s.get_text() for s in scripts])
            combined = str(form) + script_content
            
            ajax_matches = [p for p in self.AJAX_INDICATORS if re.search(p, combined, re.I)]
            
            if ajax_matches:
                result['ajax_detected'] = True
                result['type'] = 'ajax'
                logger.info(f"AJAX detected: {len(ajax_matches)} indicators")
                
                # Try to extract endpoint
                endpoint = self._extract_ajax_endpoint(combined, form_url)
                if endpoint:
                    result['endpoint'] = endpoint
            
        except Exception as e:
            logger.error(f"Detection error: {e}")
        
        return result
    
    def _extract_ajax_endpoint(self, content: str, base_url: str) -> Optional[str]:
        """Extract AJAX endpoint from JavaScript."""
        for pattern in self.AJAX_ENDPOINT_PATTERNS:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                endpoint = match.group(1)
                return urljoin(base_url, endpoint) if not endpoint.startswith('http') else endpoint
        return None
    
    def submit_ajax_form(
        self,
        endpoint: str,
        form_data: Dict,
        method: str = 'POST',
        form_url: str = None
    ) -> Tuple[Optional[requests.Response], Optional[str]]:
        """Submit form via AJAX."""
        try:
            headers = self.session.headers.copy()
            headers['X-Requested-With'] = 'XMLHttpRequest'
            headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
            
            if form_url:
                headers['Referer'] = form_url
            
            logger.info(f"AJAX submission to: {endpoint}")
            
            if method.upper() == 'POST':
                response = self.session.post(endpoint, data=form_data, headers=headers, timeout=45)
            else:
                response = self.session.get(endpoint, params=form_data, headers=headers, timeout=45)
            
            logger.info(f"AJAX response: {response.status_code}")
            return response, None
            
        except Exception as e:
            error = f"AJAX error: {str(e)}"
            logger.error(error)
            return None, error


# ============================================================================
# PRIORITY 5: CSRF & MULTI-STEP HANDLING
# ============================================================================

class CsrfTokenExtractor:
    """Extracts CSRF tokens from forms."""
    
    CSRF_FIELD_NAMES = [
        'csrf_token', 'csrfToken', '_csrf', '_token', 'token',
        'authenticity_token', 'X-CSRF-Token', '__RequestVerificationToken',
    ]
    
    def extract_csrf_tokens(self, html_content: str) -> Dict[str, str]:
        """Extract all CSRF tokens from HTML."""
        tokens = {}
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Method 1: Hidden inputs
            for inp in soup.find_all('input', type='hidden'):
                name = inp.get('name', '').lower()
                value = inp.get('value', '')
                
                if any(csrf.lower() in name for csrf in self.CSRF_FIELD_NAMES):
                    tokens[inp.get('name')] = value
                    logger.info(f"CSRF token found: {inp.get('name')}")
            
            # Method 2: Meta tags
            for meta in soup.find_all('meta'):
                name = meta.get('name', '').lower()
                content = meta.get('content', '')
                
                if 'csrf' in name:
                    tokens[meta.get('name')] = content
                    logger.info(f"CSRF meta found: {meta.get('name')}")
            
            # Method 3: JavaScript variables
            for script in soup.find_all('script'):
                text = script.get_text()
                patterns = [
                    r'csrf[_-]?token["\']?\s*[:=]\s*["\']([^"\']+)',
                    r'_token["\']?\s*[:=]\s*["\']([^"\']+)',
                ]
                for pattern in patterns:
                    match = re.search(pattern, text, re.I)
                    if match:
                        tokens['_csrf_from_script'] = match.group(1)
                        logger.info("CSRF token in script")
                        break
        
        except Exception as e:
            logger.error(f"CSRF extraction error: {e}")
        
        return tokens


class MultiStepFormHandler:
    """Handles multi-step forms."""
    
    def __init__(self, session: requests.Session):
        self.session = session
        self.csrf_extractor = CsrfTokenExtractor()
    
    def detect_multi_step(self, html_content: str) -> Dict:
        """Detect if form is multi-step."""
        result = {
            'is_multi_step': False,
            'indicators': [],
            'current_step': None,
            'total_steps': None,
        }
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text().lower()
            
            # Check for step indicators
            step_patterns = [
                (r'step\s+(\d+)\s+of\s+(\d+)', re.I),
                (r'ステップ\s*(\d+)\s*/\s*(\d+)', re.I),
                (r'(\d+)\s*/\s*(\d+)', re.I),  # Generic "1/3" pattern
            ]
            
            for pattern, flags in step_patterns:
                match = re.search(pattern, text, flags)
                if match:
                    result['is_multi_step'] = True
                    result['current_step'] = int(match.group(1))
                    result['total_steps'] = int(match.group(2))
                    result['indicators'].append(f"Step indicator: {match.group(1)}/{match.group(2)}")
                    break
            
            # Check for hidden step fields
            for inp in soup.find_all('input', type='hidden'):
                name = inp.get('name', '').lower()
                value = inp.get('value', '')
                if 'step' in name or 'page' in name:
                    result['is_multi_step'] = True
                    result['indicators'].append(f"Hidden step field: {name}={value}")
                    # Try to extract step number
                    if value.isdigit():
                        result['current_step'] = int(value)
            
            # Check for next/continue buttons
            buttons = soup.find_all(['button', 'input'], type=['submit', 'button'])
            button_texts = [b.get_text().lower() if b.name == 'button' else b.get('value', '').lower() 
                          for b in buttons]
            
            next_keywords = ['next', '次へ', 'continue', '続ける', '次へ進む']
            if any(kw in ' '.join(button_texts) for kw in next_keywords):
                result['is_multi_step'] = True
                result['indicators'].append("Next/Continue button")
            
            # Check for wizard-like structure
            if soup.find(class_=re.compile(r'wizard|step|multi-step', re.I)):
                result['is_multi_step'] = True
                result['indicators'].append("Wizard-like structure detected")
        
        except Exception as e:
            logger.error(f"Multi-step detection error: {e}")
        
        return result
    
    def handle_multi_step(
        self,
        form_url: str,
        form_data: Dict,
        html_content: str,
        browser_submitter=None
    ) -> Tuple[Optional[requests.Response], Optional[str]]:
        """
        Handle multi-step form submission.
        
        Args:
            form_url: Form URL
            form_data: Complete form data
            html_content: Current page HTML
            browser_submitter: Optional browser submitter for JS-heavy forms
            
        Returns:
            Tuple of (response, error)
        """
        try:
            # For multi-step forms, browser method is usually required
            if browser_submitter:
                logger.info("Using browser for multi-step form submission")
                result, error = browser_submitter.submit_form(form_url, form_data, html_content)
                if result:
                    # Create mock response
                    class MockResponse:
                        def __init__(self, status_code, url, content):
                            self.status_code = status_code
                            self.url = url
                            self.text = content
                            self.content = content.encode('utf-8') if isinstance(content, str) else content
                    
                    return MockResponse(
                        result.get('http_status', 200),
                        result.get('response_url', form_url),
                        result.get('response_content', '')
                    ), None
                return None, error
            
            # Fallback: try standard submission (may not work for multi-step)
            logger.warning("Multi-step form detected but no browser submitter available")
            return None, "Multi-step forms require browser submission"
            
        except Exception as e:
            return None, f"Multi-step handling error: {str(e)}"


# ============================================================================
# MAIN FORM SUBMITTER (Integrates all priorities)
# ============================================================================

class SubmissionResult:
    """Result of form submission."""
    
    def __init__(self, success: bool, form_url: str, message: str = None):
        self.success = success
        self.form_url = form_url
        self.message = message
        self.response_status = None
        self.response_url = None
        self.response_content = None
        self.error = None
        self.timestamp = None
        self.submission_type = None
        
        # Priority 1: Retry
        self.retry_count = 0
        self.retry_errors = []
        
        # Priority 3: Verification
        self.verification_confidence = 0.0
        self.verification_indicators = []
        self.verification_warnings = []
        
        # Priority 4 & 5: Submission method
        self.submission_method = 'standard'
        self.csrf_used = False
        self.multi_step = False
    
    def to_dict(self) -> Dict:
        return {
            'success': self.success,
            'form_url': self.form_url,
            'message': self.message,
            'response_status': self.response_status,
            'response_url': self.response_url,
            'error': self.error,
            'retry_count': self.retry_count,
            'retry_errors': self.retry_errors,
            'verification_confidence': self.verification_confidence,
            'verification_indicators': self.verification_indicators,
            'verification_warnings': self.verification_warnings,
            'submission_method': self.submission_method,
            'csrf_used': self.csrf_used,
            'multi_step': self.multi_step,
        }


class FormSubmitter:
    """
    Complete Form Submitter - All Phase 3 Priorities in One Class
    """
    
    # Priority 1: Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 5.0
    RETRY_BACKOFF_MULTIPLIER = 2.0
    RETRYABLE_ERRORS = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
    )
    RETRYABLE_STATUS_CODES = [408, 429, 500, 502, 503, 504]
    
    def __init__(self, timeout: int = 30, user_agent: str = "ContactBot/1.0", use_browser_fallback: bool = True):
        self.timeout = timeout
        self.user_agent = user_agent
        self.use_browser_fallback = use_browser_fallback
        
        # Main session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        
        # Initialize all handlers
        self.verifier = SubmissionVerifier(save_responses=True)
        self.ajax_handler = AjaxFormHandler(self.session)
        self.csrf_extractor = CsrfTokenExtractor()
        self.multi_step_handler = MultiStepFormHandler(self.session)
        self.validator = FormDataValidator()
        
        # Browser submitter (lazy loaded)
        self._browser_submitter = None
    
    def submit_form(
        self,
        form_url: str,
        form_data: Dict,
        html_content: str = None,
        method: str = 'POST'
    ) -> SubmissionResult:
        """
        Submit form with ALL Phase 3 enhancements.
        
        Args:
            form_url: URL of the form
            form_data: Data to submit
            html_content: HTML content (optional, will fetch if not provided)
            method: HTTP method
            
        Returns:
            SubmissionResult with all details
        """
        result = SubmissionResult(False, form_url)
        
        try:
            # Fetch HTML if not provided
            if not html_content:
                logger.info("Fetching form page...")
                page_response = self.session.get(form_url, timeout=self.timeout)
                if page_response.status_code == 200:
                    html_content = page_response.text
                else:
                    result.error = f"Failed to fetch form: HTTP {page_response.status_code}"
                    return result
            
            logger.info(f"Submitting to: {form_url}")
            
            # Submit with retry and intelligence
            response = self._submit_with_retry_and_intelligence(
                form_url,
                form_data,
                html_content,
                method,
                result
            )
            
            if response:
                result.response_status = response.status_code
                result.response_url = str(response.url)
                
                try:
                    result.response_content = response.text[:500]
                except:
                    result.response_content = response.content[:500].decode('utf-8', errors='ignore')
                
                # PRIORITY 3: Enhanced verification
                verification = self.verifier.verify_submission(response, form_data, form_url)
                result.success = verification['success']
                result.verification_confidence = verification['confidence']
                result.verification_indicators = verification['indicators']
                result.verification_warnings = verification['warnings']
                
                if result.success:
                    result.message = f"Success (confidence: {verification['confidence']:.2f})"
                    logger.info(f"✅ {result.message}")
                else:
                    result.message = f"Unclear (confidence: {verification['confidence']:.2f})"
                    logger.warning(f"⚠️ {result.message}")
        
        except Exception as e:
            result.error = str(e)
            logger.error(f"Submission error: {e}")
        
        return result
    
    def _submit_with_retry_and_intelligence(
    self,
    form_url: str,
    data: Dict,
    html_content: str,
    method: str,
    result: SubmissionResult
) -> Optional[requests.Response]:
        """Submit with PRIORITY 1 (retry) + PRIORITY 4&5 (intelligence)."""
        attempt = 0
        last_error = None
        best_response = None
        best_confidence = 0.0
        
        while attempt <= self.MAX_RETRIES:
            try:
                if attempt > 0:
                    delay = self.RETRY_DELAY_BASE * (self.RETRY_BACKOFF_MULTIPLIER ** (attempt - 1))
                    logger.info(f"🔄 Retry {attempt}/{self.MAX_RETRIES} after {delay:.1f}s...")
                    time.sleep(delay)
                    result.retry_count = attempt
                
                # INTELLIGENT SUBMISSION
                response = self._intelligent_submit(
                    form_url,
                    data,
                    html_content,
                    method,
                    result
                )
                
                if response:
                    # Check if should retry based on status code
                    if response.status_code in self.RETRYABLE_STATUS_CODES:
                        error_msg = f"HTTP {response.status_code} (retryable)"
                        logger.warning(f"⚠️ {error_msg}")
                        result.retry_errors.append(error_msg)
                        last_error = error_msg
                        attempt += 1
                        continue
                    
                    # NEW: Check verification confidence
                    if response.status_code == 200:
                        verification = self.verifier.verify_submission(response, data, form_url)
                        confidence = verification['confidence']
                        
                        # Track best response
                        if confidence > best_confidence:
                            best_confidence = confidence
                            best_response = response
                        
                        # If confidence is very low and we have retries left, try again
                        if confidence < 0.3 and attempt < self.MAX_RETRIES:
                            error_msg = f"Low verification confidence ({confidence:.2f})"
                            logger.warning(f"⚠️ {error_msg} - will retry")
                            result.retry_errors.append(error_msg)
                            attempt += 1
                            continue
                        
                        # If confidence is acceptable (>= 0.3), use this response
                        if attempt > 0:
                            logger.info(f"✅ Succeeded on retry #{attempt} (confidence: {confidence:.2f})")
                        return response
                    else:
                        # Non-200 status
                        return response
                else:
                    error_msg = "No response"
                    result.retry_errors.append(error_msg)
                    last_error = error_msg
                    attempt += 1
                    continue
                    
            except self.RETRYABLE_ERRORS as e:
                error_msg = f"{type(e).__name__}"
                logger.warning(f"⚠️ Retryable: {error_msg}")
                result.retry_errors.append(error_msg)
                last_error = error_msg
                attempt += 1
                continue
                
            except Exception as e:
                error_msg = f"Non-retryable: {type(e).__name__}"
                logger.error(f"❌ {error_msg}")
                result.retry_errors.append(error_msg)
                return best_response if best_response else None
        
        logger.error(f"❌ All {self.MAX_RETRIES + 1} attempts failed")
        return best_response if best_response else None

    def _intelligent_submit(
        self,
        form_url: str,
        data: Dict,
        html_content: str,
        method: str,
        result: SubmissionResult
    ) -> Optional[requests.Response]:
        """PRIORITY 4 & 5: Intelligent submission with browser fallback."""
        
        try:
            # Validate and format form data
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                field_info = {}
                for field in soup.find_all(['input', 'textarea', 'select']):
                    field_name = field.get('name')
                    if field_name:
                        field_info[field_name] = {
                            'type': field.get('type', 'text')
                        }
                data = self.validator.validate_form_data(data, field_info)
                logger.debug("Form data validated and formatted")
            except Exception as e:
                logger.warning(f"Validation error (continuing anyway): {e}")
            
            # Check for CSRF tokens
            csrf_tokens = self.csrf_extractor.extract_csrf_tokens(html_content)
            if csrf_tokens:
                data.update(csrf_tokens)
                result.csrf_used = True
                result.submission_method = 'csrf'
                logger.info(f"✅ Using CSRF tokens: {len(csrf_tokens)}")
            
            # Check for multi-step
            multi_step = self.multi_step_handler.detect_multi_step(html_content)
            if multi_step['is_multi_step']:
                result.multi_step = True
                result.submission_method = 'multi_step'
                logger.info(f"⚠️ Multi-step detected (step {multi_step.get('current_step')}/{multi_step.get('total_steps')}) - using browser submission")
                # For multi-step, use browser method
                if self.use_browser_fallback:
                    if not self._browser_submitter:
                        self._browser_submitter = BrowserFormSubmitter(timeout=self.timeout)
                    browser_response, error = self.multi_step_handler.handle_multi_step(
                        form_url, data, html_content, self._browser_submitter
                    )
                    if browser_response:
                        return browser_response
                    logger.warning(f"Multi-step browser submission failed: {error}")
                else:
                    logger.warning("Multi-step form requires browser submission but browser fallback is disabled")
            
            # Check for AJAX - try network analysis first if browser available
            ajax_detection = self.ajax_handler.detect_submission_type(html_content, form_url)
            
            if ajax_detection['ajax_detected']:
                result.submission_method = 'ajax'
                logger.info("Using AJAX submission")
                
                # Try network analysis to find endpoint
                if self.use_browser_fallback and not ajax_detection.get('endpoint'):
                    logger.info("No endpoint found, trying network analysis...")
                    endpoints = self._capture_ajax_endpoints(form_url)
                    if endpoints:
                        # Use first endpoint found
                        endpoint_url = list(endpoints.keys())[0]
                        ajax_detection['endpoint'] = endpoint_url
                        logger.info(f"Found endpoint via network analysis: {endpoint_url}")
                
                response, error = self.ajax_handler.submit_ajax_form(
                    ajax_detection['endpoint'] or form_url,
                    data,
                    ajax_detection['method'],
                    form_url
                )
                if response:
                    return response
                logger.warning("AJAX failed, falling back to standard")
            
            # Standard submission
            result.submission_method = result.submission_method or 'standard'
            logger.info("Using standard submission")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            form = soup.find('form')
            
            submit_url = form_url
            if form and form.get('action'):
                submit_url = urljoin(form_url, form.get('action'))
            
            try:
                if method.upper() == 'POST':
                    return self.session.post(submit_url, data=data, timeout=self.timeout, allow_redirects=True)
                else:
                    return self.session.get(submit_url, params=data, timeout=self.timeout, allow_redirects=True)
            except Exception as e:
                logger.warning(f"Standard submission failed: {e}")
                # Fallback to browser if enabled
                if self.use_browser_fallback:
                    logger.info("Falling back to browser submission...")
                    return self._try_browser_submit(form_url, data, html_content, result)
                raise
        
        except Exception as e:
            logger.error(f"Intelligent submit error: {e}")
            # Last resort: try browser
            if self.use_browser_fallback:
                logger.info("Attempting browser submission as last resort...")
                return self._try_browser_submit(form_url, data, html_content, result)
            raise
    
    def _try_browser_submit(
        self,
        form_url: str,
        data: Dict,
        html_content: str,
        result: SubmissionResult
    ) -> Optional[requests.Response]:
        """Try browser-based submission."""
        try:
            if not self._browser_submitter:
                self._browser_submitter = BrowserFormSubmitter(timeout=self.timeout)
            
            browser_result, error = self._browser_submitter.submit_form(form_url, data, html_content)
            
            if browser_result:
                result.submission_method = 'browser'
                result.response_status = browser_result.get('http_status', 200)
                result.response_url = browser_result.get('response_url', form_url)
                
                # Create mock response object for compatibility
                class MockResponse:
                    def __init__(self, status_code, url, content):
                        self.status_code = status_code
                        self.url = url
                        self.text = content
                        self.content = content.encode('utf-8') if isinstance(content, str) else content
                
                return MockResponse(
                    browser_result.get('http_status', 200),
                    browser_result.get('response_url', form_url),
                    browser_result.get('response_content', '')
                )
            else:
                logger.error(f"Browser submission failed: {error}")
                return None
                
        except Exception as e:
            logger.error(f"Browser submission error: {e}")
            return None
    
    def _capture_ajax_endpoints(self, form_url: str) -> Dict[str, str]:
        """Capture AJAX endpoints using browser network analysis."""
        try:
            if not self._browser_submitter:
                self._browser_submitter = BrowserFormSubmitter(timeout=self.timeout)
            return self._browser_submitter.capture_ajax_endpoints(form_url, timeout=5)
        except Exception as e:
            logger.error(f"Failed to capture AJAX endpoints: {e}")
            return {}
    
    def close(self):
        """Close session and browser."""
        self.session.close()
        if self._browser_submitter:
            self._browser_submitter.close()

class FormSubmissionPipeline:
    """Full pipeline for form submission - wrapper around FormSubmitter."""
    
    def __init__(self, timeout: int = 30, user_agent: str = "ContactBot/1.0", use_browser_fallback: bool = True):
        self.analyzer = FormAnalyzer(base_url="")
        self.submitter = FormSubmitter(timeout, user_agent, use_browser_fallback=use_browser_fallback)
    
    def submit_to_form(
        self,
        form_url: str,
        html_content: str,
        company_name: str = None,
        sender_email: str = None,
        sender_name: str = None,
        message_body: str = None,
        phone: str = None
    ) -> SubmissionResult:
        """
        Full pipeline: analyze form, prepare data, and submit.
        """
        logger.info("=" * 70)
        logger.info(f"FORM SUBMISSION PIPELINE: {form_url}")
        logger.info("=" * 70)
        
        # Step 1: Analyze form
        logger.info("\n[Step 1] Analyzing form...")
        analysis = self.analyzer.analyze(form_url, html_content)
        
        if not analysis:
            result = SubmissionResult(False, form_url)
            result.error = "Could not analyze form"
            return result
        
        logger.info(f"Analysis complete: {analysis.confidence:.2f} confidence")
        
        # Step 2: Check for CAPTCHA
        if analysis.has_captcha:
            result = SubmissionResult(False, form_url)
            result.error = f"Form has {analysis.captcha_type} - cannot submit"
            logger.error(f"CAPTCHA blocking submission: {analysis.captcha_type}")
            return result
        
        logger.info("✅ No CAPTCHA detected")
        
        # Step 3: Prepare data
        logger.info("\n[Step 2] Preparing form data...")
        form_data = self._prepare_data(
            analysis, company_name, sender_email, 
            sender_name, message_body, phone
        )
        
        # Step 4: Submit form
        logger.info("\n[Step 3] Submitting form...")
        result = self.submitter.submit_form(
            form_url, form_data, html_content, analysis.form_method
        )
        
        logger.info("\n" + "=" * 70)
        logger.info(f"RESULT: {'SUCCESS' if result.success else 'FAILED'}")
        logger.info(f"Message: {result.message or result.error}")
        logger.info("=" * 70 + "\n")
        
        return result
    
    def _prepare_data(self, analysis, company_name, sender_email, 
                     sender_name, message_body, phone) -> Dict:
        """Prepare form data."""
        form_data = {}
        form_data.update(analysis.hidden_fields)
        
        # Map values to fields
        field_mapping = {
            'email': sender_email,
            'name': sender_name,
            'company': company_name,
            'message': message_body,
            'phone': phone,
        }
        
        for field in analysis.fields:
            for purpose, value in field_mapping.items():
                if value and self._field_matches(field.name, purpose):
                    form_data[field.name] = value
                    logger.info(f"  - {field.name}: {value[:50]}")
                    break
        
        return form_data
    
    def _field_matches(self, field_name: str, purpose: str) -> bool:
        """Check if field matches purpose."""
        patterns = {
            'email': ['email', 'mail'],
            'name': ['name', '名前'],
            'company': ['company', '会社'],
            'message': ['message', 'content', 'inquiry'],
            'phone': ['phone', 'tel', '電話'],
        }
        return any(p in field_name.lower() for p in patterns.get(purpose, []))