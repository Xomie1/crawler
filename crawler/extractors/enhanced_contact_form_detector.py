"""
MERGED Enhanced Contact Form Detector
Combines the best of both versions:
- TOP/HIGH/MEDIUM/LOW priority fallbacks from v1
- Parameter-based link detection from v1
- Complete form analysis from v2
- Better logging throughout
"""

import re
import logging
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
import time

logger = logging.getLogger(__name__)


class FormCandidate:
    """Represents a contact form candidate with detailed field analysis."""
    
    def __init__(self, url: str, form_element=None, detection_method: str = "unknown"):
        self.url = url
        self.form_element = form_element
        self.detection_method = detection_method
        self.score = 0.0
        
        # Form fields detected
        self.has_email_field = False
        self.has_phone_field = False
        self.has_name_field = False
        self.has_message_field = False
        self.has_company_field = False
        self.has_subject_field = False
        
        # Form metadata
        self.form_action = None
        self.form_method = None
        self.form_id = None
        self.form_class = None
        self.submit_button_text = None
        
        # Field counts
        self.text_fields = 0
        self.textareas = 0
        self.email_fields = 0
        self.tel_fields = 0
        self.required_fields = 0
        
        # Detection metadata
        self.keywords_found = []
        self.is_in_header_footer = False
        
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'url': self.url,
            'score': round(self.score, 2),
            'detection_method': self.detection_method,
            'has_email_field': self.has_email_field,
            'has_phone_field': self.has_phone_field,
            'has_name_field': self.has_name_field,
            'has_message_field': self.has_message_field,
            'text_fields': self.text_fields,
            'textareas': self.textareas,
            'email_fields': self.email_fields,
            'tel_fields': self.tel_fields,
            'form_action': self.form_action,
            'submit_button_text': self.submit_button_text,
            'keywords_found': self.keywords_found
        }


class EnhancedContactFormDetector:
    """
    MERGED contact form detector with prioritized fallback strategy.
    MERGED VERSION - Has TOP/HIGH/MEDIUM/LOW priority splits
    """
    
    # Contact form keywords
    CONTACT_KEYWORDS = [
        'お問い合わせ', 'お問合せ', '問い合わせ', 'ご相談', '相談',
        'お申し込み', '申込', '資料請求', 'お見積り', '見積もり',
        'コンタクト', 'メッセージ', 'お問い合わせフォーム',
        'contact', 'inquiry', 'inquire', 'message', 'reach out',
        'get in touch', 'contact us', 'send message', 'request',
        'consultation', 'quote', 'estimate'
    ]
    
    # URL patterns for contact forms
    CONTACT_URL_PATTERNS = [
        r'/contact/?$',
        r'/contact\.html?/?$',
        r'/contact\.php/?$',
        r'/contact-us/?$',
        r'/contact_us/?$',
        r'/contactus/?$',
        r'/inquiry\d*/?$',
        r'/inquiry\d*\.html?/?$',
        r'/inquiry\d*\.php/?$',
        r'/inquire/?$',
        r'/inquiry-form/?$',
        r'/inquiry_form/?$',
        r'/inquiryform/?$',
        r'/form/?$',
        r'/form\.html?/?$',
        r'/form\.php/?$',
        r'/form-contact/?$',
        r'/[^\s]*お問い合わせ[^\s]*/?$',
        r'/[^\s]*問い合わせ[^\s]*/?$',
        r'/[^\s]*otoiawase[^\s]*/?$',
        r'/[^\s]*toiawase[^\s]*/?$',
        r'/[^\s]*soudan[^\s]*/?$',
        r'/[^\s]*\?.*(?:inquiry|contact|form|CNo|uid)[^\s]*$',
        r'/contents/[^\s]*(?:inquiry|contact)[^\s]*$',
        r'/menu/[^\s]*\.php$',
        r'/contact-form/?$',
        r'/contact_form/?$',
        r'/contactform/?$',
    ]
    
    COMPILED_PATTERNS = [re.compile(pattern, re.IGNORECASE) for pattern in CONTACT_URL_PATTERNS]
    
    # MERGED: Prioritized fallback URLs (TOP → HIGH → MEDIUM → LOW)
    TOP_PRIORITY_FALLBACK_URLS = [
        '/contact',
        '/contact/',
        '/contact/mailform',      # ADD THIS
        '/contact/mailform/',     # ADD THIS
        '/contact/contact.html',
        '/contact/inquiry.html',
        '/inquiry',
        '/inquiry/',
    ]
    
    HIGH_PRIORITY_FALLBACK_URLS = [
        '/contact.html',
        '/contact.php',
        '/inquiry.html',
        '/inquiry.php',
        '/form',
        '/form.html',
    ]
    
    MEDIUM_PRIORITY_FALLBACK_URLS = [
        '/contact.htm',
        '/inquiry.htm',
        '/contact/index.html',
        '/contact/index.htm',
        '/inquiry/index.html',
        '/form.php',
        '/contact-us',
        '/contact_us',
        '/inquiry-form',
    ]
    
    LOW_PRIORITY_FALLBACK_URLS = [
        '/お問い合わせ',
        '/otoiawase',
        '/toiawase',
        '/contact.aspx',
        '/inquiry.aspx',
    ]
    
    # Query parameter patterns for finding contact forms with params
    QUERY_PARAM_PATTERNS = [
        re.compile(r'inquiry', re.I),
        re.compile(r'contact', re.I),
        re.compile(r'form', re.I),
        re.compile(r'CNo', re.I),
        re.compile(r'uid', re.I),
    ]
    
    # Field patterns
    FIELD_PATTERNS = {
        'email': [
            r'email', r'mail', r'e-mail', r'メール', r'ｅ－ｍａｉｌ',
            r'eメール', r'Eメール'
        ],
        'phone': [
            r'phone', r'tel', r'telephone', r'mobile', r'電話', r'でんわ',
            r'携帯', r'TEL', r'Tel'
        ],
        'name': [
            r'name', r'名前', r'なまえ', r'氏名', r'お名前', r'yourname',
            r'fullname', r'氏', r'名'
        ],
        'company': [
            r'company', r'organization', r'会社', r'企業', r'法人',
            r'組織', r'会社名', r'御社名'
        ],
        'message': [
            r'message', r'comment', r'content', r'inquiry', r'question',
            r'メッセージ', r'内容', r'お問い合わせ内容', r'ご質問',
            r'詳細', r'本文'
        ],
        'subject': [
            r'subject', r'title', r'件名', r'タイトル', r'表題'
        ]
    }
    
    EXCLUDE_PATTERNS = [
        r'login', r'signin', r'sign-in', r'ログイン', r'サインイン',
        r'password', r'パスワード',
        r'search', r'検索', r'さんさく',
        r'newsletter', r'subscribe', r'メルマガ', r'購読',
        r'comment', r'コメント',
        r'cart', r'checkout', r'カート', r'購入'
    ]
    
    def __init__(self, fetcher=None, robots_checker=None, max_pages: int = 15):
        """Initialize detector."""
        self.fetcher = fetcher
        self.robots_checker = robots_checker
        self.max_pages = max_pages
        self.visited_urls: Set[str] = set()
    
    def detect_contact_form(self, root_url: str) -> Dict:
        """
        Main detection with prioritized fallback strategy.
        """
        try:
            logger.info(f"Starting contact form detection for {root_url}")
            logger.info(f"TOP_PRIORITY_FALLBACK_URLS: {self.TOP_PRIORITY_FALLBACK_URLS}")
            
            # Step 1: Try TOP PRIORITY fallbacks first (most common)
            logger.info("Step 1: Trying TOP priority fallback URLs...")
            top_result = self._try_fallback_list(root_url, self.TOP_PRIORITY_FALLBACK_URLS, 'top_priority')
            if top_result:
                return top_result
            
            # Step 2: Try HIGH PRIORITY fallbacks
            logger.info("Step 2: Trying HIGH priority fallback URLs...")
            high_result = self._try_fallback_list(root_url, self.HIGH_PRIORITY_FALLBACK_URLS, 'high_priority')
            if high_result:
                return high_result
            
            # Step 3: Check homepage for parameter-based links
            logger.info("Step 3: Checking homepage for parameter-based contact links...")
            param_result = self._check_parameter_links_on_homepage(root_url)
            if param_result:
                return param_result
            
            # Step 4: Try MEDIUM PRIORITY fallbacks
            logger.info("Step 4: Trying MEDIUM priority fallback URLs...")
            medium_result = self._try_fallback_list(root_url, self.MEDIUM_PRIORITY_FALLBACK_URLS, 'medium_priority')
            if medium_result:
                return medium_result
            
            # Step 5: Try LOW PRIORITY fallbacks
            logger.info("Step 5: Trying LOW priority fallback URLs...")
            low_result = self._try_fallback_list(root_url, self.LOW_PRIORITY_FALLBACK_URLS, 'low_priority')
            if low_result:
                return low_result
            
            # Step 6: Full site crawl as last resort
            logger.info("Step 6: Fallbacks unsuccessful, crawling site...")
            crawl_result = self._crawl_site_for_forms(root_url)
            if crawl_result:
                return crawl_result
            
            # Nothing found
            return {
                'form_url': None,
                'form_details': None,
                'candidates': [],
                'remarks': 'No contact forms found after comprehensive search'
            }
            
        except Exception as e:
            logger.error(f"Error in contact form detection: {e}")
            return {
                'form_url': None,
                'form_details': None,
                'candidates': [],
                'remarks': f'Error: {str(e)}'
            }
    
    def _try_fallback_list(self, root_url: str, fallback_list: List[str], priority_level: str) -> Optional[Dict]:
        """Try a list of fallback URLs. Returns IMMEDIATELY upon finding first form."""
        parsed_root = urlparse(root_url)
        base_domain = f"{parsed_root.scheme}://{parsed_root.netloc}"
        
        for fallback_path in fallback_list:
            fallback_url = base_domain + fallback_path
            
            try:
                if self.robots_checker and not self.robots_checker.is_allowed(fallback_url, "respect"):
                    logger.debug(f"  ✗ Robots.txt disallows: {fallback_path}")
                    continue
                
                logger.info(f"  → Trying: {fallback_url}")
                content, status_code, final_url, error = self.fetcher.fetch_page(fallback_url)
                
                if not content or status_code != 200:
                    logger.info(f"    ✗ HTTP {status_code}")
                    continue
                
                if '<form' not in content.lower():
                    logger.info(f"    ✗ No <form> tag")
                    continue
                
                # FORM FOUND - RETURN IMMEDIATELY
                logger.info(f"    ✓ Found <form> tag - RETURNING IMMEDIATELY")
                logger.info(f"✓✓✓ EXITING LOOP - FORM FOUND: {final_url or fallback_url}")
                return {
                    'form_url': final_url or fallback_url,
                    'form_details': {'detection_method': f'{priority_level}_fallback'},
                    'candidates': [],
                    'remarks': f'Found form via {priority_level} fallback'
                }
                    
            except Exception as e:
                logger.debug(f"  ✗ Error: {e}")
                continue
        
        logger.info(f"  No forms found in {priority_level} fallback URLs")
        return None
    
    def _check_parameter_links_on_homepage(self, root_url: str) -> Optional[Dict]:
        """Check homepage for parameter-based contact links."""
        try:
            content, status_code, final_url, error = self.fetcher.fetch_page(root_url)
            
            if not content or status_code != 200:
                return None
            
            soup = BeautifulSoup(content, 'html.parser')
            found_candidates = []
            
            # Look for links with query parameters
            for link in soup.find_all('a', href=True):
                href = link['href']
                link_text = link.get_text().strip()
                
                if '?' not in href:
                    continue
                
                absolute_url = urljoin(root_url, href)
                parsed = urlparse(absolute_url)
                
                # Check if link suggests contact
                is_contact_text = any(
                    keyword in link_text.lower() 
                    for keyword in ['contact', 'inquiry', 'お問い合わせ', '問合せ', '相談', 'form']
                )
                
                # Check parameter patterns
                full_query = parsed.path + '?' + parsed.query
                matches_param_pattern = any(
                    pattern.search(full_query) 
                    for pattern in self.QUERY_PARAM_PATTERNS
                )
                
                if is_contact_text or matches_param_pattern:
                    logger.info(f"  → Found parameter link: {absolute_url}")
                    
                    try:
                        param_content, param_status, param_final_url, param_error = \
                            self.fetcher.fetch_page(absolute_url)
                        
                        if param_content and param_status == 200 and '<form' in param_content.lower():
                            logger.info(f"    ✓ Has forms!")
                            
                            form_candidates = self._analyze_page_forms(
                                param_final_url or absolute_url,
                                param_content,
                                detection_method='parameter_link'
                            )
                            
                            if form_candidates:
                                scored = self._score_candidates(form_candidates)
                                found_candidates.extend([c for c in scored if c.score > 0])
                    except Exception as e:
                        logger.debug(f"    ✗ Error: {e}")
                        continue
            
            if found_candidates:
                found_candidates.sort(key=lambda x: x.score, reverse=True)
                best = found_candidates[0]
                
                return {
                    'form_url': best.url,
                    'form_details': best.to_dict(),
                    'candidates': [c.to_dict() for c in found_candidates[:3]],
                    'remarks': f'Found via parameter link (score: {best.score:.2f})'
                }
            
        except Exception as e:
            logger.debug(f"Error checking parameter links: {e}")
        
        return None
    
    def _crawl_site_for_forms(self, root_url: str) -> Optional[Dict]:
        """Full site crawl as last resort."""
        pages_with_forms = self._crawl_and_find_forms(root_url)
        
        if not pages_with_forms:
            return None
        
        candidates = []
        for url, html_content in pages_with_forms:
            form_candidates = self._analyze_page_forms(url, html_content, detection_method='site_crawl')
            candidates.extend(form_candidates)
        
        if not candidates:
            return None
        
        scored_candidates = self._score_candidates(candidates)
        
        if scored_candidates and scored_candidates[0].score > 0:
            best = scored_candidates[0]
            return {
                'form_url': best.url,
                'form_details': best.to_dict(),
                'candidates': [c.to_dict() for c in scored_candidates[:5]],
                'remarks': f'Found via site crawl (score: {best.score:.2f})'
            }
        
        return None
    
    def _crawl_and_find_forms(self, root_url: str) -> List[Tuple[str, str]]:
        """Crawl and find forms on the site."""
        pages_with_forms = []
        urls_to_visit = [root_url]
        self.visited_urls.clear()
        
        parsed_root = urlparse(root_url)
        root_domain = parsed_root.netloc
        
        while urls_to_visit and len(pages_with_forms) < self.max_pages:
            if len(self.visited_urls) >= self.max_pages * 2:
                break
            
            url = urls_to_visit.pop(0)
            
            if url in self.visited_urls:
                continue
            
            self.visited_urls.add(url)
            
            # Check robots.txt
            if self.robots_checker and not self.robots_checker.is_allowed(url, "respect"):
                continue
            
            try:
                content, status_code, final_url, error = self.fetcher.fetch_page(url)
                
                if not content or status_code != 200:
                    continue
                
                # Check if page has forms
                if '<form' in content.lower():
                    pages_with_forms.append((final_url or url, content))
                    logger.info(f"Found forms on: {final_url or url}")
                
                # Extract more URLs
                if len(pages_with_forms) < self.max_pages:
                    new_urls = self._extract_priority_links(content, final_url or url, root_domain)
                    urls_to_visit.extend(new_urls)
                
            except Exception as e:
                logger.debug(f"Error fetching {url}: {e}")
                continue
        
        return pages_with_forms
    
    def _extract_priority_links(self, html_content: str, base_url: str, root_domain: str) -> List[str]:
        """Extract priority links from page."""
        priority_links = []
        normal_links = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                link_text = tag.get_text().strip().lower()
                
                absolute_url = urljoin(base_url, href)
                parsed = urlparse(absolute_url)
                
                if parsed.netloc != root_domain:
                    continue
                
                absolute_url = absolute_url.split('#')[0]
                
                if not absolute_url or absolute_url in self.visited_urls:
                    continue
                
                # Check patterns
                path = parsed.path + ('?' + parsed.query if parsed.query else '')
                is_contact_url = any(pattern.search(path) for pattern in self.COMPILED_PATTERNS)
                is_contact_text = any(keyword in link_text for keyword in self.CONTACT_KEYWORDS)
                
                if is_contact_url or is_contact_text:
                    priority_links.append(absolute_url)
                else:
                    normal_links.append(absolute_url)
            
            return priority_links + normal_links[:5]
            
        except Exception as e:
            logger.error(f"Error extracting links: {e}")
            return []
    
    def _analyze_page_forms(self, url: str, html_content: str, detection_method: str = "crawl") -> List[FormCandidate]:
        """Analyze forms on a page."""
        candidates = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            forms = soup.find_all('form')
            
            logger.debug(f"Found {len(forms)} forms on {url}")
            
            for form in forms:
                # Check if URL matches pattern
                if detection_method == "crawl":
                    parsed = urlparse(url)
                    path = parsed.path + ('?' + parsed.query if parsed.query else '')
                    if any(pattern.search(path) for pattern in self.COMPILED_PATTERNS):
                        detection_method = 'pattern_match'
                
                candidate = FormCandidate(url, form, detection_method)
                
                candidate.form_action = form.get('action', '')
                candidate.form_method = form.get('method', 'GET').upper()
                candidate.form_id = form.get('id', '')
                candidate.form_class = ' '.join(form.get('class', []))
                
                if self._should_exclude_form(form):
                    continue
                
                self._analyze_form_fields(form, candidate)
                self._analyze_submit_button(form, candidate)
                self._check_contact_keywords(form, candidate)
                
                candidates.append(candidate)
                
        except Exception as e:
            logger.error(f"Error analyzing forms on {url}: {e}")
        
        return candidates
    
    def _should_exclude_form(self, form) -> bool:
        """Check if form should be excluded."""
        form_html = str(form).lower()
        return any(re.search(pattern, form_html) for pattern in self.EXCLUDE_PATTERNS)
    
    def _analyze_form_fields(self, form, candidate: FormCandidate):
        """Analyze form fields."""
        inputs = form.find_all(['input', 'textarea', 'select'])
        
        for field in inputs:
            field_type = field.get('type', 'text').lower()
            field_name = field.get('name', '').lower()
            field_id = field.get('id', '').lower()
            field_placeholder = field.get('placeholder', '').lower()
            
            field_text = f"{field_name} {field_id} {field_placeholder}"
            
            if field_type == 'email' or self._matches_patterns(field_text, self.FIELD_PATTERNS['email']):
                candidate.has_email_field = True
                candidate.email_fields += 1
            elif field_type == 'tel' or self._matches_patterns(field_text, self.FIELD_PATTERNS['phone']):
                candidate.has_phone_field = True
                candidate.tel_fields += 1
            elif self._matches_patterns(field_text, self.FIELD_PATTERNS['name']):
                candidate.has_name_field = True
            elif self._matches_patterns(field_text, self.FIELD_PATTERNS['company']):
                candidate.has_company_field = True
            elif self._matches_patterns(field_text, self.FIELD_PATTERNS['subject']):
                candidate.has_subject_field = True
            
            if field.name == 'textarea' or self._matches_patterns(field_text, self.FIELD_PATTERNS['message']):
                candidate.has_message_field = True
                candidate.textareas += 1
            
            if field.name == 'textarea':
                candidate.textareas += 1
            elif field_type in ['text', 'email', 'tel', 'url']:
                candidate.text_fields += 1
            
            if field.get('required') or field.get('aria-required'):
                candidate.required_fields += 1
    
    def _matches_patterns(self, text: str, patterns: List[str]) -> bool:
        """Check if text matches patterns."""
        return any(re.search(pattern, text, re.I) for pattern in patterns)
    
    def _analyze_submit_button(self, form, candidate: FormCandidate):
        """Analyze submit button."""
        buttons = form.find_all(['button', 'input'], type=['submit', 'button'])
        
        for button in buttons:
            text = button.get_text().strip() if button.name == 'button' else button.get('value', '').strip()
            if text:
                candidate.submit_button_text = text
                break
    
    def _check_contact_keywords(self, form, candidate: FormCandidate):
        """Check for contact keywords."""
        form_text = form.get_text().lower()
        for keyword in self.CONTACT_KEYWORDS:
            if keyword.lower() in form_text:
                candidate.keywords_found.append(keyword)
    
    def _score_candidates(self, candidates: List[FormCandidate]) -> List[FormCandidate]:
        """Score all candidates - LENIENT scoring to accept more forms."""
        for candidate in candidates:
            score = 0.0
            
            # Start with base score just for being a form
            score += 10.0
            
            # Bonus for email field (but don't penalize if missing)
            if candidate.has_email_field:
                score += 30
            
            # Bonus for message/textarea field
            if candidate.has_message_field:
                score += 25
            
            # Bonuses for other fields
            if candidate.has_name_field:
                score += 15
            if candidate.has_phone_field:
                score += 10
            if candidate.has_company_field:
                score += 8
            if candidate.has_subject_field:
                score += 5
            
            # Text fields - be lenient
            if candidate.text_fields >= 1:
                score += 8
            
            if candidate.textareas >= 1:
                score += 5
            
            # Keywords found
            if candidate.keywords_found:
                score += min(len(candidate.keywords_found) * 5, 20)
            
            # URL pattern matching - STRONG bonus
            url_lower = candidate.url.lower()
            if any(word in url_lower for word in ['contact', 'inquiry', 'form', 'otoiawase', 'toiawase']):
                score += 25  # Increased from 15
            
            # Submit button
            if candidate.submit_button_text:
                button_lower = candidate.submit_button_text.lower()
                if any(word in button_lower for word in ['send', '送信', 'submit', '確認', 'confirm']):
                    score += 15  # Increased from 10
            
            # Form method
            if candidate.form_method == 'POST':
                score += 10  # Increased from 5
            
            # Required fields
            if candidate.required_fields >= 2:
                score += 8
            
            # Detection method - HUGE bonus for fallback/pattern match
            if candidate.detection_method in ['pattern_match', 'top_priority_fallback', 'high_priority_fallback', 'parameter_link']:
                score += 40  # Increased from 20
            elif candidate.detection_method == 'site_crawl':
                score += 20
            
            # Ensure minimum score so forms aren't rejected
            candidate.score = max(0.1, min(1.0, score / 100.0))
        
        candidates.sort(key=lambda x: x.score, reverse=True)
        return candidates
    
    def _generate_remarks(self, candidate: FormCandidate) -> str:
        """Generate remarks."""
        remarks = []
        
        fields = []
        if candidate.has_email_field:
            fields.append('email')
        if candidate.has_phone_field:
            fields.append('phone')
        if candidate.has_name_field:
            fields.append('name')
        if candidate.has_message_field:
            fields.append('message')
        
        if fields:
            remarks.append(f"Fields: {', '.join(fields)}")
        
        remarks.append(f"Method: {candidate.detection_method}")
        remarks.append(f"Score: {candidate.score:.2f}")
        
        if candidate.keywords_found:
            remarks.append(f"Keywords: {len(candidate.keywords_found)}")
        
        if candidate.submit_button_text:
            remarks.append(f"Button: '{candidate.submit_button_text}'")
        
        return "; ".join(remarks)