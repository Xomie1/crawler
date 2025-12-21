"""Email Extraction Module - Fixed for Full-Width Characters"""
import re, html, unicodedata, json, logging
from typing import List, Dict, Optional, Set
from urllib.parse import urlparse
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class EmailCandidate:
    def __init__(self, email: str, detection_method: str, source_url: str, selector: Optional[str] = None, context_snippet: Optional[str] = None, score: float = 0.0):
        self.email, self.detection_method, self.source_url = email, detection_method, source_url
        self.selector, self.context_snippet, self.score = selector, context_snippet, score
    def to_dict(self) -> Dict:
        return {'email': self.email, 'confidence': self.score, 'source_url': self.source_url, 'selector': self.selector, 'detection_method': self.detection_method, 'context_snippet': self.context_snippet}

class EmailExtractor:
    EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    # New: Pattern for full-width @ and dot
    EMAIL_PATTERN_FULLWIDTH = re.compile(r'\b[A-Za-z0-9._%+-]+[＠@][A-Za-z0-9．.-]+[．.][A-Z|a-z]{2,}\b', re.UNICODE)
    EMAIL_KEYWORDS = ['email', 'mail', 'e-mail', 'メール', 'メールアドレス', 'contact', 'お問い合わせ', '連絡先', '電子メール', 'eメール', 'inquiry', 'お問合せ']
    OBFUSCATION_PATTERNS = [(r'\[at\]', '@'), (r'\(at\)', '@'), (r'\[dot\]', '.'), (r'\(dot\)', '.')]
    
    def __init__(self, base_url: str, use_playwright: bool = True, validate_mx: bool = False):
        self.base_url = base_url
        self.use_playwright = use_playwright
        self.validate_mx = validate_mx
        self.domain = urlparse(base_url).netloc
    
    def extract(self, html_content: str, final_url: Optional[str] = None) -> Dict:
        url = final_url or self.base_url
        candidates = []
        
        candidates.extend(self._detect_mailto_links(html_content, url))
        candidates.extend(self._detect_definition_list_emails(html_content, url))
        candidates.extend(self._detect_label_value_pairs(html_content, url))
        candidates.extend(self._detect_table_emails(html_content, url))
        candidates.extend(self._detect_list_emails(html_content, url))
        candidates.extend(self._detect_plain_emails(html_content, url))
        candidates.extend(self._detect_emails_from_text(html_content, url))
        candidates.extend(self._detect_dom_inspection(html_content, url))
        candidates.extend(self._detect_jsonld_schema(html_content, url))
        
        normalized_candidates = []
        seen_emails = set()
        
        for candidate in candidates:
            normalized = self._normalize_email(candidate.email)
            if normalized and normalized not in seen_emails and self._validate_email(normalized):
                seen_emails.add(normalized)
                candidate.email = normalized
                candidate.score = self._score_candidate(candidate, html_content)
                normalized_candidates.append(candidate)
                logger.info(f"Email candidate: {candidate.email} (method: {candidate.detection_method}, score: {candidate.score:.2f})")
        
        normalized_candidates.sort(key=lambda x: x.score, reverse=True)
        result = {'email': None, 'confidence': 0.0, 'candidates': [c.to_dict() for c in normalized_candidates]}
        
        if normalized_candidates:
            top = normalized_candidates[0]
            result['email'] = top.email
            result['confidence'] = top.score
        
        return result
    
    def _detect_mailto_links(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            for link in BeautifulSoup(html_content, 'html.parser').find_all('a', href=re.compile(r'^mailto:', re.I)):
                match = re.search(self.EMAIL_PATTERN, link.get('href', ''))
                if match:
                    candidates.append(EmailCandidate(match.group(), 'mailto_link', url, selector="a[href^='mailto:']", context_snippet=link.get_text().strip()[:100]))
                    logger.debug(f"Found mailto link: {match.group()}")
        except Exception as e:
            logger.error(f"Error detecting mailto links: {e}")
        return candidates
    
    def _detect_definition_list_emails(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            for dt in BeautifulSoup(html_content, 'html.parser').find_all('dt'):
                if any(kw.lower() in dt.get_text().lower() for kw in self.EMAIL_KEYWORDS):
                    dd = dt.find_next('dd')
                    if dd:
                        text = dd.get_text()
                        # Check for both regular and full-width @ symbols
                        if '@' in text or '＠' in text:
                            # Try full-width pattern first
                            for email in self.EMAIL_PATTERN_FULLWIDTH.findall(text):
                                candidates.append(EmailCandidate(email, 'definition_list', url, selector='dl > dt + dd', context_snippet=f"{dt.get_text()}:{text}"[:150]))
                                logger.debug(f"Found email in definition list: {email}")
                            # Then try regular pattern
                            for email in self.EMAIL_PATTERN.findall(text):
                                candidates.append(EmailCandidate(email, 'definition_list', url, selector='dl > dt + dd', context_snippet=f"{dt.get_text()}:{text}"[:150]))
                                logger.debug(f"Found email in definition list: {email}")
        except Exception as e:
            logger.error(f"Error detecting definition list emails: {e}")
        return candidates
    
    def _detect_label_value_pairs(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            for label_elem in BeautifulSoup(html_content, 'html.parser').find_all(['span', 'div', 'td', 'th', 'dt', 'label']):
                if any(kw.lower() in label_elem.get_text().lower() for kw in self.EMAIL_KEYWORDS):
                    next_elem = label_elem.find_next_sibling(['span', 'div', 'td', 'a', 'p'])
                    if next_elem:
                        text = next_elem.get_text()
                        if '@' in text or '＠' in text:
                            # Try full-width pattern first
                            for email in self.EMAIL_PATTERN_FULLWIDTH.findall(text):
                                candidates.append(EmailCandidate(email, 'label_value_pair', url, context_snippet=f"{label_elem.get_text()}:{text}"[:150]))
                                logger.debug(f"Found email in label-value pair: {email}")
                            # Then try regular pattern
                            for email in self.EMAIL_PATTERN.findall(text):
                                candidates.append(EmailCandidate(email, 'label_value_pair', url, context_snippet=f"{label_elem.get_text()}:{text}"[:150]))
                                logger.debug(f"Found email in label-value pair: {email}")
        except Exception as e:
            logger.error(f"Error detecting label-value pairs: {e}")
        return candidates
    
    def _detect_list_emails(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            for li in BeautifulSoup(html_content, 'html.parser').find_all(['li', 'dd']):
                text = li.get_text()
                if any(kw.lower() in text.lower() for kw in self.EMAIL_KEYWORDS):
                    if '@' in text or '＠' in text:
                        # Try full-width pattern first
                        for email in self.EMAIL_PATTERN_FULLWIDTH.findall(text):
                            candidates.append(EmailCandidate(email, 'list_item_email', url, context_snippet=text[:150].strip()))
                            logger.debug(f"Found email in list item: {email}")
                        # Then try regular pattern
                        for email in self.EMAIL_PATTERN.findall(text):
                            candidates.append(EmailCandidate(email, 'list_item_email', url, context_snippet=text[:150].strip()))
                            logger.debug(f"Found email in list item: {email}")
        except Exception as e:
            logger.error(f"Error detecting list emails: {e}")
        return candidates
    
    def _detect_table_emails(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    
                    # Check if first cell contains email keyword
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        if any(kw.lower() in label.lower() for kw in self.EMAIL_KEYWORDS):
                            if '@' in value or '＠' in value:
                                # Try full-width pattern first
                                for email in self.EMAIL_PATTERN_FULLWIDTH.findall(value):
                                    candidates.append(EmailCandidate(email, 'table_cell', url, selector='td/th', context_snippet=f"{label}: {value}"[:150]))
                                    logger.debug(f"Found email in table cell: {email}")
                                # Then try regular pattern
                                for email in self.EMAIL_PATTERN.findall(value):
                                    candidates.append(EmailCandidate(email, 'table_cell', url, selector='td/th', context_snippet=f"{label}: {value}"[:150]))
                                    logger.debug(f"Found email in table cell: {email}")
                    
                    # Also check all cells individually
                    for cell in cells:
                        text = cell.get_text()
                        if '@' in text or '＠' in text:
                            # Try full-width pattern first
                            for email in self.EMAIL_PATTERN_FULLWIDTH.findall(text):
                                candidates.append(EmailCandidate(email, 'table_cell', url, selector='td/th', context_snippet=text[:150]))
                                logger.debug(f"Found email in table cell: {email}")
                            # Then try regular pattern
                            for email in self.EMAIL_PATTERN.findall(text):
                                candidates.append(EmailCandidate(email, 'table_cell', url, selector='td/th', context_snippet=text[:150]))
                                logger.debug(f"Found email in table cell: {email}")
        except Exception as e:
            logger.error(f"Error detecting table emails: {e}")
        return candidates
    
    def _detect_plain_emails(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            # Try full-width pattern first
            for email in self.EMAIL_PATTERN_FULLWIDTH.findall(html_content):
                candidates.append(EmailCandidate(email, 'regex_plain', url, context_snippet=self._get_context_snippet(html_content, email)))
                logger.debug(f"Found plain email (full-width): {email}")
            # Then try regular pattern
            for email in self.EMAIL_PATTERN.findall(html_content):
                candidates.append(EmailCandidate(email, 'regex_plain', url, context_snippet=self._get_context_snippet(html_content, email)))
                logger.debug(f"Found plain email: {email}")
        except Exception as e:
            logger.error(f"Error detecting plain emails: {e}")
        return candidates
    
    def _detect_emails_from_text(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            for line in BeautifulSoup(html_content, 'html.parser').get_text().split('\n'):
                line = line.strip()
                if line and len(line) >= 5:
                    if '@' in line or '＠' in line:
                        # Try full-width pattern first
                        for email in self.EMAIL_PATTERN_FULLWIDTH.findall(line):
                            candidates.append(EmailCandidate(email, 'text_scanning', url, context_snippet=line[:100]))
                            logger.debug(f"Found email via text scan (full-width): {email}")
                        # Then try regular pattern
                        for email in self.EMAIL_PATTERN.findall(line):
                            candidates.append(EmailCandidate(email, 'text_scanning', url, context_snippet=line[:100]))
                            logger.debug(f"Found email via text scan: {email}")
        except Exception as e:
            logger.error(f"Error detecting emails from text: {e}")
        return candidates
    
    def _detect_dom_inspection(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for selector in ['div[class*="email"]', 'span[class*="email"]', 'div[class*="contact"]', 'span[class*="contact"]']:
                for element in soup.select(selector):
                    text = element.get_text()
                    if '@' in text or '＠' in text:
                        # Try full-width pattern first
                        for email in self.EMAIL_PATTERN_FULLWIDTH.findall(text):
                            candidates.append(EmailCandidate(email, 'dom_inspection', url, selector=selector, context_snippet=text[:100]))
                            logger.debug(f"Found email via DOM (full-width): {email}")
                        # Then try regular pattern
                        for email in self.EMAIL_PATTERN.findall(text):
                            candidates.append(EmailCandidate(email, 'dom_inspection', url, selector=selector, context_snippet=text[:100]))
                            logger.debug(f"Found email via DOM: {email}")
        except Exception as e:
            logger.error(f"Error in DOM inspection: {e}")
        return candidates
    
    def _detect_jsonld_schema(self, html_content: str, url: str) -> List[EmailCandidate]:
        candidates = []
        try:
            for script in BeautifulSoup(html_content, 'html.parser').find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string) if script.string else {}
                    for email in self._extract_emails_from_json(data):
                        candidates.append(EmailCandidate(email, 'jsonld_schema', url, selector='script[type="application/ld+json"]'))
                        logger.debug(f"Found JSON-LD email: {email}")
                except (json.JSONDecodeError, TypeError):
                    pass
        except Exception as e:
            logger.error(f"Error detecting JSON-LD emails: {e}")
        return candidates
    
    def _extract_emails_from_json(self, data: any, emails: Optional[Set[str]] = None) -> Set[str]:
        if emails is None:
            emails = set()
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, str) and self.EMAIL_PATTERN.match(v):
                    emails.add(v)
                elif isinstance(v, (dict, list)):
                    self._extract_emails_from_json(v, emails)
        elif isinstance(data, list):
            for item in data:
                self._extract_emails_from_json(item, emails)
        return emails
    
    def _normalize_email(self, email: str) -> Optional[str]:
        """Extract valid email from malformed text, handling full-width characters."""
        if not email:
            return None
        
        # First, convert full-width characters to half-width
        email = email.replace('＠', '@')
        email = email.replace('．', '.')
        
        # Remove common Japanese parentheses/annotations
        email = re.sub(r'[（）()]', '', email)
        email = re.sub(r'代表|担当|事務局', '', email)
        
        if '@' not in email:
            return None
        
        at_pos = email.find('@')
        domain = email[at_pos+1:]
        potential_local = email[:at_pos]
        
        # Find FIRST letter (emails must have letters, phone numbers don't)
        local_start = -1
        for i, char in enumerate(potential_local):
            if char.isalpha():
                candidate = potential_local[i:]
                if re.match(r'^[a-zA-Z][a-zA-Z0-9._%-]*$', candidate):
                    local_start = i
                    break
        
        # Fallback: take last sequence of valid email chars
        if local_start == -1:
            for i in range(len(potential_local)-1, -1, -1):
                if potential_local[i].isalnum() or potential_local[i] in '._%-+':
                    j = i
                    while j >= 0 and (potential_local[j].isalnum() or potential_local[j] in '._%-+'):
                        j -= 1
                    local_start = j + 1
                    break
        
        local_part = potential_local[local_start:] if local_start >= 0 else potential_local
        
        try:
            normalized = (local_part + '@' + domain).lower().strip()
            normalized = html.unescape(normalized)
            normalized = unicodedata.normalize('NFKC', normalized)
            normalized = re.sub(r'^mailto:', '', normalized, flags=re.I)
            normalized = re.sub(r'\?.*$', '', normalized)
            
            for pattern, replacement in self.OBFUSCATION_PATTERNS:
                normalized = re.sub(pattern, replacement, normalized, flags=re.I)
            
            return normalized if self.EMAIL_PATTERN.match(normalized) and len(normalized) > 5 else None
        except:
            return None
    
    def _validate_email(self, email: str) -> bool:
        if not email or not self.EMAIL_PATTERN.match(email):
            return False
        return not any(re.search(p, email, re.I) for p in [r'test@test', r'@test\.', r'noreply', r'no-reply', r'placeholder', r'dummy@', r'@dummy'])
    
    def _score_candidate(self, candidate: EmailCandidate, html_content: str) -> float:
        score = {'mailto_link': 45, 'definition_list': 38, 'label_value_pair': 37, 'label_value_container': 36, 'table_cell': 35, 'list_item_email': 32, 'jsonld_schema': 35, 'dom_inspection': 30, 'regex_plain': 25, 'text_scanning': 20}.get(candidate.detection_method, 10)
        
        try:
            footer = BeautifulSoup(html_content, 'html.parser').find('footer') or BeautifulSoup(html_content, 'html.parser').find(id='footer')
            if footer and candidate.email.lower() in footer.get_text().lower():
                score += 15
        except:
            pass
        
        if self._is_same_domain(candidate.email):
            score += 30
            logger.debug(f"  +30 same domain bonus for {candidate.email}")
        
        if candidate.context_snippet:
            keywords = ['mail', 'email', 'contact', 'inquiry', 'メール', 'お問い合わせ', '連絡先']
            matches = sum(1 for kw in keywords if kw.lower() in candidate.context_snippet.lower())
            if matches > 0:
                bonus = min(20, 5 * matches)
                score += bonus
                logger.debug(f"  +{bonus} context bonus for {candidate.email}")
        
        if candidate.detection_method == 'obfuscated_pattern':
            score -= 10
        
        return max(0.0, min(1.0, score / 100.0))
    
    def _is_same_domain(self, email: str) -> bool:
        try:
            email_domain = email.split('@')[1].lstrip('www.')
            page_domain = self.domain.lstrip('www.')
            return email_domain == page_domain
        except:
            return False
    
    def _get_context_snippet(self, html_content: str, email: str, context_size: int = 50) -> str:
        try:
            idx = html_content.lower().find(email.lower())
            if idx != -1:
                start = max(0, idx - context_size)
                end = min(len(html_content), idx + len(email) + context_size)
                return re.sub(r'\s+', ' ', html_content[start:end]).strip()
        except:
            pass
        return ''
    
    def close(self):
        pass