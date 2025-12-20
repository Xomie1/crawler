"""
HTML parsing utilities
Handles link extraction, form detection, and email extraction.
"""

import re
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


class HTMLParser:
    """Handles HTML parsing operations."""
    
    # Email regex pattern
    EMAIL_PATTERN = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    )
    
    # Inquiry form keywords (multi-language support)
    INQUIRY_KEYWORDS = [
        # English
        'contact', 'inquiry', 'inquiry', 'consultation', 'consult', 'request',
        'form', 'message', 'reach', 'get in touch',
        # Japanese
        '問い合わせ', 'お問い合わせ', '相談', 'お問合せ', 'コンタクト',
        'フォーム', 'メッセージ', '連絡', 'お申し込み',
        # Common variations
        'contact us', 'contact-form', 'inquiry-form', 'contactform'
    ]
    
    def __init__(self, base_url: str = None):
        """
        Initialize HTML parser.
        
        Args:
            base_url: Base URL for resolving relative links (optional)
        """
        self.base_url = base_url
        self.parsed_base = urlparse(base_url) if base_url else None
    
    def parse_links(self, html_content: str, exclude_patterns: List[str] = None) -> Set[str]:
        """
        Extract all links from HTML content.
        
        Args:
            html_content: HTML content to parse
            exclude_patterns: List of URL patterns to exclude
            
        Returns:
            Set of absolute URLs
        """
        if exclude_patterns is None:
            exclude_patterns = []
        
        links = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all anchor tags
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                absolute_url = urljoin(self.base_url, href)
                
                # Skip if matches exclude pattern
                if any(pattern in absolute_url for pattern in exclude_patterns):
                    continue
                
                # Only include HTTP/HTTPS URLs from same domain
                parsed = urlparse(absolute_url)
                if parsed.scheme in ['http', 'https']:
                    if parsed.netloc == self.parsed_base.netloc:
                        links.add(absolute_url)
            
            logger.debug(f"Extracted {len(links)} links from {self.base_url}")
            return links
            
        except Exception as e:
            logger.error(f"Error parsing links from {self.base_url}: {e}")
            return set()
    
    def detect_forms(self, html_content: str) -> List[str]:
        """
        Detect inquiry/contact forms in HTML.
        
        Args:
            html_content: HTML content to parse
            
        Returns:
            List of form URLs (absolute URLs)
        """
        form_urls = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all form tags
            forms = soup.find_all('form', action=True)
            
            for form in forms:
                action = form.get('action', '')
                if not action:
                    continue
                
                # Check form attributes and content for inquiry keywords
                form_text = form.get_text().lower()
                form_id = form.get('id', '').lower()
                form_class = ' '.join(form.get('class', [])).lower()
                form_name = form.get('name', '').lower()
                
                # Combine all text for keyword matching
                combined_text = f"{form_text} {form_id} {form_class} {form_name}"
                
                # Check for inquiry keywords
                if any(keyword.lower() in combined_text for keyword in self.INQUIRY_KEYWORDS):
                    absolute_url = urljoin(self.base_url, action)
                    form_urls.append(absolute_url)
                    logger.debug(f"Detected inquiry form: {absolute_url}")
                    continue
                
                # Check button labels
                buttons = form.find_all(['button', 'input'], type=['submit', 'button'])
                for button in buttons:
                    button_text = button.get_text().lower()
                    button_value = button.get('value', '').lower()
                    
                    if any(keyword.lower() in button_text or keyword.lower() in button_value 
                           for keyword in self.INQUIRY_KEYWORDS):
                        absolute_url = urljoin(self.base_url, action)
                        form_urls.append(absolute_url)
                        logger.debug(f"Detected inquiry form via button: {absolute_url}")
                        break
            
            # Also check for links that might lead to forms
            links = soup.find_all('a', href=True)
            for link in links:
                link_text = link.get_text().lower()
                href = link.get('href', '')
                
                if any(keyword.lower() in link_text for keyword in self.INQUIRY_KEYWORDS):
                    absolute_url = urljoin(self.base_url, href)
                    # Check if it's likely a form page
                    if any(pattern in absolute_url.lower() for pattern in ['form', 'contact', 'inquiry', '問い合わせ']):
                        form_urls.append(absolute_url)
                        logger.debug(f"Detected inquiry form link: {absolute_url}")
            
            return list(set(form_urls))  # Remove duplicates
            
        except Exception as e:
            logger.error(f"Error detecting forms from {self.base_url}: {e}")
            return []
    
    def extract_emails(self, html_content: str) -> Set[str]:
        """
        Extract email addresses from HTML content.
        
        Args:
            html_content: HTML content to parse
            
        Returns:
            Set of normalized email addresses
        """
        emails = set()
        
        try:
            # Find emails in text content
            text_emails = self.EMAIL_PATTERN.findall(html_content)
            
            # Also check mailto links
            soup = BeautifulSoup(html_content, 'html.parser')
            mailto_links = soup.find_all('a', href=re.compile(r'^mailto:', re.I))
            
            for link in mailto_links:
                href = link.get('href', '')
                # Extract email from mailto: link
                match = re.search(self.EMAIL_PATTERN, href)
                if match:
                    text_emails.append(match.group())
            
            # Normalize and deduplicate emails
            for email in text_emails:
                normalized = self._normalize_email(email)
                if normalized:
                    emails.add(normalized)
            
            logger.debug(f"Extracted {len(emails)} emails from {self.base_url}")
            return emails
            
        except Exception as e:
            logger.error(f"Error extracting emails from {self.base_url}: {e}")
            return set()
    
    def extract_metadata(self, html_content: str) -> dict:
        """
        Extract website metadata (company name, industry, etc.).
        
        Args:
            html_content: HTML content to parse
            
        Returns:
            Dictionary with metadata
        """
        metadata = {
            'companyName': None,
            'industry': None
        }
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract company name from title or meta tags
            title_tag = soup.find('title')
            if title_tag:
                title = title_tag.get_text().strip()
                # Try to extract company name (remove common suffixes)
                company_name = re.sub(r'\s*[-|]\s*(.*)$', '', title).strip()
                metadata['companyName'] = company_name if company_name else None
            
            # Try meta tags
            meta_title = soup.find('meta', property='og:title')
            if meta_title and not metadata['companyName']:
                metadata['companyName'] = meta_title.get('content', '').strip()
            
            # Extract industry from meta tags or structured data
            meta_description = soup.find('meta', {'name': 'description'})
            if meta_description:
                description = meta_description.get('content', '').lower()
                # Simple industry detection (can be enhanced)
                industry_keywords = {
                    'technology': ['tech', 'software', 'it', 'technology'],
                    'finance': ['finance', 'banking', 'financial', 'investment'],
                    'retail': ['retail', 'shop', 'store', 'e-commerce'],
                    'healthcare': ['health', 'medical', 'hospital', 'clinic'],
                    'education': ['education', 'school', 'university', 'learning'],
                    'manufacturing': ['manufacturing', 'factory', 'production'],
                }
                
                for industry, keywords in industry_keywords.items():
                    if any(keyword in description for keyword in keywords):
                        metadata['industry'] = industry
                        break
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata from {self.base_url}: {e}")
            return metadata
    
    @staticmethod
    def _normalize_email(email: str) -> Optional[str]:
        """
        Normalize email address.
        
        Args:
            email: Raw email string
            
        Returns:
            Normalized email or None if invalid
        """
        if not email:
            return None
        
        # Convert to lowercase and strip whitespace
        normalized = email.lower().strip()
        
        # Basic validation
        if '@' not in normalized or '.' not in normalized.split('@')[1]:
            return None
        
        # Remove common prefixes/suffixes
        normalized = re.sub(r'^mailto:', '', normalized, flags=re.I)
        normalized = re.sub(r'\?.*$', '', normalized)  # Remove query params
        
        return normalized if len(normalized) > 5 else None

