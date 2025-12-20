"""
Company Name Extractor v8 - PRIVACY & ABOUT PAGE PRIORITY
Focuses on privacy/about pages for formal legal declarations first
"""

import re
import unicodedata
import logging
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class CompanyNameCandidate:
    """Represents a company name candidate with metadata."""
    
    def __init__(self, value: str, source: str, confidence: float, method: str = "", has_legal_entity: bool = False):
        self.value = value
        self.source = source
        self.confidence = confidence
        self.method = method
        self.has_legal_entity = has_legal_entity
    
    def to_dict(self) -> Dict:
        return {
            'value': self.value,
            'source': self.source,
            'confidence': self.confidence,
            'method': self.method,
            'has_legal_entity': self.has_legal_entity
        }


class EnhancedCompanyNameExtractor:
    """Extract company names with privacy/about page priority."""
    
    LEGAL_ENTITIES = [
        '株式会社', '有限会社', '合同会社', '合資会社', '合名会社',
        '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
        '特定非営利活動法人', '学校法人', '医療法人', '社会医療法人',
        '社会福祉法人', '宗教法人', '労働組合', '組合'
    ]
    
    COMPANY_NAME_PATTERN = re.compile(
        r'(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|'
        r'公益社団法人|公益財団法人|特定非営利活動法人|学校法人|医療法人|'
        r'社会医療法人|社会福祉法人|宗教法人|労働組合|組合)'
        r'\s*([ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{1,50})?',
        re.UNICODE
    )
    
    FORMAL_DECLARATION_PATTERNS = [
        r'(株式会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{2,40}?)[は|が|を]',
        r'(有限会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{2,40}?)[は|が|を]',
        r'(合同会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{2,40}?)[は|が|を]',
        r'(株式会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{2,40}?)(?:の|です|ます|以下)',
    ]
    
    LABELED_FIELD_PATTERNS = [
        r'商号\s*[:：]\s*([^\n\r,。、]+)',
        r'会社名\s*[:：]\s*([^\n\r,。、]+)',
        r'法人名\s*[:：]\s*([^\n\r,。、]+)',
        r'企業名\s*[:：]\s*([^\n\r,。、]+)',
        r'正式名称\s*[:：]\s*([^\n\r,。、]+)',
        r'Company Name\s*[:：]\s*([^\n\r,。、]+)',
        r'Corporate Name\s*[:：]\s*([^\n\r,。、]+)',
    ]
    
    PRIVACY_PAGE_PATTERNS = ['/privacy.html', '/privacy/', '/privacy-policy', '/privacy.php']
    ABOUT_PAGE_PATTERNS = ['/aboutus.html', '/aboutus', '/about.html', '/about/', '/company/info.html', '/gaiyou']
    COMMON_PATTERNS = ['/company/', '/company', '/corporate/', '/corporate', '/profile/', '/profile', '/terms/', '/terms']
    
    GARBAGE_SUFFIXES = [
        'からの独立', 'の要項', 'の事業', 'のアクセス', 'の会社', 'を含む', 'グループ会社',
    ]
    
    def __init__(self, base_url: str, fetcher=None):
        self.base_url = base_url
        self.fetcher = fetcher
    
    def extract(self, html_content: str, final_url: Optional[str] = None) -> Dict:
        """Extract company name with privacy/about page priority."""
        url = final_url or self.base_url
        candidates: List[CompanyNameCandidate] = []
        
        print("=" * 80)
        print(f"Extracting from: {url}")
        print("=" * 80)
        
        # Phase 1: Homepage
        print("\nPHASE 1: Homepage")
        homepage_candidates = self._extract_from_homepage(html_content)
        if homepage_candidates:
            for c in homepage_candidates:
                print(f"  Found: {c.value} (conf: {c.confidence:.2f})")
            candidates.extend(homepage_candidates)
        
        # Phase 2: Privacy & About pages (HIGHEST PRIORITY)
        if self.fetcher:
            print("\nPHASE 2: Privacy & About pages")
            privacy_candidates = self._fetch_pages(html_content, url, self.PRIVACY_PAGE_PATTERNS + self.ABOUT_PAGE_PATTERNS, 'privacy_about')
            candidates.extend(privacy_candidates)
            
            # Phase 3: Other company pages
            print("\nPHASE 3: Other company pages")
            company_candidates = self._fetch_pages(html_content, url, self.COMMON_PATTERNS, 'company')
            candidates.extend(company_candidates)
        
        return self._select_best_candidate(candidates)
    
    def _extract_from_homepage(self, html_content: str) -> List[CompanyNameCandidate]:
        """Extract legal entities from homepage."""
        results = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            title = soup.find('title')
            if title and title.string:
                for match in self.COMPANY_NAME_PATTERN.finditer(title.string.strip()):
                    entity, name_part = match.group(1), match.group(2) or ''
                    full = (entity + name_part).strip()
                    cleaned = self._clean(full)
                    
                    if cleaned and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        results.append(CompanyNameCandidate(cleaned, 'homepage_title', 0.88, 'regex', True))
        except Exception as e:
            logger.error(f"Homepage extraction error: {e}")
        
        return results
    
    def _fetch_pages(self, html_content: str, base_url: str, patterns: List[str], source_type: str) -> List[CompanyNameCandidate]:
        """Fetch and extract from pages matching patterns."""
        results = []
        if not self.fetcher:
            return results
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = set()
            
            # Find matching links - be aggressive about finding company/about/privacy pages
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                link_text = link.get_text().lower()
                
                for pattern in patterns:
                    if pattern in href:
                        links.add(urljoin(base_url, link['href']))
                        break
                    # Also check text content for company/about keywords
                    if source_type == 'privacy_about' and any(kw in link_text for kw in ['会社', 'プライバシー', 'about', 'privacy']):
                        if 'contact' not in href and 'form' not in href:
                            links.add(urljoin(base_url, link['href']))
                            break
            
            # Add common URLs
            base_norm = base_url.rstrip('/')
            for pattern in patterns:
                links.add(base_norm + pattern)
            
            print(f"  Found {len(links)} links")
            
            # Prioritize based on source type
            if source_type == 'privacy_about':
                priority = ['privacy', 'aboutus', 'about', 'company/info', 'gaiyou', 'company']
            else:
                priority = ['company', 'corporate', 'profile', 'terms']
            
            sorted_links = sorted(links, key=lambda x: next((i for i, p in enumerate(priority) if p in x.lower()), 999))
            
            for url in sorted_links[:15]:
                try:
                    content, status, _, _ = self.fetcher.fetch_page(url)
                    if status == 200 and content:
                        print(f"    Fetching: {url}")
                        page_results = self._extract_from_page(content, url, source_type)
                        results.extend(page_results)
                        
                        # Stop if found formal declaration or labeled field
                        if any(r.method in ['formal_declaration', 'labeled_field'] for r in page_results):
                            print(f"    [Found high-confidence match - stopping]")
                            break
                except Exception as e:
                    logger.debug(f"Fetch error {url}: {e}")
        except Exception as e:
            logger.error(f"Page fetch error: {e}")
        
        return results
    
    def _extract_from_page(self, html_content: str, page_url: str, source_type: str) -> List[CompanyNameCandidate]:
        """Extract company names from page."""
        results = []
        seen = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            text = soup.get_text()
            page_type = self._get_page_type(page_url)
            
            # Strategy 1: Formal declarations (highest confidence)
            for pattern in self.FORMAL_DECLARATION_PATTERNS:
                for match in re.finditer(pattern, text, re.MULTILINE):
                    name = match.group(1).strip()
                    cleaned = self._clean(name)
                    
                    if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_{page_type}', 0.98, 'formal_declaration', 
                            any(e in cleaned for e in self.LEGAL_ENTITIES)
                        ))
            
            # Strategy 2: Labeled fields in tables
            if not results:
                for table in soup.find_all('table'):
                    for row in table.find_all('tr'):
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            label = cells[0].get_text().strip()
                            value = cells[1].get_text().strip()
                            
                            if any(kw in label for kw in ['商号', '会社名', '法人名', '企業名', '正式名称']):
                                cleaned = self._clean(value)
                                if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                                    seen.add(cleaned)
                                    results.append(CompanyNameCandidate(
                                        cleaned, f'{source_type}_{page_type}', 0.97, 'labeled_field',
                                        any(e in cleaned for e in self.LEGAL_ENTITIES)
                                    ))
            
            # Strategy 3: Inline labeled fields
            if not results:
                for pattern in self.LABELED_FIELD_PATTERNS:
                    for match in re.finditer(pattern, text, re.MULTILINE):
                        name = match.group(1).strip().split('\n')[0].split('。')[0].strip()
                        cleaned = self._clean(name)
                        
                        if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                            seen.add(cleaned)
                            results.append(CompanyNameCandidate(
                                cleaned, f'{source_type}_{page_type}', 0.96, 'labeled_field',
                                any(e in cleaned for e in self.LEGAL_ENTITIES)
                            ))
            
            # Strategy 4: Regex patterns (fallback)
            if not results:
                for match in self.COMPANY_NAME_PATTERN.finditer(text):
                    entity, name_part = match.group(1), match.group(2) or ''
                    
                    if name_part:
                        name_part = re.split(r'[。、は が を\s]{1,5}|の[ぁ-ん]{2,}|[0-9]{4}年|から|より|設立|成立', name_part)[0].strip()
                    
                    full = (entity + name_part).strip()
                    cleaned = self._clean(full)
                    
                    if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_{page_type}', 0.94, 'regex', True
                        ))
        
        except Exception as e:
            logger.error(f"Page extraction error: {e}")
        
        return results
    
    def _select_best_candidate(self, candidates: List[CompanyNameCandidate]) -> Dict:
        """Select best candidate with priority rules."""
        result = {
            'company_name': None,
            'company_name_source': None,
            'company_name_confidence': 0.0,
            'company_name_method': None,
            'company_name_candidates': [c.to_dict() for c in candidates],
            'needs_ai_verification': False
        }
        
        if not candidates:
            print("\n[ERROR] No candidates found")
            return result
        
        print("\n" + "="*80)
        print("SELECTING BEST CANDIDATE")
        print("="*80)
        
        # Remove duplicates - keep highest confidence
        seen = {}
        for c in candidates:
            if c.value not in seen or c.confidence > seen[c.value].confidence:
                seen[c.value] = c
        
        unique = list(seen.values())
        cleaned = [c for c in unique if not self._is_garbage(c.value)]
        legal_only = [c for c in cleaned if c.has_legal_entity]
        
        print(f"After dedup/filter: {len(legal_only)}/{len(unique)} candidates")
        
        if not legal_only:
            result['needs_ai_verification'] = True
            return result
        
        for c in legal_only:
            print(f"  ✓ {c.value} ({c.confidence:.2f}) - {c.method} [{c.source}]")
        
        # Priority 1: Formal declarations from privacy/about
        formal_pa = [c for c in legal_only if c.method == 'formal_declaration' and 'privacy_about' in c.source]
        if formal_pa:
            best = sorted(formal_pa, key=lambda x: (-x.confidence, -len(x.value)))[0]
            reason = "Formal declaration from privacy/about (HIGHEST)"
        else:
            # Priority 2: Labeled fields from privacy/about
            labeled_pa = [c for c in legal_only if 'labeled_field' in c.method and 'privacy_about' in c.source]
            if labeled_pa:
                best = sorted(labeled_pa, key=lambda x: (-x.confidence, -len(x.value)))[0]
                reason = "Labeled field from privacy/about (VERY HIGH)"
            else:
                # Priority 3: Labeled fields from any page
                labeled_any = [c for c in legal_only if 'labeled_field' in c.method]
                if labeled_any:
                    best = sorted(labeled_any, key=lambda x: (-x.confidence, -len(x.value)))[0]
                    reason = "Labeled field (HIGH)"
                else:
                    # Priority 4: Complete entities from privacy/about
                    complete_pa = [c for c in legal_only if 'privacy_about' in c.source and 8 <= len(c.value) <= 40]
                    if complete_pa:
                        best = sorted(complete_pa, key=lambda x: (-x.confidence, -len(x.value)))[0]
                        reason = "Complete entity from privacy/about"
                    else:
                        # Priority 5: Complete entities from other pages
                        complete = [c for c in legal_only if 8 <= len(c.value) <= 40]
                        if complete:
                            best = sorted(complete, key=lambda x: (-x.confidence, -len(x.value)))[0]
                            reason = "Complete entity from company page"
                        else:
                            best = sorted(legal_only, key=lambda x: (-x.confidence, -len(x.value)))[0]
                            reason = "Legal entity (fallback)"
        
        print(f"\n[FINAL] {best.value}")
        print(f"        Confidence: {best.confidence:.2f}")
        print(f"        Reason: {reason}")
        
        result['company_name'] = best.value
        result['company_name_source'] = best.source
        result['company_name_confidence'] = best.confidence
        result['company_name_method'] = best.method
        result['needs_ai_verification'] = best.confidence < 0.90
        
        return result
    
    def _clean(self, text: str) -> str:
        """Clean and normalize company name."""
        if not text:
            return ''
        text = unicodedata.normalize('NFKC', text)
        text = re.sub(r'[\n\r]+', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()  # Normalize multiple spaces to single space
        text = re.sub(r'^©\s*|&copy;\s*', '', text)
        return text.strip()
    
    def _is_valid(self, name: str) -> bool:
        """Check if name is valid format."""
        if not name or len(name) < 3 or len(name) > 40:
            return False
        
        jp_chars = len(re.findall(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]', name))
        latin_chars = len(re.findall(r'[a-zA-Z]', name))
        
        if jp_chars == 0 and latin_chars == 0:
            return False
        
        if any(word in name for word in ['ページ', 'トップ', 'contact', 'gallery', 'メール']):
            return False
        
        return True
    
    def _is_garbage(self, name: str) -> bool:
        """Check if name is garbage."""
        if not name or len(name) > 40:
            return True
        
        if any(suffix in name for suffix in self.GARBAGE_SUFFIXES):
            return True
        
        if sum(1 for e in self.LEGAL_ENTITIES if e in name) > 1:
            return True
        
        return False
    
    def _get_page_type(self, url: str) -> str:
        """Determine page type from URL."""
        url_lower = url.lower()
        
        if any(x in url_lower for x in ['privacy']):
            return 'privacy'
        elif any(x in url_lower for x in ['about', 'gaiyou', 'outline']):
            return 'about'
        elif any(x in url_lower for x in ['company', 'corporate']):
            return 'company'
        elif any(x in url_lower for x in ['terms', 'legal']):
            return 'terms'
        else:
            return 'other'