"""
Company Name Extractor v9 - FIXED TABLE EXTRACTION PRIORITY
Prioritizes company info tables, fixes early stopping bug
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
    """Extract company names with info page table priority."""
    
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
        r'(株式会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{2,60}?)(?=は|が|を|。|、|\n)',
        r'(有限会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{2,60}?)(?=は|が|を|。|、|\n)',
        r'(合同会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]{2,60}?)(?=は|が|を|。|、|\n)',
        r'会社名\s*[:：]\s*(株式会社\s+[ぁ-ん ァ-ヴー\u4e00-\u9fff0-9・\-\s]+?)(?:\n|。|、)',
    ]
    
    LABELED_FIELD_PATTERNS = [
        # Match: label + colon + legal entity + optional name (2-25 chars total after entity)
        r'商号\s*[:：]\s*((?:株式会社|有限会社|合同会社|合資会社|合名会社)[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ffA-Za-z0-9ー]{0,25}?)(?=相談所名|連絡先|代表者名|住所|所在地|電話|FAX|\s{2,}|$)',
        r'会社名\s*[:：]\s*((?:株式会社|有限会社|合同会社|合資会社|合名会社)[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ffA-Za-z0-9ー]{0,25}?)(?=相談所名|連絡先|代表者名|住所|所在地|電話|FAX|\s{2,}|$)',
        r'法人名\s*[:：]\s*((?:株式会社|有限会社|合同会社)[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ffA-Za-z0-9ー]{0,25}?)(?=相談所名|連絡先|代表者名|住所|所在地|電話|FAX|\s{2,}|$)',
        r'企業名\s*[:：]\s*((?:株式会社|有限会社)[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ffA-Za-z0-9ー]{0,25}?)(?=相談所名|連絡先|代表者名|住所|所在地|電話|FAX|\s{2,}|$)',
        r'正式名称\s*[:：]\s*((?:株式会社|有限会社)[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ffA-Za-z0-9ー]{0,25}?)(?=相談所名|連絡先|代表者名|住所|所在地|電話|FAX|\s{2,}|$)',
    ]
    
    PRIVACY_PAGE_PATTERNS = ['/privacy.html', '/privacy/', '/privacy-policy', '/privacy.php']
    ABOUT_PAGE_PATTERNS = ['/aboutus.html', '/aboutus', '/about.html', '/about/', '/company/info.html', '/gaiyou']
    COMMON_PATTERNS = ['/company/', '/company', '/corporate/', '/corporate', '/profile/', '/profile', '/terms/', '/terms']
    
    GARBAGE_SUFFIXES = [
        'からの独立', 'の要項', 'の事業', 'のアクセス', 'の会社', 'を含む', 'グループ会社',
        '代表', '社長', '住所', '屋号', '事業内容', 'についての', '経営陣', '最終改定',
        'の提供', 'に直接', 'の定める', 'に同意', 'について', 'の情報', '入社',
        'のミッション', 'それ', 'これ', '所在地', 'の理念', 'の目標', 'の方針',
        '協会', '協議会', '推進協会', '推進会', 'センター', '研究所', '財団', 'を運営'
    ]
    
    # Patterns that indicate this is likely NOT the main company
    ASSOCIATION_PATTERNS = [
        '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
        '協会', '協議会', '推進協会', '連盟', 'センター', '研究所',
        '組合', '共同組合', '事業協同組合'
    ]
    
    def __init__(self, base_url: str, fetcher=None):
        self.base_url = base_url
        self.fetcher = fetcher
    
    def extract(self, html_content: str, final_url: Optional[str] = None) -> Dict:
        """Extract company name with info page table priority."""
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
        
        # Phase 2: Company info pages FIRST (highest priority for tables)
        if self.fetcher:
            print("\nPHASE 2: Company info pages (TABLE PRIORITY)")
            info_candidates = self._fetch_info_pages(html_content, url)
            candidates.extend(info_candidates)
            
            # Phase 3: Privacy & About pages
            print("\nPHASE 3: Privacy & About pages")
            privacy_candidates = self._fetch_pages(html_content, url, self.PRIVACY_PAGE_PATTERNS + self.ABOUT_PAGE_PATTERNS, 'privacy_about')
            candidates.extend(privacy_candidates)
            
            # Phase 4: Other company pages
            print("\nPHASE 4: Other company pages")
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
    
    def _fetch_info_pages(self, html_content: str, base_url: str) -> List[CompanyNameCandidate]:
        """Fetch company info pages first - they have the best table data."""
        results = []
        if not self.fetcher:
            return results
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            info_urls = set()
            
            # Find info/outline/company profile pages
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                link_text = link.get_text().lower()
                
                # Prioritize pages that typically have company tables
                if any(x in href for x in ['info.html', 'outline', 'profile', 'gaiyou', 'summary']):
                    info_urls.add(urljoin(base_url, link['href']))
                elif any(kw in link_text for kw in ['会社概要', '会社情報', '企業情報', 'company info']):
                    if 'contact' not in href and 'form' not in href:
                        info_urls.add(urljoin(base_url, link['href']))
            
            # Add common info URLs
            base_norm = base_url.rstrip('/')
            info_urls.add(base_norm + '/company/info.html')
            info_urls.add(base_norm + '/company/outline.html')
            info_urls.add(base_norm + '/company/profile.html')
            info_urls.add(base_norm + '/company/gaiyou.html')
            
            print(f"  Found {len(info_urls)} info page URLs")
            
            # Sort to prioritize info.html
            sorted_urls = sorted(info_urls, key=lambda x: (
                0 if 'info.html' in x else
                1 if 'outline' in x or 'gaiyou' in x else
                2 if 'profile' in x else
                3
            ))
            
            for url in sorted_urls[:10]:
                try:
                    content, status, _, _ = self.fetcher.fetch_page(url)
                    if status == 200 and content:
                        print(f"    Fetching: {url}")
                        page_results = self._extract_from_page(content, url, 'company_info')
                        results.extend(page_results)
                        
                        # Stop if we found a high-quality table match
                        table_matches = [r for r in page_results if r.method == 'table_field' and r.confidence >= 0.99]
                        if table_matches:
                            print(f"    [✓ Found high-quality table match - stopping info page search]")
                            break
                except Exception as e:
                    logger.debug(f"Fetch error {url}: {e}")
        except Exception as e:
            logger.error(f"Info page fetch error: {e}")
        
        return results
    
    def _fetch_pages(self, html_content: str, base_url: str, patterns: List[str], source_type: str) -> List[CompanyNameCandidate]:
        """Fetch and extract from pages matching patterns."""
        results = []
        if not self.fetcher:
            return results
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = set()
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                link_text = link.get_text().lower()
                
                for pattern in patterns:
                    if pattern in href:
                        links.add(urljoin(base_url, link['href']))
                        break
                    if source_type == 'privacy_about' and any(kw in link_text for kw in ['会社', 'プライバシー', 'about', 'privacy']):
                        if 'contact' not in href and 'form' not in href:
                            links.add(urljoin(base_url, link['href']))
                            break
            
            base_norm = base_url.rstrip('/')
            for pattern in patterns:
                links.add(base_norm + pattern)
            
            print(f"  Found {len(links)} links")
            
            if source_type == 'privacy_about':
                priority = ['privacy', 'aboutus', 'about', 'company']
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
            
            # Strategy 1: TABLE EXTRACTION (HIGHEST PRIORITY for info pages)
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text().strip()
                        value = cells[1].get_text().strip()
                        
                        # Check for company name labels
                        if any(kw in label for kw in ['商号', '会社名', '法人名', '企業名', '正式名称', 'Company Name', 'Corporate Name']):
                            cleaned = self._clean(value)
                            
                            # CRITICAL: Verify it contains a legal entity
                            has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                            
                            if cleaned and has_legal and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                                seen.add(cleaned)
                                confidence = 0.99 if source_type == 'company_info' else 0.97
                                results.append(CompanyNameCandidate(
                                    cleaned, f'{source_type}_{page_type}', confidence, 'table_field', True
                                ))
                                print(f"      [TABLE] {cleaned}")
            
            # If we found table results on info pages, prioritize them
            if results and source_type == 'company_info':
                return results
            
            # Strategy 2: Formal declarations
            for pattern in self.FORMAL_DECLARATION_PATTERNS:
                for match in re.finditer(pattern, text, re.MULTILINE):
                    name = match.group(1).strip() if match.lastindex >= 1 else match.group(0).strip()
                    
                    if len(name) > 50:
                        name = re.split(r'\n|。|、|  |（', name)[0].strip()
                    
                    cleaned = self._clean(name)
                    
                    if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_{page_type}', 0.98, 'formal_declaration', 
                            any(e in cleaned for e in self.LEGAL_ENTITIES)
                        ))
            
            # Strategy 3: Inline labeled fields (ALWAYS CHECK - don't skip!)
            for pattern in self.LABELED_FIELD_PATTERNS:
                for match in re.finditer(pattern, text, re.MULTILINE):
                    name = match.group(1).strip().split('\n')[0].split('。')[0].strip()
                    cleaned = self._clean(name)
                    
                    print(f"      [LABELED] Raw: '{name}' -> Cleaned: '{cleaned}'")
                    
                    if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_{page_type}', 0.96, 'labeled_field',
                            any(e in cleaned for e in self.LEGAL_ENTITIES)
                        ))
                    elif cleaned:
                        if cleaned in seen:
                            print(f"        -> Skipped (duplicate)")
                        elif not self._is_valid(cleaned):
                            print(f"        -> Skipped (invalid)")
                        elif self._is_garbage(cleaned):
                            print(f"        -> Skipped (garbage)")
            
            # Strategy 4: Regex patterns (fallback - only if no better results)
            if not results:
                for match in self.COMPANY_NAME_PATTERN.finditer(text):
                    entity, name_part = match.group(1), match.group(2) or ''
                    
                    if name_part:
                        name_part = re.split(r'[。、は が を\s]{1,5}|の[ぁ-ん]{2,}|[0-9]{4}年|から|より|設立|成立', name_part)[0].strip()
                    
                    full = (entity + name_part).strip()
                    cleaned = self._clean(full)
                    
                    print(f"      [REGEX] Entity: '{entity}' + Name: '{name_part}' -> '{cleaned}'")
                    
                    if cleaned and 8 <= len(cleaned) <= 30 and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_{page_type}', 0.94, 'regex', True
                        ))
        
        except Exception as e:
            logger.error(f"Page extraction error: {e}")
        
        return results
    
    def _select_best_candidate(self, candidates: List[CompanyNameCandidate]) -> Dict:
        """Select best candidate with table priority and domain validation."""
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
        
        # Remove duplicates
        seen = {}
        for c in candidates:
            if c.value not in seen or c.confidence > seen[c.value].confidence:
                seen[c.value] = c
        
        unique = list(seen.values())
        
        # Extract domain name for matching
        domain_name = self._extract_domain_name(self.base_url)
        print(f"Domain extracted: {domain_name}")
        
        # Enhanced filtering
        cleaned = []
        for c in unique:
            # Basic filters
            if self._is_garbage(c.value):
                print(f"  ✗ Filtered (garbage): {c.value}")
                continue
            if not (8 <= len(c.value) <= 40):
                print(f"  ✗ Filtered (length): {c.value}")
                continue
            
            # Filter associations if they don't match domain
            if self._is_likely_association(c.value):
                if not self._matches_domain(c.value, domain_name):
                    print(f"  ✗ Filtered (association, no domain match): {c.value}")
                    continue
            
            cleaned.append(c)
        
        if not cleaned:
            cleaned = [c for c in unique if not self._is_garbage(c.value)]
        
        legal_only = [c for c in cleaned if c.has_legal_entity]
        
        print(f"\nAfter dedup/filter: {len(legal_only)}/{len(unique)} candidates")
        
        if not legal_only:
            result['needs_ai_verification'] = True
            return result
        
        # Apply domain boost
        for c in legal_only:
            domain_boost = ""
            if self._matches_domain(c.value, domain_name):
                c.confidence += 0.05  # Boost confidence for domain matches
                domain_boost = " [DOMAIN MATCH]"
            print(f"  ✓ {c.value} ({c.confidence:.2f}) - {c.method} [{c.source}]{domain_boost}")
        
        # Priority 1: Table fields from info pages (HIGHEST)
        table_info = [c for c in legal_only if c.method == 'table_field' and 'company_info' in c.source]
        if table_info:
            best = sorted(table_info, key=lambda x: (-x.confidence, len(x.value)))[0]  # Prefer SHORTER names
            reason = "Table field from company info page (HIGHEST PRIORITY)"
        else:
            # Priority 2: Any table field
            table_any = [c for c in legal_only if c.method == 'table_field']
            if table_any:
                best = sorted(table_any, key=lambda x: (-x.confidence, len(x.value)))[0]  # Prefer SHORTER names
                reason = "Table field (HIGH CONFIDENCE)"
            else:
                # Priority 3: Labeled fields
                labeled = [c for c in legal_only if 'labeled_field' in c.method]
                if labeled:
                    best = sorted(labeled, key=lambda x: (-x.confidence, len(x.value)))[0]  # Prefer SHORTER names
                    reason = "Labeled field"
                else:
                    # Priority 4: Formal declarations
                    formal = [c for c in legal_only if c.method == 'formal_declaration']
                    if formal:
                        best = sorted(formal, key=lambda x: (-x.confidence, len(x.value)))[0]  # Prefer SHORTER names
                        reason = "Formal declaration"
                    else:
                        # Priority 5: Fallback
                        best = sorted(legal_only, key=lambda x: (-x.confidence, len(x.value)))[0]  # Prefer SHORTER names
                        reason = "Best match (fallback)"
        
        print(f"\n[FINAL] {best.value}")
        print(f"        Confidence: {best.confidence:.2f}")
        print(f"        Reason: {reason}")
        
        result['company_name'] = best.value
        result['company_name_source'] = best.source
        result['company_name_confidence'] = best.confidence
        result['company_name_method'] = best.method
        
        # Flag for AI verification if:
        # - Low confidence
        # - Long name (might be garbage)
        # - No domain match
        needs_ai = (
            best.confidence < 0.90 or
            len(best.value) > 20 or
            not self._matches_domain(best.value, domain_name)
        )
        result['needs_ai_verification'] = needs_ai
        
        return result
    
    def _clean(self, text: str) -> str:
        """Clean and normalize company name."""
        if not text:
            return ''
        text = unicodedata.normalize('NFKC', text)
        text = re.sub(r'[\n\r]+', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'^©\s*|&copy;\s*', '', text)
        return text.strip()
    
    def _is_valid(self, name: str) -> bool:
        """Check if name is valid format."""
        if not name or len(name) < 3 or len(name) > 60:
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
    
    def _extract_domain_name(self, url: str) -> str:
        """Extract core domain name for matching."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            
            # Remove www. and TLD
            domain = domain.replace('www.', '')
            domain = domain.split('.')[0]  # Get first part before .com/.co.jp etc
            
            # Convert to katakana/hiragana if possible (basic romanization matching)
            # For now, just return lowercase
            return domain.lower()
        except:
            return ""
    
    def _matches_domain(self, company_name: str, domain_name: str) -> bool:
        """Check if company name matches the domain."""
        if not domain_name:
            return False
        
        # Remove legal entity for comparison
        name_without_entity = company_name
        for entity in self.LEGAL_ENTITIES:
            name_without_entity = name_without_entity.replace(entity, '')
        
        name_without_entity = name_without_entity.strip()
        
        # Check if domain appears in company name (case insensitive)
        # Example: "crane-a" matches "クレイン"
        if domain_name in name_without_entity.lower():
            return True
        
        # Romanization mapping (basic - can be expanded)
        romanization_map = {
            'konanhanbai': 'コナン販売',
            'konan': 'コナン',
            'wedding-b': 'ウェディング',
            'webclub': 'ウェブ',
            'crane': 'クレイン',
            'cowa': '幸和',
            'globe': 'globe',
            'lamour': 'ラムール',
            'kma-h': 'KMA',
            'kma': 'KMA',
            'aics': 'アイクス',
            'asante-sana': 'アサンテサーナ',
            'fairlen': 'フェアレン',
            'nsjh': '日生情報',
            'ita-net': 'アイティーエー',
            'officesano': 'オフィスさの',
        }
        
        # Check if domain has a known romanization
        if domain_name in romanization_map:
            expected = romanization_map[domain_name]
            if expected in name_without_entity:
                return True
        
        return False
    
    def _is_likely_association(self, company_name: str) -> bool:
        """Check if this looks like an association/NPO rather than main company."""
        return any(pattern in company_name for pattern in self.ASSOCIATION_PATTERNS)
    
    def _get_page_type(self, url: str) -> str:
        """Determine page type from URL."""
        url_lower = url.lower()
        
        if any(x in url_lower for x in ['info', 'outline', 'gaiyou']):
            return 'info'
        elif any(x in url_lower for x in ['privacy']):
            return 'privacy'
        elif any(x in url_lower for x in ['about']):
            return 'about'
        elif any(x in url_lower for x in ['company', 'corporate']):
            return 'company'
        elif any(x in url_lower for x in ['terms', 'legal']):
            return 'terms'
        else:
            return 'other'