# -*- coding: utf-8 -*-
"""
Company Name Extractor v16 - Black Square Marker Strategy
- Added new extraction method for ■ (BLACK SQUARE) marker format
- Handles both malformed tables and properly structured lists
- Inserted before text pattern fallback for optimal priority
"""

import re, json, logging, unicodedata
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

class CompanyNameCandidate:
    def __init__(self, value: str, source: str, confidence: float, method: str = "", 
                 has_legal_entity: bool = False, is_auto_completed: bool = False):
        self.value = value
        self.source = source
        self.confidence = confidence
        self.method = method
        self.has_legal_entity = has_legal_entity
        self.is_auto_completed = is_auto_completed
    
    def to_dict(self) -> Dict:
        return {
            'value': self.value, 'source': self.source, 'confidence': self.confidence,
            'method': self.method, 'has_legal_entity': self.has_legal_entity,
            'is_auto_completed': self.is_auto_completed
        }

class EnhancedCompanyNameExtractor:
    
    LEGAL_ENTITIES = [
        '株式会社', '有限会社', '合同会社', '合資会社', '合名会社',
        '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
        '特定非営利活動法人', '学校法人', '医療法人', '社会医療法人',
        '社会福祉法人', '宗教法人', '労働組合', '組合',
        '行政書士', '弁護士', '法務書士', '税理士', '公認会計士'
    ]
    
    PRIMARY_COMPANY_LABELS = [
        '会社名', '商号', '法人名', '企業名', '正式名称', '名称', '社名',
        '事業者名', '法人の名称', '屋号', '法人名称', '運営会社', '運営法人',
        '事務所名', '事務所', '店舗名', '施設名', "商　号", "会 社 名", "称号", "社　名"
    ]
    
    SECONDARY_COMPANY_LABELS = [
        '名前', '会社', '名', 'Company', 'Name', 'company name'
    ]
    
    EXCLUDED_LABELS = [
        '項目', '住所', '価格', '料金', '費用', '時間', '金額',
        'item', 'price', 'cost', 'fee', 'amount', 'メディア名', '番組名', '放送局', 'タイトル', '出演',
        'media', 'program', 'title', 'show', 'broadcast', '加盟団体', '所属団体', 'affiliated', 'member of'
    ]
    
    SEO_SUFFIXES = [
        '保険調査', '調査会社',
        '不動産', '建設', 'コンサルティング', 'システム開発',
        '福岡', '東京', '大阪', '名古屋', '札幌', '仙台', '横浜', '京都', '神戸', '広島'
    ]
    
    GARBAGE = ['からの独立', 'の要項', 'の事業', 'のアクセス', 'の会社', 'を含む',
               'グループ会社', '代表', '社長', '住所', '屋号', '事業内容', '概要',
               'に相談', 'に伝える', 'ページトップ', 'へ戻る']
    
    def __init__(self, base_url: str, fetcher=None):
        self.base_url = base_url
        self.fetcher = fetcher
    
    def extract(self, html_content: str, final_url: Optional[str] = None) -> Dict:
        url = final_url or self.base_url
        candidates: List[CompanyNameCandidate] = []
        
        print("=" * 80)
        print(f"Extracting from: {url}")
        print("=" * 80)
        
        # PHASE 0: Structured Data (JSON-LD, Meta)
        print("\nPHASE 0: Structured Data")
        struct_candidate = self._extract_structured_data(html_content)
        if struct_candidate:
            print(f"  ✓ Found: {struct_candidate.value}")
            candidates.append(struct_candidate)
            if struct_candidate.method == 'json_ld' and struct_candidate.confidence >= 0.96 and struct_candidate.has_legal_entity:
                if '|' not in struct_candidate.value and '｜' not in struct_candidate.value:
                    print(f"  ↓ High confidence JSON-LD with legal entity - using immediately")
                    return self._format_result(struct_candidate)
        
        # PHASE 1: Current Page Extraction (DL, UL, TABLE)
        print("\nPHASE 1: Current Page Structured Content")
        current_page_candidates = self._extract_page(html_content, url, 'current_page')
        candidates.extend(current_page_candidates)
        
        high_quality = [c for c in current_page_candidates 
                        if c.method in ['dl_field', 'table_field', 'ul_field'] and c.confidence >= 0.95]
        if high_quality:
            best = max(high_quality, key=lambda x: x.confidence)
            print(f"  ✓ Found high-quality match on current page: {best.value}")
            print(f"  ↓ Using immediately (confidence: {best.confidence:.2f})")
            return self._format_result(best)
        
        # PHASE 2: Fetch Other Company Info Pages
        if self.fetcher:
            print("\nPHASE 2: Other Company Info Pages")
            info_candidates = self._fetch_info_pages(html_content, url)
            candidates.extend(info_candidates)
        
        # PHASE 3: Black Square Marker Strategy (NEW)
        print("\nPHASE 3: Black Square Marker Strategy")
        marker_candidates = self._extract_black_square_markers(html_content)
        candidates.extend(marker_candidates)
        
        if marker_candidates:
            best_marker = max(marker_candidates, key=lambda x: x.confidence)
            if best_marker.confidence >= 0.97:
                print(f"  ✓ Found high-confidence marker match: {best_marker.value}")
                print(f"  ↓ Using immediately (confidence: {best_marker.confidence:.2f})")
                return self._format_result(best_marker)
        
        # PHASE 4: Homepage Fallbacks (h1, title, copyright)
        print("\nPHASE 4: Homepage Fallbacks")
        home_candidates = self._extract_homepage(html_content)
        candidates.extend(home_candidates)
        
        return self._select_best_candidate(candidates, html_content)
    
    def _clean(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ''
        text = unicodedata.normalize('NFKC', text)
        text = re.sub(r'[\n\r]+', ' ', text)
        return re.sub(r'\s+', ' ', text).strip()
    
    def _remove_seo(self, text: str) -> str:
        """Remove SEO suffixes from text"""
        for suffix in self.SEO_SUFFIXES:
            if text.endswith(suffix):
                text = text[:-len(suffix)].strip()
        return text
    
    def _is_form_field(self, text: str) -> bool:
        """Check if text is a form field marker"""
        return any(marker in text for marker in ['※必須', '必須', '※', '任意', 'required'])

    def _is_valid(self, name: str) -> bool:
        """Check if name is valid - STRICTER validation"""
        if self._is_form_field(name) or not name:
            return False
        
        if len(name) < 2 or len(name) > 30:
            return False
        
        if '。' in name or any(name.endswith(e) for e in ['ます', 'です', 'ください', 'ませ']):
            return False
        
        if sum(name.count(p) for p in ['にて', 'から', 'まで', 'なら', 'への']) >= 2:
            return False
        
        jp_chars = len(re.findall(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]', name))
        en_chars = len(re.findall(r'[a-zA-Z]', name))
        return jp_chars > 0 or en_chars > 3
    
    def _is_garbage(self, name: str) -> bool:
        """Check if name contains garbage patterns"""
        return any(suffix in name for suffix in self.GARBAGE)
    
    def _should_auto_complete(self, name: str) -> bool:
        """Check if a name should be auto-completed with legal entity"""
        brand_indicators = ['ドットコム', 'ドット', '.com', 'さん', 'くん', 'ちゃん',
                           'オンライン', 'ネット', 'web', 'Web']
        
        if any(indicator in name for indicator in brand_indicators):
            return False
        
        location_suffixes = [' 京都', ' 東京', ' 大阪', ' 福岡', ' 札幌']
        if any(name.endswith(loc) for loc in location_suffixes):
            return False
        
        return True

    def _extract_company_from_mixed_text(self, text: str) -> Optional[str]:
        """Extract company name from mixed text containing company + address + rep"""
        for entity in self.LEGAL_ENTITIES:
            if text.startswith(entity):
                separators = [
                    '代表', '所在地', '住所', '電話', 'TEL', '〒',
                    '東京都', '大阪府', '京都府', '北海道',
                    '千葉県', '神奈川県', '埼玉県', '茨城県', '栃木県', '群馬県',
                    '宮城県', '福島県', '山形県', '岩手県', '秋田県', '青森県',
                    '愛知県', '三重県', '岐阜県', '静岡県', '山梨県', '長野県',
                    '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県',
                    '広島県', '岡山県', '鳥取県', '島根県', '山口県',
                    '兵庫県', '奈良県', '和歌山県', '滋賀県',
                    '新潟県', '富山県', '石川県', '福井県',
                    '香川県', '徳島県', '愛媛県', '高知県',
                    '市', '区', '町', '村',
                ]
                for separator in separators:
                    if separator in text:
                        company_part = text.split(separator)[0].strip()
                        if self._is_valid(company_part):
                            return company_part
                
                if len(text) <= 50:
                    return text.strip()
        
        return None
    
    def _label_matches_company_name(self, label: str) -> Tuple[bool, float]:
        """Check if label indicates a company name field"""
        label_lower = label.lower().strip()
        label_normalized = re.sub(r'\s+', '', label)
        
        for excluded in self.EXCLUDED_LABELS:
            if excluded in label or excluded in label_lower:
                return False, 0.0
        
        for primary in self.PRIMARY_COMPANY_LABELS:
            if primary == label or primary == label_normalized:
                return True, 1.0
        
        for primary in self.PRIMARY_COMPANY_LABELS:
            if primary in label:
                return True, 0.95
        
        for secondary in self.SECONDARY_COMPANY_LABELS:
            if secondary in label_lower or secondary in label:
                if '概要' in label or 'overview' in label_lower:
                    return False, 0.0
                return True, 0.85
        
        return False, 0.0
    
    def _extract_structured_data(self, html_content: str) -> Optional[CompanyNameCandidate]:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string) if script.string else {}
                    if isinstance(data, list):
                        data = data[0] if data else {}
                    
                    if isinstance(data, dict) and 'organization' in data.get('@type', '').lower():
                        name = data.get('name', '').strip()
                        if name and self._is_valid(name):
                            return CompanyNameCandidate(name, 'json_ld', 0.96, 'json_ld', 
                                                    any(e in name for e in self.LEGAL_ENTITIES))
                except (json.JSONDecodeError, TypeError):
                    pass
            
            for attr, conf in [('og:site_name', 0.90), ('og:title', 0.88)]:
                tag = soup.find('meta', property=attr) or soup.find('meta', attrs={'name': attr})
                if tag:
                    for part in re.split(r'[|｜/\-]', tag.get('content', '')):
                        part = part.strip()
                        
                        if any(seo in part for seo in ['ご相談', 'お問い合わせ', 'ください', '選び']):
                            continue
                        
                        if '認可の' in part:
                            part = part.split('認可の')[-1].strip()
                        
                        if self._is_valid(part):
                            has_entity = any(e in part for e in self.LEGAL_ENTITIES) or '組合' in part
                            if has_entity:
                                return CompanyNameCandidate(part, 'meta_tag', conf, 'meta_tag', True)
        except Exception as e:
            logger.debug(f"Structured data error: {e}")
        
        return None

    def _extract_black_square_markers(self, html_content: str) -> List[CompanyNameCandidate]:
        """NEW: Extract company names from ■ (BLACK SQUARE) marker format
        
        Handles formats like:
        ■名　　 称 日本総合調査会 ジェイティーリサーチ
        ■<font>■</font>名　　 称日本総合調査会 (with nested tags)
        ■商　号 株式会社アビリティオフィス
        ■会社名 Some Company Name
        """
        results = []
        seen = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all text nodes and elements containing ■ marker
            # Strategy: Look for ■ followed by company labels within reasonable distance
            
            # Method 1: Use regex on raw HTML to preserve structure
            # Pattern: ■ followed by optional tags, then label, then value
            pattern = r'■[^■]*?(?:名　+称|商　*号|会社名|法人名|企業名)[^■]*?(?:<br|<BR|\n)'
            
            matches = re.finditer(pattern, html_content, re.DOTALL | re.IGNORECASE)
            found_count = 0
            
            for match in matches:
                found_count += 1
                chunk = match.group(0)
                
                # Remove HTML tags to get clean text
                clean_chunk = re.sub(r'<[^>]+>', '', chunk)
                clean_chunk = self._clean(clean_chunk)
                
                # Split label and value using known company labels
                label = None
                value = None
                
                for known_label in self.PRIMARY_COMPANY_LABELS:
                    if known_label in clean_chunk:
                        # Find position of label
                        label_pos = clean_chunk.find(known_label)
                        label = known_label
                        
                        # Value starts after label
                        value_start = label_pos + len(known_label)
                        value = clean_chunk[value_start:].strip()
                        
                        # Stop at common delimiters
                        for delimiter in ['■', '東京', '〒', 'TEL', '代表', '所在地']:
                            if delimiter in value:
                                value = value.split(delimiter)[0].strip()
                        
                        break
                
                if not label or not value:
                    continue
                
                # Clean value
                cleaned = self._clean(self._remove_seo(value))
                
                # Remove garbage patterns
                cleaned = re.sub(r'[（(][^）)]*[）)]', '', cleaned).strip()
                
                # Handle mixed text
                if any(e in cleaned for e in self.LEGAL_ENTITIES):
                    extracted = self._extract_company_from_mixed_text(cleaned)
                    if extracted:
                        cleaned = extracted
                
                if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                    seen.add(cleaned)
                    has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                    confidence = 0.97 if has_legal else 0.96
                    
                    results.append(CompanyNameCandidate(
                        cleaned, 'black_square_marker', confidence, 'black_square', has_legal
                    ))
                    print(f"      ✓ [BLACK SQUARE] '{label}' → {cleaned} (confidence: {confidence:.2f})")
            
            print(f"      Found {found_count} black square marker(s)")
        
        except Exception as e:
            logger.debug(f"Black square marker error: {e}")
        
        return results

    def _extract_homepage(self, html_content: str) -> List[CompanyNameCandidate]:
        results = []
        soup = BeautifulSoup(html_content, 'html.parser')
        for tag in soup(['script', 'style', 'noscript']):
            tag.decompose()
        
        print("  Checking h1 tags...")
        h1_tags = soup.find_all('h1')
        print(f"  Found {len(h1_tags)} h1 tag(s)")
        
        for idx, h1 in enumerate(h1_tags):
            text = self._clean(h1.get_text(strip=True))
            print(f"    h1[{idx}]: '{text[:60]}'...")
            
            business_keywords = ['探偵事務所', '調査事務所', '探偵社', '調査会社', 
                                '法律事務所', '会計事務所', 'コンサルティング']
            
            if any(kw in text for kw in business_keywords):
                print(f"    ✓ [BUSINESS MATCH]")
                if self._is_valid(text):
                    results.append(CompanyNameCandidate(text, 'homepage_h1', 0.92, 'business_name', False))
                    return results
        
        print("  ✗ No homepage matches found")
        return results
    
    def _fetch_info_pages(self, html_content: str, base_url: str) -> List[CompanyNameCandidate]:
        results = []
        if not self.fetcher:
            return results
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            info_urls = set()
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                if any(x in href for x in ['info', 'outline', 'profile', 'gaiyou', 'company', 'about']):
                    info_urls.add(urljoin(base_url, link['href']))
            
            parsed = urlparse(base_url)
            domain_root = f"{parsed.scheme}://{parsed.netloc}"
            common_paths = ['/company', '/about', '/company/info.html', '/gaiyou.html']
            
            for path in common_paths:
                info_urls.add(domain_root + path)
            
            print(f"  Attempting {min(len(info_urls), 15)} company info URLs...")
            
            for url in sorted(info_urls)[:15]:
                try:
                    print(f"    Trying: {url}")
                    content, status, _, _ = self.fetcher.fetch_page(url)
                    
                    if status == 200 and content:
                        page_results = self._extract_page(content, url, 'company_info')
                        results.extend(page_results)
                        
                        if any(r.method in ['dl_field', 'table_field'] and r.confidence >= 0.98 for r in page_results):
                            print(f"    [✓ Found high-quality match]")
                            break
                except Exception as e:
                    logger.debug(f"Fetch error {url}: {e}")
        except Exception as e:
            logger.error(f"Info page error: {e}")
        
        return results
    
    def _extract_page(self, html_content: str, page_url: str, source_type: str) -> List[CompanyNameCandidate]:
        results = []
        seen = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            # DL extraction
            dls = soup.find_all('dl')
            if dls:
                print(f"      Found {len(dls)} definition list(s)")
                
                for dl in dls:
                    dts = dl.find_all('dt')
                    dds = dl.find_all('dd')
                    
                    for dt_idx, dt in enumerate(dts):
                        label = dt.get_text(strip=True)
                        
                        if dt_idx < len(dds):
                            dd = dds[dt_idx]
                            value = dd.get_text(strip=True)
                            
                            matches, conf_boost = self._label_matches_company_name(label)
                            
                            if matches and value:
                                cleaned = self._clean(self._remove_seo(value))
                                
                                if any(e in cleaned for e in self.LEGAL_ENTITIES):
                                    extracted = self._extract_company_from_mixed_text(cleaned)
                                    if extracted:
                                        cleaned = extracted
                                
                                if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                                    seen.add(cleaned)
                                    has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                                    confidence = 0.99 if has_legal else 0.95 + (conf_boost * 0.04)
                                    
                                    results.append(CompanyNameCandidate(
                                        cleaned, f'{source_type}_dl', confidence, 'dl_field', has_legal
                                    ))
                                    print(f"          ✓ [DL MATCH] {cleaned} (confidence: {confidence:.2f})")
            
            if results:
                return results
            
            # Table extraction
            tables = soup.find_all('table')
            if tables:
                print(f"      Found {len(tables)} table(s)")
            
            for table_idx, table in enumerate(tables):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        # Skip if value is just the label name itself (e.g., "会社名" → "会社名")
                        if value == label or value in self.PRIMARY_COMPANY_LABELS or value in self.SECONDARY_COMPANY_LABELS:
                            continue
                        
                        matches, conf_boost = self._label_matches_company_name(label)
                        
                        if matches and value:
                            cleaned = self._clean(self._remove_seo(value))
                            
                            # Skip if cleaned value is empty or is just whitespace
                            if not cleaned or cleaned in self.PRIMARY_COMPANY_LABELS:
                                continue
                            
                            # Skip affiliates/subsidiaries (containing 関連会社, 子会社, 米国, etc)
                            if any(affiliate_marker in cleaned for affiliate_marker in ['関連会社', '子会社', '米国', 'USA', '(米国)', 'Inc(', '海外']):
                                print(f"          ⊗ [SKIP AFFILIATE] {cleaned}")
                                continue
                            
                            if any(e in cleaned for e in self.LEGAL_ENTITIES):
                                extracted = self._extract_company_from_mixed_text(cleaned)
                                if extracted:
                                    cleaned = extracted
                            
                            if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                                seen.add(cleaned)
                                has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                                confidence = 0.99 if has_legal else 0.95 + (conf_boost * 0.04)
                                
                                results.append(CompanyNameCandidate(
                                    cleaned, f'{source_type}_table', confidence, 'table_field', has_legal
                                ))
                                print(f"          ✓ [TABLE MATCH] {cleaned}")
                                # Return immediately on first valid match from first table with company info
                                if table_idx == 0 or label in ['会社名', '商号', '法人名']:
                                    return results
            
            if results:
                return results
            
            print(f"      No DL/table results - trying text pattern fallback...")
            text = soup.get_text()
            
            for label_kw in self.PRIMARY_COMPANY_LABELS:
                pattern = re.escape(label_kw) + r'\s*[:：]\s*([\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff0-9ー\s]{2,50})'
                for match in re.finditer(pattern, text, re.UNICODE):
                    candidate = match.group(1).strip()
                    cleaned = self._clean(self._remove_seo(candidate))
                    
                    if cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(cleaned, f'{source_type}_text', 0.85, 'text_pattern_label', False))
                        print(f"      [TEXT LABEL] {cleaned}")
                        return results
        
        except Exception as e:
            logger.error(f"Page extraction error: {e}")
        
        return results
    
    def _select_best_candidate(self, candidates: List[CompanyNameCandidate], html_content: str) -> Dict:
        if not candidates:
            print("\n[ERROR] No candidates found")
            return {'company_name': None, 'company_name_source': None, 'company_name_confidence': 0.0,
                    'company_name_method': None, 'is_auto_completed': False, 'company_name_candidates': []}
        
        print("\n" + "="*80)
        print("SELECTING BEST CANDIDATE")
        print("="*80)
        
        seen = {}
        for c in candidates:
            if c.value not in seen or c.confidence > seen[c.value].confidence:
                seen[c.value] = c
        
        best = sorted(list(seen.values()), 
                 key=lambda x: (
                     -2 if x.method in ['dl_field', 'table_field', 'black_square'] else 0,
                     -1 if x.method == 'business_name' else 0,
                     -x.has_legal_entity,
                     -x.confidence,
                     len(x.value)
                 ))[0]
        
        print(f"\n[CANDIDATE] {best.value}")
        print(f"  Confidence: {best.confidence:.2f} | Source: {best.source} | Method: {best.method}")
        
        if not best.has_legal_entity and self._should_auto_complete(best.value):
            completed, found = self._auto_complete_legal_entity(best.value, html_content)
            if completed:
                best.value = completed
                best.has_legal_entity = True
                best.is_auto_completed = True
                if not found:
                    best.confidence = min(best.confidence, 0.82)
                print(f"  ↓ Completed: {completed} (found_in_html: {found})")
        
        print(f"\n[FINAL] {best.value} (Confidence: {best.confidence:.2f})")
        
        return {
            'company_name': best.value,
            'company_name_source': best.source,
            'company_name_confidence': best.confidence,
            'company_name_method': best.method,
            'is_auto_completed': best.is_auto_completed,
            'company_name_candidates': [c.to_dict() for c in candidates]
        }
    
    def _auto_complete_legal_entity(self, company_name: str, html_content: str) -> Tuple[Optional[str], bool]:
        if any(entity in company_name for entity in self.LEGAL_ENTITIES):
            return company_name, True
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text()
            escaped_name = re.escape(company_name)
            
            for entity in self.LEGAL_ENTITIES:
                if re.search(re.escape(entity) + r'\s*' + escaped_name, text, re.IGNORECASE):
                    return entity + company_name, True
                if re.search(escaped_name + r'\s*' + re.escape(entity), text, re.IGNORECASE):
                    return company_name + entity, True
            
            entity_counts = {e: len(re.findall(re.escape(e), text)) for e in self.LEGAL_ENTITIES}
            entity_counts = {e: c for e, c in entity_counts.items() if c > 0}
            
            if entity_counts:
                most_common = max(entity_counts, key=entity_counts.get)
                return most_common + company_name, False
            
            return '株式会社' + company_name, False
        except Exception as e:
            logger.error(f"Auto-complete error: {e}")
            return '株式会社' + company_name, False
    
    def _format_result(self, candidate: CompanyNameCandidate) -> Dict:
        return {
            'company_name': candidate.value,
            'company_name_source': candidate.source,
            'company_name_confidence': candidate.confidence,
            'company_name_method': candidate.method,
            'is_auto_completed': candidate.is_auto_completed,
            'company_name_candidates': [candidate.to_dict()]
        }