"""
Company Name Extractor v15 - COMPLETE FIX
- Fixed method ordering issue
- Added definition list (<dl>) extraction
- Added business name fallbacks (探偵事務所, etc.)
- Handles mixed text (company + address + rep)
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
        '行政書士', '弁護士', '司法書士', '税理士', '公認会計士'
    ]
    
    PRIMARY_COMPANY_LABELS = [
        '会社名', '商号', '法人名', '企業名', '正式名称', '名称', '社名',
        '事業者名', '法人の名称', '屋号', '法人名称', '運営会社', '運営法人',
        '事務所名',  '事務所',  '店舗名',   '施設名', "商　号", "会 社 名", "称号", "社　名"
    ]
    
    SECONDARY_COMPANY_LABELS = [
        '名前', '会社', '名', 'Company', 'Name', 'company name'
    ]
    
    EXCLUDED_LABELS = [
        '項目', '単位', '価格', '料金', '費用', '時間', '金額',
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
                # Also check it's not an SEO-style title (no pipes, no multiple parts)
                if '|' not in struct_candidate.value and '／' not in struct_candidate.value:
                    print(f"  → High confidence JSON-LD with legal entity - using immediately")
                    return self._format_result(struct_candidate)
                else:
                    print(f"  → Contains separators, letting it compete with other candidates")
        
        # PHASE 1: Current Page Extraction (DL, UL, TABLE)
        print("\nPHASE 1: Current Page Structured Content")
        current_page_candidates = self._extract_page(html_content, url, 'current_page')
        candidates.extend(current_page_candidates)
        
        # If current page has high-quality structured data, use it
        high_quality = [c for c in current_page_candidates 
                        if c.method in ['dl_field', 'table_field', 'ul_field'] and c.confidence >= 0.95]
        if high_quality:
            best = max(high_quality, key=lambda x: x.confidence)
            print(f"  ✓ Found high-quality match on current page: {best.value}")
            print(f"  → Using immediately (confidence: {best.confidence:.2f})")
            return self._format_result(best)
        
        # PHASE 2: Fetch Other Company Info Pages
        if self.fetcher:
            print("\nPHASE 2: Other Company Info Pages")
            info_candidates = self._fetch_info_pages(html_content, url)
            candidates.extend(info_candidates)
        
        # PHASE 3: Homepage Fallbacks (h1, title, copyright)
        print("\nPHASE 3: Homepage Fallbacks")
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
        
        # Length: 2-30 characters (reduced from 50)
        if len(name) < 2 or len(name) > 30:
            return False
        
        # Reject sentences (has period OR ends with polite forms)
        if '。' in name or any(name.endswith(e) for e in ['ます', 'です', 'ください', 'ませ']):
            return False
        
        # Reject if too many particles (4+ = sentence structure)
        if sum(name.count(p) for p in ['にて', 'から', 'まで', 'なら', 'への']) >= 2:
            return False
        
        # Must have Japanese or English characters
        jp_chars = len(re.findall(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]', name))
        en_chars = len(re.findall(r'[a-zA-Z]', name))
        return jp_chars > 0 or en_chars > 3
    
    def _is_garbage(self, name: str) -> bool:
        """Check if name contains garbage patterns"""
        return any(suffix in name for suffix in self.GARBAGE)
    
    def _is_association_not_company(self, text: str) -> bool:
        """Check if this is an association/organization name, not a company"""
        association_markers = [
            '加盟団体', '所属団体', '理事', '会員', '協会連合会',
            'NPO法人', '全国〜協会', '業界団体'
        ]
        return any(marker in text for marker in association_markers)

    # def _is_sentence_not_name(self, text: str) -> bool:
    #     """Check if text is a sentence/paragraph"""
    #     if text.count('。') > 0:
    #         return True
    #     if any(text.endswith(end) for end in ['ます', 'です', 'ください', 'せん']):
    #         return True
    #     sentence_markers = ['から', 'まで', 'など', 'により', 'について', 'における']
    #     if sum(1 for m in sentence_markers if m in text) >= 3:
    #         return True
    #     return False

    def _should_auto_complete(self, name: str) -> bool:
        """Check if a name should be auto-completed with legal entity"""
        
        # Don't auto-complete if it's clearly a brand/service name
        brand_indicators = [
            'ドットコム', 'ドット', '.com', 'さん', 'くん', 'ちゃん',
            'オンライン', 'ネット', 'web', 'Web'
        ]
        
        if any(indicator in name for indicator in brand_indicators):
            return False
        
        # Don't auto-complete if it ends with a location
        location_suffixes = [
            ' 京都', ' 東京', ' 大阪', ' 福岡', ' 札幌'
        ]
        
        if any(name.endswith(loc) for loc in location_suffixes):
            return False
        
        return True

    def _extract_company_from_sentence(self, sentence: str) -> Optional[str]:
        """Extract company name from marketing sentences like '〜なら【NAME】へ'"""
        # Pattern: "なら COMPANY_NAME へ/に"
        patterns = [
            r'なら\s*([^へに]+(?:探偵事務所|探偵社|調査事務所|興信所))\s*[へに]',
            r'([^、。]+(?:探偵事務所|探偵社|調査事務所|興信所))\s*へお任せ',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sentence)
            if match:
                name = match.group(1).strip()
                if self._is_valid(name):
                    return name
        return None

    def _extract_company_from_mixed_text(self, text: str) -> Optional[str]:
        """Extract company name from mixed text containing company + address + rep"""
        for entity in self.LEGAL_ENTITIES:
            if text.startswith(entity):
                separators = [
                '代表', '所在地', '住所', '電話', 'TEL', '〒',
                # Prefecture names
                '東京都', '大阪府', '京都府', '北海道',
                '千葉県', '神奈川県', '埼玉県', '茨城県', '栃木県', '群馬県',
                '宮城県', '福島県', '山形県', '岩手県', '秋田県', '青森県',
                '愛知県', '静岡県', '岐阜県', '三重県', '長野県', '山梨県',
                '福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県',
                '広島県', '岡山県', '鳥取県', '島根県', '山口県',
                '兵庫県', '奈良県', '和歌山県', '滋賀県',
                '新潟県', '富山県', '石川県', '福井県',
                '香川県', '徳島県', '愛媛県', '高知県',
                # Common city patterns
                '市', '区', '町', '村',
            ]
                for separator in separators:
                    if separator in text:
                        company_part = text.split(separator)[0].strip()
                        if self._is_valid(company_part):
                            return company_part
                
                if len(text) <= 50:
                    return text.strip()
                else:
                    for i in range(min(50, len(text)), 0, -1):
                        candidate = text[:i].strip()
                        if self._is_valid(candidate) and not self._is_garbage(candidate):
                            return candidate
        
        return None
    
    def _label_matches_company_name(self, label: str) -> Tuple[bool, float]:
        """Check if label indicates a company name field"""
        label_lower = label.lower().strip()
        label_normalized = re.sub(r'\s+', '', label)
        
        # Check exclusions first
        for excluded in self.EXCLUDED_LABELS:
            if excluded in label or excluded in label_lower:
                return False, 0.0
        
        # Exact primary match
        for primary in self.PRIMARY_COMPANY_LABELS:
            if primary == label or primary == label_normalized:
                return True, 1.0
        
        # Partial primary match
        for primary in self.PRIMARY_COMPANY_LABELS:
            if primary in label:
                return True, 0.95
        
        # Secondary match
        for secondary in self.SECONDARY_COMPANY_LABELS:
            if secondary in label_lower or secondary in label:
                if '概要' in label or 'overview' in label_lower:
                    return False, 0.0
                return True, 0.85
        
        return False, 0.0
    
    def _extract_structured_data(self, html_content: str) -> Optional[CompanyNameCandidate]:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # JSON-LD
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
            
            # Meta tags
            for attr, conf in [('og:site_name', 0.90), ('og:title', 0.88)]:
                tag = soup.find('meta', property=attr) or soup.find('meta', attrs={'name': attr})
                if tag:
                    for part in re.split(r'[|｜/\-]', tag.get('content', '')):
                        part = part.strip()
                        
                        # Skip SEO phrases
                        if any(seo in part for seo in ['ご相談', 'お問い合わせ', 'ください', '選び']):
                            continue
                        
                        # Extract from prefix: "認可の〜" → "〜"
                        if '認可の' in part:
                            part = part.split('認可の')[-1].strip()
                        
                        if self._is_valid(part):
                            has_entity = any(e in part for e in self.LEGAL_ENTITIES) or '組合' in part
                            if has_entity:
                                return CompanyNameCandidate(part, 'meta_tag', conf, 'meta_tag', True)
        except Exception as e:
            logger.debug(f"Structured data error: {e}")
        
        return None

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
            
            # Check for business keywords
            business_keywords = ['探偵事務所', '調査事務所', '探偵社', '調査会社', 
                                '法律事務所', '会計事務所', 'コンサルティング']
            
            if any(kw in text for kw in business_keywords):
                print(f"    ✓ [BUSINESS MATCH]")
                
                # If it's a sentence (>40 chars or has particles), extract just the name
                if len(text) > 40 or any(p in text for p in ['にて', 'から', 'なら', 'への']):
                    extracted = self._extract_company_from_sentence(text)
                    if extracted:
                        text = extracted
                        print(f"      → Extracted from sentence: {text}")
                    else:
                        print(f"      ✗ Couldn't extract from sentence, skipping")
                        continue
                
                # Handle pipe separators
                if '|' in text or '｜' in text:
                    parts = [p.strip() for p in re.split(r'[|｜]', text)]
                    # Prefer parts without location prefixes
                    location_prefixes = ['京都の', '東京の', '大阪の', '福岡の']
                    non_location = [p for p in parts if not any(p.startswith(loc) for loc in location_prefixes)]
                    text = non_location[-1] if non_location else parts[-1]
                
                # Remove trailing locations
                for loc in [' 京都', ' 東京', ' 大阪', '　京都', '　東京']:
                    if text.endswith(loc):
                        text = text[:-len(loc)].strip()
                
                if self._is_valid(text):
                    results.append(CompanyNameCandidate(text, 'homepage_h1', 0.92, 'business_name', False))
                    return results
        
        # Title with legal entities
        print("  Checking title tag...")
        title = soup.find('title')
        if title and title.string:
            for part in re.split(r'[|｜/\-]', title.string.strip()):
                part = part.strip()
                # Skip SEO phrases
                if any(seo in part for seo in ['ご相談', 'お問い合わせ', 'ください', 'はこちら', '選び']):
                    continue
                
                # Extract from descriptive prefixes: "内閣総理大臣認可の全国調査業協同組合"
                if '認可の' in part:
                    part = part.split('認可の')[-1].strip()
                
                if any(e in part for e in self.LEGAL_ENTITIES) or '組合' in part:
                    cleaned = self._clean(part)
                    if self._is_valid(cleaned):
                        print(f"    ✓ [TITLE MATCH] {cleaned}")
                        return [CompanyNameCandidate(cleaned, 'homepage_title', 0.85, 'title', True)]
        
        # H1 fallback (no business keywords)
        print("  Checking h1 fallback...")
        if h1_tags:
            for h1 in h1_tags:
                text = self._clean(h1.get_text(strip=True))
                if self._is_valid(text) and not any(nav in text.lower() for nav in ['menu', 'navigation']):
                    print(f"    ✓ [H1 FALLBACK] {text}")
                    return [CompanyNameCandidate(text, 'homepage_h1', 0.72, 'h1_fallback', False)]
        
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
                href, text = link.get('href', '').lower(), link.get_text().lower()
                if any(x in href for x in ['info', 'outline', 'profile', 'gaiyou', 'company', 'about']):
                    info_urls.add(urljoin(base_url, link['href']))
                if any(x in text for x in ['会社情報', '会社概要', '企業情報', '概要', 'about']):
                    info_urls.add(urljoin(base_url, link['href']))
            
            parsed = urlparse(base_url)
            domain_root = f"{parsed.scheme}://{parsed.netloc}"
            common_paths = ['/company', '/company/', '/about', '/about/', '/company/info.html',
                           '/company/outline.html', '/gaiyou', '/gaiyou.html', '/kaisya.html']
            
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
                    else:
                        print(f"      → HTTP {status}")
                except Exception as e:
                    logger.debug(f"Fetch error {url}: {e}")
        except Exception as e:
            logger.error(f"Info page error: {e}")
        
        return results
    
    def _extract_text_with_breaks(self, element) -> List[str]:
        """Extract text from element, treating <br> as line breaks"""
        # Replace <br> tags with newlines
        for br in element.find_all('br'):
            br.replace_with('\n')
        
        # Get text and split by lines
        text = element.get_text()
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return lines

    def _extract_company_from_complex_format(self, text: str) -> Optional[str]:
        """Extract company name from complex formats with parentheses, colons, etc."""
        
        original_text = text
        
        # Pattern 0: Remove abbreviation suffix (略称：XXX or 略：XXX)
        # Example: "株式会社　ナショナル・エージェント・カンパニー　略称：ＮＡＣ（ナック）"
        if '略称' in text or '略：' in text:
            # Split at abbreviation marker and take the first part
            for marker in ['略称：', '略称:', '略：', '略:']:
                if marker in text:
                    text = text.split(marker)[0].strip()
                    # Also remove any trailing full-width spaces
                    text = re.sub(r'　+', '', text).strip()
                    break
        
        # Pattern 1: (会社名：株式会社ABC) or （会社名：株式会社ABC）
        paren_match = re.search(r'[（(](?:会社名|法人名|社名)[：:]\s*([^）)]+)[）)]', text)
        if paren_match:
            return paren_match.group(1).strip()
        
        # Pattern 2: 会社名：株式会社ABC
        label_keywords = ['会社名', '法人名', '法人の名称', '社名', '事業所名', '事業所名称', '屋号']
    
        for label in label_keywords:
            # Match label followed by colon, then capture until next label/newline/parenthesis
            pattern = re.escape(label) + r'[：:]\s*([^\n（(]+?)(?=(?:会社名|法人名|法人の名称|社名|事業所名|事業所名称|屋号|代表|所在地|住所)[：:]|\n|$)'
            match = re.search(pattern, text)
            if match:
                candidate = match.group(1).strip()
                # Clean up any trailing punctuation or spaces
                candidate = re.sub(r'[・\s]+', '', candidate)
                if self._is_valid(candidate):
                    return candidate
        
        # Pattern 3: Parentheses with legal entity
        for entity in self.LEGAL_ENTITIES:
            paren_entity = re.search(r'[（(]([^）)]*' + re.escape(entity) + r'[^）)]*)[）)]', text)
            if paren_entity:
                candidate = paren_entity.group(1).strip()
                if self._is_valid(candidate):
                    return candidate
        
        # If we cleaned the text (removed abbreviation), return it if valid
        if text != original_text and self._is_valid(text):
            return text
        
        return None
    def _is_valid_company_name(self, name: str) -> bool:
        """Check if text looks like an actual company name, not just any entity match"""
        
        # If it's ONLY professional titles without a name, reject it
        standalone_titles = ['弁護士', '税理士', '公認会計士', '司法書士', '行政書士']
        
        # Reject if it's exactly a single title
        if name in standalone_titles:
            return False
        
        # Reject connector patterns like "税理士・警察" or "弁護士、税理士"
        if any(sep in name for sep in ['・', '、', 'と']):
            parts = re.split('[・、、]', name)
            # If all parts are titles/generic terms, reject
            generic_terms = standalone_titles + ['警察', '企業', '個人', 'マスコミ', '大手']
            if all(part.strip() in generic_terms for part in parts):
                return False
        
        return True


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
                
                for idx, dl in enumerate(dls):
                    print(f"        Analyzing <dl> {idx}...")
                    dts = dl.find_all('dt')
                    dds = dl.find_all('dd')
                    
                    for dt_idx, dt in enumerate(dts):
                        label = dt.get_text(strip=True)
                        
                        img = dt.find('img')
                        if img:
                            alt_text = img.get('alt', '').strip()
                            if alt_text:
                                label = alt_text
                            elif img.get('title'):
                                label = img.get('title').strip()
                            elif img.get('src') and not label:
                                src = img.get('src', '')
                                filename = src.split('/')[-1].split('.')[0]
                                if any(kw in filename.lower() for kw in ['company', 'name', 'kaisya']):
                                    label = '会社名'
                        
                        if dt_idx < len(dds):
                            dd = dds[dt_idx]
                            value = dd.get_text(strip=True)
                            
                            if label:
                                print(f"          <dt>: '{label}' → <dd>: '{value[:50]}'")
                            
                            matches, conf_boost = self._label_matches_company_name(label)
                            
                            if matches and value:
                                cleaned = self._clean(self._remove_seo(value))
                                
                                if '代表' in cleaned or '所在地' in cleaned:
                                    extracted = self._extract_company_from_mixed_text(cleaned)
                                    if extracted:
                                        cleaned = extracted
                                
                                if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                                    seen.add(cleaned)
                                    has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                                    
                                    if label in ['商号', '運営会社', '運営法人'] and has_legal:
                                        confidence = 0.99
                                    else:
                                        confidence = 0.95 + (conf_boost * 0.04)
                                    
                                    results.append(CompanyNameCandidate(
                                        cleaned, f'{source_type}_dl', confidence, 'dl_field', has_legal
                                    ))
                                    print(f"          ✓ [DL MATCH] {cleaned} (confidence: {confidence:.2f})")
            
            if results:
                return results
            
            # UL/LI extraction (after DL, before TABLE)
            uls = soup.find_all('ul')
            if uls:
                print(f"      Found {len(uls)} unordered list(s)")
                
                for idx, ul in enumerate(uls):
                    lis = ul.find_all('li', recursive=False)
                    
                    for li in lis:
                        # Method 1: Look for class-based label/value pattern
                        label_elem = li.find(class_=lambda x: x and any(kw in x.lower() for kw in ['name', 'tit', 'label', 'head', 'dt']))
                        value_elem = li.find(class_=lambda x: x and any(kw in x.lower() for kw in ['data', 'txt', 'value', 'cont', 'dd']))
                        
                        # Method 2: Look for <strong> tag as label (NEW)
                        if not (label_elem and value_elem):
                            strong_tag = li.find('strong')
                            if strong_tag:
                                label_elem = strong_tag
                                # Value is the remaining text after removing the strong tag
                                value_elem = None  # Will be handled differently
                        
                        if label_elem:
                            label = label_elem.get_text(strip=True)
                            
                            # Get value
                            if value_elem:
                                value_lines = self._extract_text_with_breaks(value_elem)
                            else:
                                # Extract text after <strong> tag
                                full_text = li.get_text()
                                label_text = label_elem.get_text()
                                # Remove label from full text
                                remaining = full_text.replace(label_text, '', 1).strip()
                                # Split by line breaks
                                value_lines = [line.strip() for line in remaining.split('\n') if line.strip()]
                            
                            if label and value_lines:
                                print(f"          <li> '{label}' → '{value_lines[0][:50] if value_lines else ''}'")
                                if len(value_lines) > 1:
                                    print(f"               (+ {len(value_lines)-1} more line(s))")
                            
                            matches, conf_boost = self._label_matches_company_name(label)
                            
                            if matches and value_lines:
                                # Try each line, prioritizing the first valid one
                                for line in value_lines:
                                    # Try complex format extraction first
                                    extracted_from_format = self._extract_company_from_complex_format(line)
                                    if extracted_from_format:
                                        cleaned = self._clean(extracted_from_format)
                                    else:
                                        cleaned = self._clean(line)
                                        
                                        if '代表' in cleaned or '所在地' in cleaned:
                                            extracted = self._extract_company_from_mixed_text(cleaned)
                                            if extracted:
                                                cleaned = extracted
                                    
                                    if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                                        seen.add(cleaned)
                                        has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                                        
                                        # First line with legal entity gets highest confidence
                                        if has_legal and value_lines.index(line) == 0:
                                            confidence = 0.99
                                        elif label in ['商号', '運営会社', '運営法人', '組織名', '法人の名称'] and has_legal:
                                            confidence = 0.99
                                        else:
                                            confidence = 0.95 + (conf_boost * 0.04)
                                        
                                        results.append(CompanyNameCandidate(
                                            cleaned, f'{source_type}_ul', confidence, 'ul_field', has_legal
                                        ))
                                        print(f"          ✓ [UL MATCH] {cleaned} (confidence: {confidence:.2f})")
                                        break
                                    
            if results:
                return results
            
            # Table extraction
            tables = soup.find_all('table')
            if tables:
                print(f"      Found {len(tables)} table(s)")
            
            for idx, table in enumerate(tables):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        if label:
                            print(f"          '{label}' → '{value[:50]}'")
                        
                        matches, conf_boost = self._label_matches_company_name(label)
                        
                        if matches and value:
                            extracted_from_format = self._extract_company_from_complex_format(value)
                            if extracted_from_format:
                                cleaned = self._clean(extracted_from_format)
                            else:
                                cleaned = self._clean(self._remove_seo(value))
                            
                            is_mixed_label = any(kw in label for kw in ['代表', '所在地', '住所', '本社'])
                            is_mixed_value = any(e in cleaned for e in self.LEGAL_ENTITIES) and \
                                            any(pref in cleaned for pref in ['東京都', '大阪府', '京都府', '北海道',
                                                                            '千葉県', '神奈川県', '埼玉県', '宮城県',
                                                                            '福岡県', '愛知県', '広島県', '兵庫県'])
                            
                            if is_mixed_label or is_mixed_value:
                                extracted = self._extract_company_from_mixed_text(cleaned)
                                if extracted:
                                    cleaned = extracted
                            
                            if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                                seen.add(cleaned)
                                has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                                
                                if label in ['商号', '運営会社', '運営法人'] and has_legal:
                                    confidence = 0.99
                                else:
                                    confidence = 0.95 + (conf_boost * 0.04)
                                
                                results.append(CompanyNameCandidate(
                                    cleaned, f'{source_type}_table', confidence, 'table_field', has_legal
                                ))
                                print(f"          ✓ [TABLE MATCH] {cleaned}")
                            
            if results:
                return results
            
            
            # Text pattern fallback
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
            
            for entity in self.LEGAL_ENTITIES:
                pattern = re.escape(entity) + r'[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff0-9ー\s]{2,30}'
                for match in re.finditer(pattern, text, re.UNICODE):
                    cleaned = self._clean(self._remove_seo(match.group(0)))
                    
                    if cleaned and cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(cleaned, f'{source_type}_text', 0.75, 'text_pattern_entity', True))
                        print(f"      [TEXT ENTITY] {cleaned}")
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
                     -2 if x.method in ['dl_field', 'table_field'] else 0,  # Highest priority
                     -1 if x.method == 'business_name' else 0,  # NEW: H1 business names second priority
                     -x.has_legal_entity,  # Then legal entities
                     -x.confidence,  # Then confidence
                     len(x.value)  # Then length
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
                print(f"  → Completed: {completed} (found_in_html: {found})")
        
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