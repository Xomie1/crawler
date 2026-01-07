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
import chardet

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
        '行政書士', '弁護士', '法務書士', '税理士', '公認会計士', '弁護士法人'
    ]
    
    PRIMARY_COMPANY_LABELS = [
        '会社名', '商号', '法人名', '企業名', '正式名称', '名称', '社名',
        '事業者名', '法人の名称', '屋号', '法人名称', '運営会社', '運営法人',
        '事務所名', '事務所', '店舗名', '施設名', '商　号', '会 社 名', '称号', '社　名', '事業所名', '事業名', '団体名'
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

        # PHASE 5: Title Introduction Pattern (NEW - LAST RESORT)
        print("\nPHASE 5: Title Introduction Pattern (Last Resort)")
        intro_candidates = self._extract_title_introduction_pattern(html_content)
        candidates.extend(intro_candidates)
        
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
    
    def _extract_from_dt_dd_merged(self, html_content: str) -> List[CompanyNameCandidate]:
        """NEW: Extract from merged <dt> content with labels inline
        
        Handles malformed DL structures where all content is merged into single dt elements:
        <dt>事務所名 弁護士法人太田川法律事務所 代表 弁護士 田中亮次 所在地 ...</dt>
        
        Extracts by:
        1. Finding known company labels (事務所名, 会社名, 法人名, 名称)
        2. Extracting text after label
        3. Stopping at known field delimiters (代表, 所在地, 連絡先, TEL, FAX)
        """
        results = []
        seen = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Company name labels to search for
            company_labels = ['事務所名', '会社名', '法人名', '名称', 'å•†å·', 'ä¼šç¤¾å']
            
            # Field delimiters that mark end of company name
            stop_words = ['代表', '所在地', '連絡先', 'TEL', '電話', 'FAX', 'e-mail', 
                         'ä»£è¡¨', 'æ‰€åœ¨åœ°', 'é€£çµ¡å…ˆ', 'é›»è©±', 'E-mail', 'Email']
            
            dts = soup.find_all('dt')
            
            if not dts:
                print(f"      No <dt> elements found for merged DL extraction")
                return results
            
            print(f"      Found {len(dts)} <dt> element(s) - trying merged DL extraction...")
            
            for dt_idx, dt in enumerate(dts):
                # Get text preserving internal structure but collapsing to single line
                dt_text = dt.get_text(" ", strip=True)
                print(f"        [DT {dt_idx}] raw: '{dt_text[:80]}'...")
                
                # Try each company label
                for company_label in company_labels:
                    if company_label not in dt_text:
                        continue
                    
                    # Split on the label and take text after it
                    parts = dt_text.split(company_label, 1)
                    if len(parts) < 2:
                        continue
                    
                    value_text = parts[1].strip()
                    
                    # Stop at known field delimiters
                    for stop_word in stop_words:
                        if stop_word in value_text:
                            value_text = value_text.split(stop_word, 1)[0].strip()
                    
                    # Clean up excessive whitespace
                    value_text = re.sub(r'\s+', ' ', value_text).strip()
                    
                    print(f"          After '{company_label}': '{value_text[:60]}'")
                    
                    # Validate the extracted value
                    if not value_text or len(value_text) < 3:
                        print(f"            ⊘ Too short")
                        continue
                    
                    if not self._is_valid(value_text):
                        print(f"            ⊘ Validation failed")
                        continue
                    
                    if self._is_garbage(value_text):
                        print(f"            ⊘ Contains garbage")
                        continue
                    
                    # Normalize encoding just in case
                    cleaned = self._normalize_encoding(value_text)
                    cleaned = self._clean(cleaned)
                    
                    if cleaned in seen:
                        print(f"            ⊘ Duplicate")
                        continue
                    
                    seen.add(cleaned)
                    has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                    confidence = 0.97 if has_legal else 0.94
                    
                    results.append(CompanyNameCandidate(
                        cleaned, 'dt_dd_merged', confidence, 'dt_merged', has_legal
                    ))
                    print(f"            ✓ [DT MERGED] '{cleaned}' (conf: {confidence:.2f})")
                    break  # Move to next dt element after finding a match
        
        except Exception as e:
            logger.debug(f"DT/DD merged extraction error: {e}")
        
        return results
    
    def _is_form_field(self, text: str) -> bool:
        """Check if text is a form field marker"""
        return any(marker in text for marker in ['※必須', '必須', '※', '任意', 'required'])

    def _is_valid(self, name: str) -> bool:
        """Check if name is valid - STRICTER validation"""
        if self._is_form_field(name) or not name:
            return False
        
        if any(npo_marker in name for npo_marker in ['特定非営利活動法人', '一般社団法人', '一般財団法人']):
            max_length = 80  # NPOs/associations can be longer
        else:
            max_length = 30  # Regular companies stay at 30
        
        if len(name) < 2 or len(name) > max_length:
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

    def _looks_like_date(self, text: str) -> bool:
        """
        Enhanced date detection - catches Japanese date formats
        Examples:
        - 令和元年6月3日
        - 2019年6月3日
        - (2019年6月3日)
        - 平成30年
        - 令和2年
        """
        date_patterns = [
            # Japanese era dates
            r'令和\d+年',
            r'平成\d+年',
            r'昭和\d+年',
            r'進字\d+年',
            
            # Full Japanese dates (YYYY年M月D日)
            r'\d{4}年\d{1,2}月\d{1,2}日',
            
            # Parenthetical dates
            r'\(\d{4}年\d{1,2}月\d{1,2}日\)',
            
            # Western format dates
            r'\d{4}[-/]\d{1,2}[-/]\d{1,2}',
            
            # Mixed format: 令和元年6月3日(2019年6月3日)
            r'[令平昭]\w+\d+年\d{1,2}月\d{1,2}日',
        ]
        
        return any(re.search(pattern, text) for pattern in date_patterns)
    
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

    def _normalize_encoding(self, text: str) -> str:
        """
        Try to fix garbled UTF-8 text by detecting and re-encoding
        Examples:
        - 'ä¼šç¤¾å' -> '会社名'
        - 'æ‰€åœ¨åœ°' -> '所在地'
        """
        if not text:
            return text
        
        # First, try standard NFKC normalization
        try:
            normalized = unicodedata.normalize('NFKC', text)
            if normalized != text:
                return normalized
        except:
            pass
        
        # If text looks garbled (has mojibake patterns), try to detect encoding
        # Common garbled patterns: å, ç, æ, ä, etc. appearing where CJK should be
        if any(c in text for c in 'äåæçéèêëìíîï'):
            try:
                # Try to detect if it's UTF-8 misinterpreted as Latin-1
                fixed = text.encode('latin-1').decode('utf-8', errors='ignore')
                if fixed and fixed != text:
                    return fixed
            except:
                pass
            
            try:
                # Try UTF-8 with replacement
                fixed = text.encode('utf-8', errors='ignore').decode('utf-8')
                if fixed and fixed != text:
                    return fixed
            except:
                pass
        
        return text

    def _extract_h1_with_legal_entity_split(self, html_content: str) -> List[CompanyNameCandidate]:
        """
        NEW METHOD: Extract company name from h1 by intelligently splitting on delimiters.
        
        Handles cases like:
        - "弁護士法人八千代佐倉総合法律事務所 八千代や佐倉、印西での弁護士相談ならおまかせ"
        → Extracts: "弁護士法人八千代佐倉総合法律事務所"
        
        This runs BEFORE the generic h1 business keyword check to catch legal entity names.
        """
        results = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            print("  [NEW] Checking h1 for legal entity with smart splitting...")
            h1_tags = soup.find_all('h1')
            print(f"  Found {len(h1_tags)} h1 tag(s)")
            
            for idx, h1 in enumerate(h1_tags):
                text = self._clean(h1.get_text(strip=True))
                print(f"    h1[{idx}]: '{text[:80]}'...")
                
                # Check if text starts with a legal entity
                for entity in self.LEGAL_ENTITIES:
                    if text.startswith(entity):
                        print(f"      → Starts with legal entity: {entity}")
                        
                        # Split on common delimiters to remove taglines
                        # Priority order: most specific → least specific
                        delimiters = [
                            ' | ',           # Pipe separator
                            '｜',            # Full-width pipe
                            '　',            # Full-width space (often used before taglines)
                            'での',          # "in/at" marker (e.g., "での弁護士相談")
                            'による',        # "by" marker
                            'へ',            # Direction marker
                            'の',            # Possessive/descriptive marker (be careful with this)
                        ]
                        
                        extracted = text
                        split_on = None
                        
                        for delimiter in delimiters:
                            if delimiter in text:
                                # Split and take first part
                                candidate = text.split(delimiter)[0].strip()
                                
                                # Validate: must still have the legal entity
                                if entity in candidate:
                                    # Check if this looks like a complete company name
                                    # Should be between 5-40 chars for legal entities
                                    if 5 <= len(candidate) <= 40:
                                        extracted = candidate
                                        split_on = delimiter
                                        print(f"      → Split on '{delimiter}': '{extracted}'")
                                        break
                        
                        # Validate extracted name
                        if self._is_valid(extracted) and not self._is_garbage(extracted):
                            # Check if it's just the legal entity alone (too short)
                            if extracted == entity:
                                print(f"      ✗ Just legal entity, no company name")
                                continue
                            
                            has_legal = True
                            confidence = 0.94  # High confidence - legal entity + h1 tag
                            
                            results.append(CompanyNameCandidate(
                                extracted, 
                                'h1_legal_entity_split', 
                                confidence, 
                                'h1_smart_split', 
                                has_legal
                            ))
                            print(f"      ✓ [H1 LEGAL SPLIT] '{extracted}' (conf: {confidence:.2f})")
                            return results  # Return immediately on first match
                
                # If no legal entity at start, check if legal entity appears anywhere in text
                for entity in self.LEGAL_ENTITIES:
                    if entity in text and not text.startswith(entity):
                        print(f"      → Contains legal entity: {entity} (not at start)")
                        
                        # Try to extract the segment with legal entity
                        # Look for patterns: [text][entity][text] -> extract [text][entity]
                        entity_pos = text.find(entity)
                        
                        # Find next delimiter after entity
                        search_start = entity_pos + len(entity)
                        remaining = text[search_start:]
                        
                        delimiters = ['　', ' ', 'での', 'による', 'の', 'へ', '、']
                        
                        extracted = text
                        for delimiter in delimiters:
                            if delimiter in remaining:
                                # Extract from start to first delimiter after entity
                                end_pos = search_start + remaining.find(delimiter)
                                candidate = text[:end_pos].strip()
                                
                                if self._is_valid(candidate) and entity in candidate:
                                    extracted = candidate
                                    print(f"      → Extracted: '{extracted}'")
                                    break
                        
                        if self._is_valid(extracted) and not self._is_garbage(extracted):
                            has_legal = True
                            confidence = 0.93
                            
                            results.append(CompanyNameCandidate(
                                extracted,
                                'h1_legal_entity_split',
                                confidence,
                                'h1_smart_split',
                                has_legal
                            ))
                            print(f"      ✓ [H1 LEGAL SPLIT] '{extracted}' (conf: {confidence:.2f})")
                            return results
            
            print("  ✗ No legal entity found in h1 tags")
        
        except Exception as e:
            logger.error(f"H1 legal entity split error: {e}")
        
        return results

    def _extract_title_introduction_pattern(self, html_content: str) -> List[CompanyNameCandidate]:
        """PHASE 5: Extract from title/heading introduction patterns
        
        Handles patterns like:
        - 行政書士阿部オフィスの紹介 (Introduction to Administrative Scrivener Abe Office)
        - 株式会社〇〇の会社概要 (Company Overview of XX Corporation)
        - 〇〇事務所のご案内 (Guide to XX Office)
        """
        results = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            print("  Checking title/introduction patterns...")
            
            # Check h1, h2, and title tags
            tags_to_check = []
            tags_to_check.extend(soup.find_all(['h1', 'h2']))
            if soup.title:
                tags_to_check.append(soup.title)
            
            print(f"  Found {len(tags_to_check)} heading/title tag(s)")
            
            for idx, tag in enumerate(tags_to_check):
                text = self._clean(tag.get_text(strip=True))
                print(f"    [{tag.name}] '{text[:80]}'...")
                
                # Pattern 1: [Legal Entity][Company Name]の[Introduction Word]
                # Example: 行政書士阿部オフィスの紹介
                introduction_words = ['紹介', '案内', 'ご案内', '会社概要', '事務所概要', 
                                    '会社案内', '事務所案内', 'について', '概要']
                
                for intro_word in introduction_words:
                    if text.endswith(f'の{intro_word}') or text.endswith(intro_word):
                        # Remove the introduction suffix
                        clean_text = text.replace(f'の{intro_word}', '').replace(intro_word, '').strip()
                        
                        # Check if it contains legal entity or professional designation
                        professional_designations = ['行政書士', '弁護士', '司法書士', '税理士', 
                                                    '公認会計士', '社会保険労務士', '弁理士']
                        
                        has_designation = any(desig in clean_text for desig in professional_designations)
                        has_legal_entity = any(entity in clean_text for entity in self.LEGAL_ENTITIES)
                        
                        if has_designation or has_legal_entity:
                            # Extract just the company/office name
                            company_name = clean_text
                            
                            # Remove professional designation prefix if present
                            for desig in professional_designations:
                                if company_name.startswith(desig):
                                    company_name = company_name[len(desig):].strip()
                            
                            # Validate
                            if self._is_valid(company_name) and not self._is_garbage(company_name):
                                confidence = 0.90 if has_legal_entity else 0.88
                                
                                results.append(CompanyNameCandidate(
                                    company_name, 
                                    'title_introduction_pattern', 
                                    confidence, 
                                    'title_intro', 
                                    has_legal_entity
                                ))
                                print(f"    ✓ [TITLE INTRO] '{intro_word}' → {company_name} (confidence: {confidence:.2f})")
                                return results  # Return immediately on first match
                
                # Pattern 2: [Company Name][Office/Business Type]の[Introduction Word]
                # Example: 〇〇探偵事務所のご案内
                office_types = ['事務所', 'オフィス', '法人', '株式会社', '会社', '協会', '組合']
                
                for office_type in office_types:
                    for intro_word in introduction_words:
                        pattern = f'{office_type}の{intro_word}'
                        if pattern in text:
                            # Extract everything before the pattern
                            company_name = text.split(pattern)[0].strip()
                            
                            # Add back the office type if it's not a legal entity
                            if office_type not in self.LEGAL_ENTITIES and office_type in ['事務所', 'オフィス']:
                                company_name = company_name + office_type
                            
                            if self._is_valid(company_name) and not self._is_garbage(company_name):
                                has_legal_entity = any(entity in company_name for entity in self.LEGAL_ENTITIES)
                                confidence = 0.89 if has_legal_entity else 0.87
                                
                                results.append(CompanyNameCandidate(
                                    company_name, 
                                    'title_introduction_pattern', 
                                    confidence, 
                                    'title_intro', 
                                    has_legal_entity
                                ))
                                print(f"    ✓ [TITLE INTRO] Pattern '{pattern}' → {company_name} (confidence: {confidence:.2f})")
                                return results
            
            print("  ✗ No title/introduction patterns found")
        
        except Exception as e:
            logger.debug(f"Title introduction pattern error: {e}")
        
        return results

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
    
    def _calculate_completeness(self, company_name: str) -> float:
        """Calculate how 'complete' a company name entry is (0.0 to 1.0)
        
        Fuller company names with descriptions score higher:
        - 株式会社 シマトモ = 0.3 (basic name)
        - 株式会社　ナショナル・エージェント・カンパニー　略称：NAC = 0.8 (full info)
        - 株式会社　ナショナル・エージェント・カンパニー　略称：NAC（ナック） = 1.0 (complete)
        """
        score = 0.0
        
        # Has multiple words/segments (multi-part name)
        parts = company_name.split('　')  # Full-width space
        if len(parts) >= 3:
            score += 0.3
        elif len(parts) >= 2:
            score += 0.15
        
        # Has abbreviated form indicator
        if '略称' in company_name or '：' in company_name or ':' in company_name:
            score += 0.3
        
        # Has parentheses with abbreviation
        if '（' in company_name and '）' in company_name:
            score += 0.25
        
        # Has punctuation/special formatting (suggests formal company info)
        if '・' in company_name:
            score += 0.1
        
        # Length bonus - fuller names are usually more complete
        if len(company_name) > 20:
            score += 0.15
        elif len(company_name) > 15:
            score += 0.08
        
        return min(score, 1.0)
    
    def _label_matches_company_name(self, label: str) -> Tuple[bool, float]:
        """Check if label indicates a company name field"""
        if not label:
            return False, 0.0

        label = self._normalize_encoding(label)
        label_lower = label.lower().strip()
        label_normalized = re.sub(r'\s+', '', label)
        
        # === EXCLUDED LABELS - Skip these ===
        excluded = [
            '項目', '住所', '価格', '料金', '費用', '時間', '金額',
            'item', 'price', 'cost', 'fee', 'amount',
            'メディア名', '番組名', '放送局', 'タイトル', '出演',
            'media', 'program', 'title', 'show', 'broadcast',
            '加盟団体', '所属団体', 'affiliated', 'member of',
            '血液型', '出身地', '大好物', '長所', '短所', '趣味', '隠れた能力',
            '保有資格', 'blood', 'origin', 'hobby', 'skill', 'qualification',
            'tel', 'phone', 'fax', 'email',
        ]
        
        for excluded_term in excluded:
            if excluded_term in label or excluded_term in label_lower:
                return False, 0.0
        
        # === PRIMARY LABELS - High confidence match ===
        primary = [
            '会社名', '商号', '法人名', '企業名', '正式名称', '名称', '社名',
            '事業者名', '法人の名称', '屋号', '法人名称', '運営会社', '運営法人',
            '事務所名', '事務所', '店舗名', '施設名',
            'company name', 'company', '会社', 'name', 'corporation','団体名'
        ]
        
        for primary_term in primary:
            # Exact match (normalized)
            if primary_term == label or primary_term == label_normalized:
                return True, 1.0
            
            # Contains match
            if primary_term in label or primary_term in label_lower:
                return True, 0.95
        
        # === SECONDARY LABELS - Medium confidence match ===
        secondary = [
            '名前', '名前', 'name',
            '組織', '団体', 'organization',
        ]
        
        for secondary_term in secondary:
            if secondary_term in label_lower or secondary_term in label:
                # But exclude if it's clearly about overview/summary
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
        """Homepage fallback extraction - UPDATED to use smart split first."""
        results = []
        soup = BeautifulSoup(html_content, 'html.parser')
        for tag in soup(['script', 'style', 'noscript']):
            tag.decompose()
        
        # FIRST: Try new smart split method (handles legal entities better)
        smart_split_results = self._extract_h1_with_legal_entity_split(html_content)
        if smart_split_results:
            return smart_split_results
        
        # FALLBACK: Original business keyword matching
        print("  Checking h1 tags (business keyword fallback)...")
        h1_tags = soup.find_all('h1')
        print(f"  Found {len(h1_tags)} h1 tag(s)")
        
        for idx, h1 in enumerate(h1_tags):
            text = self._clean(h1.get_text(strip=True))
            print(f"    h1[{idx}]: '{text[:60]}'...")
            
            business_keywords = ['探偵事務所', '調査事務所', '探偵社', '調査会社', 
                                '法律事務所', '会計事務所', 'コンサルティング']
            
            if any(kw in text for kw in business_keywords):
                print(f"    ✓ [BUSINESS MATCH]")
                
                # Try to clean up taglines from business names
                # Split on common separators
                for separator in ['　', ' ', 'での', 'なら', 'へ', 'の']:
                    if separator in text:
                        candidate = text.split(separator)[0].strip()
                        if self._is_valid(candidate):
                            text = candidate
                            break
                
                if self._is_valid(text):
                    results.append(CompanyNameCandidate(text, 'homepage_h1', 0.88, 'business_name', False))
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
    
    def _extract_page_with_debug(self, html_content: str, page_url: str, source_type: str) -> List:
        """
        Enhanced table extraction with better label debugging
        Prints actual label values to help diagnose encoding issues
        """
        results = []
        seen = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            # === TABLE EXTRACTION ===
            tables = soup.find_all('table')
            if tables:
                print(f"      Found {len(tables)} table(s)")
            
            for table_idx, table in enumerate(tables):
                print(f"\n      === TABLE {table_idx} ===")
                table_context = table.get_text()
                affiliate_keywords = ['関連会社', '子会社', 'パートナー', 'グループ会社', 
                                    'subsidiary', 'partner', 'affiliated']
                
                if table_idx > 0 and any(kw in table_context for kw in affiliate_keywords):
                    print(f"      ⊘ [SKIP] Appears to be affiliate/partner list")
                    continue
                
                row_num = 0
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        row_num += 1
                        
                        # Skip empty rows
                        if not label and not value:
                            continue
                        
                        # Debug output: show raw label and value
                        print(f"      Row {row_num}: label='{label}' → value='{value[:50]}'")
                        
                        # Skip obvious duplicates
                        if value == label or value in self.PRIMARY_COMPANY_LABELS:
                            print(f"        ⊘ [SKIP] Value matches label")
                            continue
                        
                        # Normalize to check for near-duplicates
                        value_normalized = re.sub(r'[\s　]+', '', value)
                        label_normalized = re.sub(r'[\s　]+', '', label)
                        if value_normalized == label_normalized:
                            print(f"        ⊘ [SKIP] Normalized value == label")
                            continue
                        
                        # Check if label matches company name pattern
                        matches, conf_boost = self._label_matches_company_name(label)
                        
                        if not matches:
                            print(f"        ⊘ Label not recognized as company name field")
                            continue
                        
                        print(f"        ✓ Label recognized (boost: {conf_boost:.2f})")
                        
                        # Check for date
                        if self._looks_like_date(value):
                            print(f"        ⊘ [SKIP DATE] Value is a date")
                            continue
                        
                        # Validate value
                        if not self._is_valid(value):
                            print(f"        ⊘ [SKIP] Value fails validation")
                            continue
                        
                        # Clean value
                        cleaned = self._clean(self._remove_seo(value))
                        
                        if not cleaned or cleaned in self.PRIMARY_COMPANY_LABELS:
                            print(f"        ⊘ [SKIP] Cleaned value is empty/invalid")
                            continue
                        
                        # Skip affiliates
                        if any(marker in cleaned for marker in ['関連会社', '子会社', 'USA', 'Inc(']):
                            print(f"        ⊘ [SKIP] Affiliate marker detected")
                            continue
                        
                        # Extract from mixed text
                        if any(e in cleaned for e in self.LEGAL_ENTITIES):
                            extracted = self._extract_company_from_mixed_text(cleaned)
                            if extracted:
                                cleaned = extracted
                                print(f"        → Extracted from mixed: {cleaned}")
                        
                        if cleaned and cleaned not in seen and not self._is_garbage(cleaned):
                            seen.add(cleaned)
                            has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                            
                            completeness = self._calculate_completeness(cleaned)
                            confidence = 0.99 if has_legal else 0.95 + (conf_boost * 0.04)
                            confidence = min(confidence + (completeness * 0.02), 0.99)
                            
                            results.append(CompanyNameCandidate(
                                cleaned, f'{source_type}_table', confidence, 'table_field', has_legal
                            ))
                            print(f"        ✓ [MATCH] '{cleaned}' (confidence: {confidence:.2f})")
                
                if results:
                    best = max(results, key=lambda x: x.confidence)
                    print(f"\n      Best match from table {table_idx}: {best.value}")
                    return [best]
            
            # === TEXT PATTERN FALLBACK ===
            print(f"      No table results - trying text patterns...")
            text = soup.get_text()
            
            for label_kw in self.PRIMARY_COMPANY_LABELS:
                pattern = re.escape(label_kw) + r'\s*[:：]\s*([\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff0-9ー\s]{2,50})'
                for match in re.finditer(pattern, text, re.UNICODE):
                    candidate = match.group(1).strip()
                    
                    if self._looks_like_date(candidate):
                        continue
                    
                    cleaned = self._clean(self._remove_seo(candidate))
                    
                    if cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_text', 0.85, 'text_pattern_label', False
                        ))
                        print(f"      ✓ [TEXT PATTERN] {cleaned}")
                        return results
        
        except Exception as e:
            logger.error(f"Page extraction error: {e}")
        
        return results


    # Also add this helper for raw text scanning (fallback when structure fails)
    def _extract_company_from_raw_text(self, html_content: str) -> Optional[str]:
        """
        Last resort: Scan raw text for company name patterns
        Looks for common company name indicators in visible text
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            text = soup.get_text()
            
            # Look for patterns like "会社名: XXX" or "Company: XXX"
            patterns = [
                r'会社名\s*[:：]\s*([^\n\r。、]+)',
                r'社名\s*[:：]\s*([^\n\r。、]+)',
                r'企業名\s*[:：]\s*([^\n\r。、]+)',
                r'(?:about|company|our)\s+([a-zA-Z][a-zA-Z0-9\s\-\.]+?)(?:\s*\||$)',
            ]
            
            for pattern in patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE | re.UNICODE)
                for match in matches:
                    candidate = match.group(1).strip()
                    if self._is_valid(candidate) and not self._is_garbage(candidate):
                        return candidate
        
        except Exception as e:
            logger.debug(f"Raw text extraction error: {e}")
        
        return None

    
    def _extract_page(self, html_content: str, page_url: str, source_type: str) -> List:
        """
        Extract company name from structured page content (tables, definition lists)
        CRITICAL: Check for dates BEFORE adding to results
        """
        results = []
        seen = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup(['script', 'style', 'noscript']):
                tag.decompose()
            
            # === TABLE EXTRACTION ===
            tables = soup.find_all('table')
            if tables:
                print(f"      Found {len(tables)} table(s)")
            
            for table_idx, table in enumerate(tables):
                table_context = table.get_text()
                affiliate_keywords = ['関連会社', '子会社', 'パートナー', 'グループ会社']
                
                if table_idx > 0 and any(kw in table_context for kw in affiliate_keywords):
                    print(f"      ⊘ [SKIP TABLE {table_idx}] Appears to be affiliate list")
                    continue
                
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) < 2:
                        continue
                    
                    # Get label and value
                    label_raw = cells[0].get_text(strip=True)
                    value_raw = cells[1].get_text(strip=True)
                    
                    # === NORMALIZE ENCODING ===
                    label = self._normalize_encoding(label_raw)
                    value = self._normalize_encoding(value_raw)
                    
                    if not label and not value:
                        continue
                    
                    # Debug output
                    print(f"        -> label='{label}' (raw: '{label_raw[:30]}')")
                    
                    # Skip duplicates
                    if value == label or value in self.PRIMARY_COMPANY_LABELS:
                        continue
                    
                    # Check label match
                    matches, conf_boost = self._label_matches_company_name(label)
                    
                    if not matches:
                        continue
                    
                    print(f"        ✓ Label matched: '{label}'")
                    
                    # Skip dates
                    if self._looks_like_date(value):
                        print(f"        ⊘ [DATE] {value}")
                        continue
                    
                    # Validate and clean value
                    if not self._is_valid(value):
                        print(f"        ⊘ [INVALID] {value}")
                        continue
                    
                    cleaned = self._clean(self._remove_seo(value))
                    
                    if not cleaned or cleaned in self.PRIMARY_COMPANY_LABELS:
                        continue
                    
                    # Skip affiliates
                    if any(m in cleaned for m in ['関連会社', '子会社', 'USA', 'Inc(']):
                        print(f"        ⊘ [AFFILIATE] {cleaned}")
                        continue
                    
                    # Extract from mixed text
                    if any(e in cleaned for e in self.LEGAL_ENTITIES):
                        extracted = self._extract_company_from_mixed_text(cleaned)
                        if extracted:
                            cleaned = extracted
                    
                    if cleaned and cleaned not in seen and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                        
                        completeness = self._calculate_completeness(cleaned)
                        confidence = 0.99 if has_legal else 0.95 + (conf_boost * 0.04)
                        confidence = min(confidence + (completeness * 0.02), 0.99)
                        
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_table', confidence, 'table_field', has_legal
                        ))
                        print(f"        ✓ [MATCH] {cleaned} (conf: {confidence:.2f})")
                
                if results:
                    best = max(results, key=lambda x: x.confidence)
                    print(f"      → Selected: {best.value}")
                    return [best]
            
            # === DL EXTRACTION (if no tables worked) ===
            dls = soup.find_all('dl')
            if dls:
                print(f"      Found {len(dls)} definition list(s)")
                
                for dl in dls:
                    # === TRY STANDARD DL EXTRACTION FIRST ===
                    dts = dl.find_all('dt')
                    dds = dl.find_all('dd')
                    
                    if dts and dds:
                        for dt_idx, dt in enumerate(dts):
                            label_raw = dt.get_text(strip=True)
                            label = self._normalize_encoding(label_raw)
                            
                            if dt_idx < len(dds):
                                value = dds[dt_idx].get_text(strip=True)
                                
                                matches, conf_boost = self._label_matches_company_name(label)
                                
                                if matches and value:
                                    if self._looks_like_date(value):
                                        continue
                                    
                                    cleaned = self._clean(self._remove_seo(value))
                                    
                                    if any(e in cleaned for e in self.LEGAL_ENTITIES):
                                        extracted = self._extract_company_from_mixed_text(cleaned)
                                        if extracted:
                                            cleaned = extracted
                                    
                                    if cleaned and cleaned not in seen and self._is_valid(cleaned):
                                        seen.add(cleaned)
                                        has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                                        confidence = 0.99 if has_legal else 0.95 + (conf_boost * 0.04)
                                        
                                        results.append(CompanyNameCandidate(
                                            cleaned, f'{source_type}_dl', confidence, 'dl_field', has_legal
                                        ))
                                        print(f"        ✓ [DL MATCH] {cleaned}")
                    
                    # === FALLBACK: MALFORMED DL EXTRACTION ===
                    # For malformed <dl> without closing tags, parse raw HTML
                    if not results:
                        print(f"      Trying malformed DL fallback...")
                        # Get the raw HTML of this DL element
                        dl_html = str(dl)
                        
                        # Extract dt/dd pairs using regex on raw HTML
                        # Pattern: <dt...>label</dt> OR <dt...>label (without closing)
                        # followed by <dd...>value</dd> OR <dd...>value (without closing)
                        pattern = r'<dt[^>]*>([^<]*(?:<[^/][^>]*>[^<]*)*?)<(?:dt|/dl|dd)|<dd[^>]*>([^<]*(?:<[^/][^>]*>[^<]*)*?)<(?:dt|/dl|dd)'
                        
                        # More robust: split on <dt and <dd, then pair them
                        dt_pattern = r'<dt[^>]*>(.*?)(?=<(?:dt|dd|/dl))'
                        dd_pattern = r'<dd[^>]*>(.*?)(?=<(?:dt|dd|/dl))'
                        
                        dts_raw = re.findall(dt_pattern, dl_html, re.DOTALL | re.IGNORECASE)
                        dds_raw = re.findall(dd_pattern, dl_html, re.DOTALL | re.IGNORECASE)
                        
                        print(f"        Found {len(dts_raw)} <dt> and {len(dds_raw)} <dd> elements")
                        
                        i = 0
                        while i < len(all_children):
                            if all_children[i].name == 'dt' and i + 1 < len(all_children) and all_children[i + 1].name == 'dd':
                                label_raw = all_children[i].get_text(strip=True)
                                value_raw = all_children[i + 1].get_text(strip=True)
                            
                            label = self._normalize_encoding(label_raw)
                            value = self._normalize_encoding(value_raw)
                            
                            print(f"        [MALFORMED DL] label='{label}' → value='{value[:50]}'")
                            
                            matches, conf_boost = self._label_matches_company_name(label)
                            print(f"          Label match: {matches}")
                            
                            if not matches:
                                print(f"          ⊘ Label not recognized")
                                continue
                            
                            if not value:
                                print(f"          ⊘ No value")
                                continue
                            
                            if self._looks_like_date(value):
                                print(f"          ⊘ Value is a date")
                                continue
                            
                            if not self._is_valid(value):
                                print(f"          ⊘ Value fails validation: '{value}'")
                                continue
                            
                            cleaned = self._clean(self._remove_seo(value))
                            print(f"          Cleaned: '{cleaned}'")
                            
                            if any(e in cleaned for e in self.LEGAL_ENTITIES):
                                extracted = self._extract_company_from_mixed_text(cleaned)
                                if extracted:
                                    cleaned = extracted
                                    print(f"          Extracted from mixed: '{cleaned}'")
                            
                            if self._is_garbage(cleaned):
                                print(f"          ⊘ Contains garbage patterns")
                                continue
                            
                            if cleaned not in seen:
                                seen.add(cleaned)
                                has_legal = any(e in cleaned for e in self.LEGAL_ENTITIES)
                                confidence = 0.98 if has_legal else 0.94 + (conf_boost * 0.04)
                                
                                results.append(CompanyNameCandidate(
                                    cleaned, f'{source_type}_dl_malformed', confidence, 'dl_field', has_legal
                                ))
                                print(f"        ✓ [MALFORMED MATCH] {cleaned} (conf: {confidence:.2f})")
            
            if results:
                return results
            
            # === DT/DD MERGED EXTRACTION (NEW) ===
            # For malformed DL where all content is merged into single dt elements
            if not results:
                print(f"      Trying merged DT/DD extraction...")
                merged_results = self._extract_from_dt_dd_merged(html_content)
                results.extend(merged_results)
                if merged_results:
                    return merged_results
            
            # === TEXT PATTERN FALLBACK ===
            print(f"      No structured results - trying text patterns...")
            text = soup.get_text()
            
            for label_kw in self.PRIMARY_COMPANY_LABELS:
                pattern = re.escape(label_kw) + r'\s*[:：]\s*([\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff0-9ー\s]{2,50})'
                for match in re.finditer(pattern, text, re.UNICODE):
                    candidate = match.group(1).strip()
                    
                    if self._looks_like_date(candidate):
                        continue
                    
                    cleaned = self._clean(self._remove_seo(candidate))
                    
                    if cleaned not in seen and self._is_valid(cleaned) and not self._is_garbage(cleaned):
                        seen.add(cleaned)
                        results.append(CompanyNameCandidate(
                            cleaned, f'{source_type}_text', 0.85, 'text_pattern_label', False
                        ))
                        print(f"      ✓ [TEXT] {cleaned}")
                        return results
        
        except Exception as e:
            logger.error(f"Extraction error: {e}")
        
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