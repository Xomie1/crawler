"""
Industry Extraction Module
Extracts industry information from websites using multiple detection methods.
"""

import re
import json
import logging
from typing import Dict, List, Optional, Set
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class IndustryCandidate:
    """Represents an industry candidate with confidence."""
    
    def __init__(self, value: str, source: str, confidence: float):
        self.value = value
        self.source = source
        self.confidence = confidence
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            'value': self.value,
            'source': self.source,
            'confidence': self.confidence
        }


class IndustryExtractor:
    """Extracts industry information from websites."""
    
    # Industry keywords mapping (Japanese only)
    INDUSTRY_KEYWORDS = {
        'technology': ['IT', '情報技術', 'ソフトウェア', 'テクノロジー', 'システム開発', 'クラウド', 'AI', '人工知能', '情報システム', 'システムインテグレーション'],
        'finance': ['金融', '銀行', '保険', '証券', '投資', '資産運用', 'ファイナンス', '信用金庫', '信用組合', '証券会社'],
        'retail': ['小売', 'ショップ', '店舗', 'EC', 'ECサイト', 'オンラインショップ', '通販', 'ネットショップ', '百貨店', 'スーパー'],
        'healthcare': ['医療', '病院', 'クリニック', 'ヘルスケア', '製薬', '薬品', '医療機器', '診療所', '医院', '薬局'],
        'education': ['教育', '学校', '大学', '学習', 'トレーニング', 'アカデミー', 'スクール', '塾', '予備校', '専門学校'],
        'manufacturing': ['製造', '工場', '生産', '工業', 'メーカー', '製造業', '生産管理', '工場管理'],
        'construction': ['建設', '建築', '工事', '土木', 'エンジニアリング', '建築設計', '施工管理'],
        'real_estate': ['不動産', '住宅', 'マンション', '土地', '賃貸', '不動産管理', '宅地建物取引'],
        'food': ['食品', 'レストラン', '飲食', '外食', '飲料', 'フードサービス', '食品製造', '食品加工'],
        'automotive': ['自動車', '車', 'カー', 'モビリティ', '自動車関連', '自動車部品', '自動車販売'],
        'energy': ['エネルギー', '電力', '電気', '再生可能エネルギー', '太陽光', '風力', '発電', '電力会社'],
        'logistics': ['物流', '運輸', '配送', '輸送', 'サプライチェーン', '運送', '倉庫', '物流センター'],
        'consulting': ['コンサルティング', 'コンサル', 'アドバイザリー', '経営コンサル', '経営相談'],
        'media': ['メディア', '出版', '放送', 'エンターテインメント', '広告', '広告代理店', 'テレビ', 'ラジオ'],
        'telecommunications': ['通信', 'テレコム', 'モバイル', '無線', '通信事業', '通信会社', '携帯電話']
    }
    
    def __init__(self, base_url: str, fetcher=None):
        """
        Initialize industry extractor.
        
        Args:
            base_url: Base URL of the website
            fetcher: PageFetcher instance for fetching additional pages
        """
        self.base_url = base_url
        self.fetcher = fetcher
    
    def extract(self, html_content: str, final_url: Optional[str] = None) -> Dict:
        """
        Extract industry information using all methods.
        
        Args:
            html_content: HTML content to parse
            final_url: Final URL after redirects
            
        Returns:
            Dictionary with industry, source, confidence, and candidates
        """
        url = final_url or self.base_url
        candidates: List[IndustryCandidate] = []
        
        # Extract from multiple sources
        meta_result = self._extract_from_metadata(html_content, url)
        if meta_result:
            candidates.append(meta_result)
        
        jsonld_result = self._extract_from_jsonld(html_content, url)
        if jsonld_result:
            candidates.append(jsonld_result)
        
        text_result = self._extract_from_text(html_content, url)
        if text_result:
            candidates.append(text_result)
        
        # Select best candidate (highest confidence)
        result = {
            'industry': None,
            'industry_source': None,
            'industry_confidence': 0.0,
            'industry_candidates': [c.to_dict() for c in candidates]
        }
        
        if candidates:
            # Sort by confidence (descending)
            candidates.sort(key=lambda x: x.confidence, reverse=True)
            best = candidates[0]
            
            result['industry'] = best.value
            result['industry_source'] = best.source
            result['industry_confidence'] = best.confidence
            
            logger.info(
                f"Extracted industry: {best.value} "
                f"(source: {best.source}, confidence: {best.confidence:.2f})"
            )
        
        return result
    
    def _extract_from_metadata(self, html_content: str, url: str) -> Optional[IndustryCandidate]:
        """Extract industry from meta tags and structured data."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Check meta description
            meta_description = soup.find('meta', {'name': 'description'})
            if meta_description:
                description = meta_description.get('content', '').lower()
                industry = self._match_industry_keywords(description)
                if industry:
                    logger.debug(f"Found industry in meta description: {industry}")
                    return IndustryCandidate(industry, 'metadata', 0.8)
            
            # Check og:description
            og_description = soup.find('meta', property='og:description')
            if og_description:
                description = og_description.get('content', '').lower()
                industry = self._match_industry_keywords(description)
                if industry:
                    logger.debug(f"Found industry in og:description: {industry}")
                    return IndustryCandidate(industry, 'metadata', 0.8)
            
            # Check keywords meta tag
            meta_keywords = soup.find('meta', {'name': 'keywords'})
            if meta_keywords:
                keywords = meta_keywords.get('content', '').lower()
                industry = self._match_industry_keywords(keywords)
                if industry:
                    logger.debug(f"Found industry in meta keywords: {industry}")
                    return IndustryCandidate(industry, 'metadata', 0.75)
            
        except Exception as e:
            logger.error(f"Error extracting industry from metadata: {e}")
        
        return None
    
    def _extract_from_jsonld(self, html_content: str, url: str) -> Optional[IndustryCandidate]:
        """Extract industry from JSON-LD structured data."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find JSON-LD scripts
            jsonld_scripts = soup.find_all('script', type='application/ld+json')
            for script in jsonld_scripts:
                try:
                    data = json.loads(script.string)
                    industry = self._extract_industry_from_json(data)
                    if industry:
                        logger.debug(f"Found industry in JSON-LD: {industry}")
                        return IndustryCandidate(industry, 'jsonld', 0.9)
                except (json.JSONDecodeError, TypeError):
                    continue
            
        except Exception as e:
            logger.error(f"Error extracting industry from JSON-LD: {e}")
        
        return None
    
    def _extract_industry_from_json(self, data: any) -> Optional[str]:
        """Recursively extract industry from JSON structure."""
        if isinstance(data, dict):
            # Check for industry-related fields
            industry_fields = ['industry', 'sector', 'businessType', '@type']
            for field in industry_fields:
                if field in data:
                    value = str(data[field]).lower()
                    industry = self._match_industry_keywords(value)
                    if industry:
                        return industry
            
            # Check @type for schema.org types
            if '@type' in data:
                schema_type = str(data['@type']).lower()
                # Map schema.org types to industries
                schema_mapping = {
                    'softwareapplication': 'technology',
                    'financialservice': 'finance',
                    'store': 'retail',
                    'hospital': 'healthcare',
                    'school': 'education',
                    'organization': None,  # Too generic
                }
                if schema_type in schema_mapping and schema_mapping[schema_type]:
                    return schema_mapping[schema_type]
            
            # Recursively search
            for value in data.values():
                if isinstance(value, (dict, list)):
                    result = self._extract_industry_from_json(value)
                    if result:
                        return result
        
        elif isinstance(data, list):
            for item in data:
                result = self._extract_industry_from_json(item)
                if result:
                    return result
        
        return None
    
    def _extract_from_text(self, html_content: str, url: str) -> Optional[IndustryCandidate]:
        """Extract industry from page text content."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract text from key sections
            sections = []
            
            # Check title
            title_tag = soup.find('title')
            if title_tag:
                sections.append(title_tag.get_text())
            
            # Check H1 tags
            h1_tags = soup.find_all('h1')
            for h1 in h1_tags[:3]:  # Limit to first 3
                sections.append(h1.get_text())
            
            # Check meta description
            meta_desc = soup.find('meta', {'name': 'description'})
            if meta_desc:
                sections.append(meta_desc.get('content', ''))
            
            # Combine and search
            combined_text = ' '.join(sections).lower()
            industry = self._match_industry_keywords(combined_text)
            if industry:
                logger.debug(f"Found industry in text: {industry}")
                return IndustryCandidate(industry, 'text', 0.6)
            
        except Exception as e:
            logger.error(f"Error extracting industry from text: {e}")
        
        return None
    
    def _match_industry_keywords(self, text: str) -> Optional[str]:
        """Match text against industry keywords and return best match."""
        if not text:
            return None
        
        best_match = None
        best_score = 0
        
        for industry, keywords in self.INDUSTRY_KEYWORDS.items():
            score = 0
            
            # Check Japanese keywords only
            for keyword in keywords:
                if keyword in text:
                    score += 1
            
            if score > best_score:
                best_score = score
                best_match = industry
        
        return best_match if best_score > 0 else None

