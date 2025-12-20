"""
Groq Response Normalizer - FIXED VERSION
Parses Groq's text-based extraction output into structured JSON
"""

import re
import logging

logger = logging.getLogger(__name__)

def normalize_groq_text(text: str, html_content: str = None) -> dict:
    """
    Parse Groq's text output into structured extraction results.
    
    Handles various output formats from Groq:
    - Structured: 会社名：値\nメールアドレス：値\n業界：値
    - Alternate: Company Name: 値
    - Mixed Japanese/English
    
    Args:
        text: Raw text response from Groq
        html_content: Original HTML (for fallback extraction)
    
    Returns:
        Dictionary with company_name, email, industry
    """
    
    logger.debug(f"Normalizing Groq response (first 200 chars):\n{text[:200]}")
    
    # Extract fields using Japanese labels (primary)
    company = _extract_field(text, ['会社名', 'company name', 'company'])
    email = _extract_field(text, ['メールアドレス', 'email address', 'email', 'e-mail'])
    industry_jp = _extract_field(text, ['業界', 'industry'])
    
    # Fallback: if email not found, try aggressive regex search
    if not email:
        email = _extract_email_fallback(text, html_content)
        if email:
            logger.info(f"Email extracted via fallback: {email}")
    
    # Clean and validate
    company = clean_value(company)
    email = clean_value(email)
    industry_jp = clean_value(industry_jp)
    
    # Reject obvious prompt template values
    if company == "会社名 or なし" or company == "会社名":
        company = None
        logger.warning("Rejected literal prompt template for company")
    
    if email == "メールアドレス or なし" or email == "メールアドレス":
        email = None
        logger.warning("Rejected literal prompt template for email")
    
    if industry_jp == "業界名 or なし" or industry_jp == "業界名":
        industry_jp = None
        logger.warning("Rejected literal prompt template for industry")
    
    # Map industry to standard category
    industry = industry_jp if industry_jp else None
    
    result = {
        "company_name": {
            "value": company if company and company != "なし" else None,
            "confidence": 0.9 if company and company != "なし" else 0.0,
            "source": "ai_groq"
        },
        "email": {
            "value": email if email and email != "なし" else None,
            "confidence": 0.95 if (email and email != "なし") else 0.0,
            "source": "ai_groq" if email and email != "なし" else "not_found"
        },
        "industry": {
            "value": industry,
            "confidence": 0.8 if industry else 0.0,
            "source": "ai_groq" if industry_jp and industry_jp != "なし" else "not_found"
        }
    }
    
    logger.info(f"Normalized: company={company}, email={email}, industry={industry}")
    return result


def _extract_field(text: str, labels: list) -> str | None:
    """
    Extract field value from text using multiple label variations.
    
    Handles formats like:
    - 会社名：セルビスメンバーズ
    - 会社名: セルビスメンバーズ
    - Company name: セルビスメンバーズ
    - 会社名セルビスメンバーズ (no separator)
    
    Args:
        text: Text to search
        labels: List of label variations to try
    
    Returns:
        Extracted value or None
    """
    
    if not text:
        return None
    
    for label in labels:
        # Try different separator patterns
        for separator in [
            r'：\s*',      # Full-width colon with space
            r':\s*',       # Half-width colon with space
            r'：',         # Full-width colon no space
            r':',          # Half-width colon no space
            r'｜\s*',      # Full-width pipe
            r'\|\s*',      # Half-width pipe
            r'\s+',        # Just whitespace
        ]:
            # Build regex pattern: label + separator + value until newline
            pattern = rf'{re.escape(label)}{separator}([^\n]+?)(?:\n|$)'
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            
            if match:
                value = match.group(1).strip()
                if value and len(value) > 1:  # Must have at least 2 chars
                    logger.debug(f"Found '{label}' → '{value}'")
                    return value
    
    return None


def _extract_email_fallback(text: str, html_content: str = None) -> str | None:
    """
    Aggressive email extraction using multiple strategies.
    
    Args:
        text: Groq response text
        html_content: Original HTML content
    
    Returns:
        Email address or None
    """
    
    EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    
    # Strategy 1: Search in Groq response text
    matches = EMAIL_REGEX.findall(text)
    for email in matches:
        if is_valid_email(email):
            logger.info(f"Strategy 1 - Found email in Groq response: {email}")
            return email
    
    # Strategy 2: Search in HTML if provided
    if html_content:
        matches = EMAIL_REGEX.findall(html_content)
        for email in matches:
            if is_valid_email(email):
                logger.info(f"Strategy 2 - Found email in HTML: {email}")
                return email
    
    # Strategy 3: Look for "contact" or "info" sections in text
    contact_pattern = re.compile(
        r'(?:contact|mail|email|info|question)[\s:]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        re.IGNORECASE
    )
    match = contact_pattern.search(text or '' + html_content or '')
    if match:
        email = match.group(1)
        if is_valid_email(email):
            logger.info(f"Strategy 3 - Found email via context: {email}")
            return email
    
    logger.debug("No email found via any strategy")
    return None


def is_valid_email(email: str) -> bool:
    """
    Check if email looks valid.
    
    Args:
        email: Email to validate
    
    Returns:
        True if valid
    """
    if not email:
        return False
    
    email_lower = email.lower()
    
    # Reject obvious invalid emails
    invalid_patterns = [
        'noreply',
        'no-reply',
        'test@',
        '@test',
        'placeholder',
        'example.com',
        'メール',           # Japanese placeholder text
        'address',
        'not found',
        'or なし',         # Prompt template leak
    ]
    
    for pattern in invalid_patterns:
        if pattern in email_lower:
            logger.debug(f"Rejected email '{email}' - contains '{pattern}'")
            return False
    
    # Must have valid length
    if len(email) < 5 or len(email) > 254:
        return False
    
    return True


def map_industry(jp: str) -> str | None:
    """
    Map Japanese industry text to standardized category.
    
    Args:
        jp: Japanese industry text (e.g., "冠婚葬祭業", "製造業")
    
    Returns:
        Standard industry category or None
    """
    if not jp:
        return None
    
    jp_lower = jp.lower()
    
    # Comprehensive industry mapping
    industry_map = {
        'technology': [
            'it', 'システム', 'ソフトウェア', 'クラウド', 'ai', '人工知能',
            'tech', 'テック', 'アプリ', 'ウェブ', 'デジタル'
        ],
        'finance': [
            '銀行', '金融', '証券', '保険', '投資', 'ファイナンス',
            'ファイナンシャル', 'バンク'
        ],
        'retail': [
            '小売', 'ショップ', 'ec', 'eコマース', '販売', '店舗',
            'リテール', 'ストア'
        ],
        'healthcare': [
            '医療', '病院', 'クリニック', '診療', '医学', '薬局',
            '製薬', 'ヘルスケア', 'メディカル'
        ],
        'education': [
            '教育', '学校', '大学', '学習', 'スクール', 'トレーニング',
            'エデュケーション', 'アカデミー'
        ],
        'manufacturing': [
            '製造', '製造業', '工場', '生産', 'メーカー', '加工',
            'マニュファクチャリング'
        ],
        'construction': [
            '建設', '建築', '工事', '土木', 'エンジニアリング',
            'コンストラクション'
        ],
        'real_estate': [
            '不動産', '住宅', 'マンション', '賃貸', '土地', 'リアルエステート'
        ],
        'food': [
            '飲食', 'レストラン', '食品', '外食', 'カフェ', 'ベーカリー',
            'フード'
        ],
        'automotive': [
            '自動車', '車', 'カー', 'モビリティ', 'auto', 'オートモーティブ'
        ],
        'energy': [
            'エネルギー', '電力', '電気', '再生可能', '太陽光', '発電',
            '風力', 'パワー'
        ],
        'logistics': [
            '物流', '運送', '配送', '輸送', 'サプライチェーン',
            'ロジスティクス', 'デリバリー'
        ],
        'consulting': [
            'コンサルティング', 'コンサル', '経営', 'アドバイザリー',
            'コンサルタント'
        ],
        'media': [
            'メディア', '出版', '放送', 'テレビ', 'ラジオ', 'ad',
            '広告', 'パブリッシング'
        ],
        'telecommunications': [
            '通信', 'テレコム', 'モバイル', 'isp', 'キャリア'
        ],
        'other_services': [
            '結婚', 'ウェディング', '葬祭', '冠婚葬祭', 'サービス',
            '介護', '保育', 'ブライダル', '婚活'
        ],
    }
    
    # Check each category
    for category, keywords in industry_map.items():
        for keyword in keywords:
            if keyword in jp_lower:
                logger.debug(f"Mapped '{jp}' → {category}")
                return category
    
    # No match - return original if it's not placeholder
    if jp not in ['なし', 'not found', 'unknown', '']:
        logger.debug(f"No category match for '{jp}', returning original")
        return jp
    
    return None


def clean_value(val: str | None) -> str | None:
    """
    Clean extracted value.
    
    Args:
        val: Value to clean
    
    Returns:
        Cleaned value or None
    """
    if not val:
        return None
    
    # Strip whitespace
    val = val.strip()
    
    # Remove Japanese brackets
    val = val.strip('「」').strip()
    val = val.strip('【】').strip()
    
    # Remove angle brackets
    val = val.strip('<>').strip()
    
    # Remove markdown code blocks
    val = val.strip('```').strip()
    
    # Remove common noise patterns
    val = re.sub(r'^\[.+?\]\s*', '', val)  # Remove [labels]
    val = re.sub(r'\s*\(.+?\)\s*$', '', val)  # Remove trailing (notes)
    val = re.sub(r'\s*（.+?）\s*$', '', val)  # Remove trailing （notes）
    
    # Final cleanup
    val = val.strip()
    
    # Normalize legal entity: replace old '有限会社' with '株式会社'
    val = val.replace('有限会社', '株式会社')
    
    # Reject empty or placeholder values
    placeholder_values = [
        'なし', 'n/a', 'na', 'none', 'not found', 'unknown',
        'not provided', 'tbd', '-', '--', '---', ''
    ]
    
    if val.lower() in placeholder_values:
        return None
    
    return val if val else None