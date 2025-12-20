"""
Improved Prompt Templates for AI Extraction
Enhanced instructions for better accuracy
"""

import json
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup

LEGAL_ENTITY_RE = re.compile(
    r"(株式会社|有限会社|合同会社)[\s　]*[^\n<]{2,60}"
)



class PromptTemplates:
    """Improved prompt templates for AI extraction."""
    
    SYSTEM_PROMPT = """You are a data extraction engine.

                        You MUST return ONE complete JSON object and NOTHING ELSE.

                        You MUST NOT:
                        - Explain your reasoning
                        - Describe the website
                        - Include any text before or after the JSON
                        - Include headings, notes, or comments

                        If you cannot find a value, use null.
                        """

    @staticmethod
    def build_messages(
        url: str,
        html_content: str,
        fields_to_extract: List[str],
        existing_results: Optional[Dict] = None
    ) -> List[Dict]:
        """Build messages for AI extraction with improved prompts."""
        
        # Clean and truncate HTML for context limits
        cleaned_html = PromptTemplates._clean_html(html_content)
        
        # Build user prompt
        user_prompt = f"""Extract the following information from this Japanese website:
URL: {url}

Fields to extract: {', '.join(fields_to_extract)}

HTML CONTENT:
{cleaned_html[:15000]}  # Limit to 15k chars to fit in context

"""
        
        # Add existing results context if provided
        if existing_results:
            user_prompt += "\n\nNOTE: Rule-based extraction found these results (use them to verify your extraction):\n"
            for field, data in existing_results.items():
                if isinstance(data, dict):
                    value = data.get('company_name') or data.get('email') or data.get('industry')
                    if value:
                        user_prompt += f"- {field}: {value}\n"
                elif data:
                    user_prompt += f"- {field}: {data}\n"
        
        # Add specific instructions per field
        instructions = []
        
        if 'company_name' in fields_to_extract:
            instructions.append("""
        COMPANY NAME EXTRACTION:
        - Look for: <title>, <h1>, og:site_name meta tag, copyright footer, header logo alt text
        - IMPORTANT: NOT ALL JAPANESE COMPANIES HAVE LEGAL ENTITIES (like 株式会社, 有限会社)
        - Many businesses use just their brand name without legal entity
        - Examples of CORRECT extraction (with legal entity):
        * "株式会社エムシー・ケーおよびそのグループ" (include the legal entity)
        * "コナン販売株式会社" (not just "コナン販売")
        - Examples of CORRECT extraction (WITHOUT legal entity):
        * "セルビスメンバーズ" ✓ (brand name only, no legal entity required)
        * "アマゾンジャパン" ✓ (brand name is sufficient)
        * "ソフトバンク" ✓ (no legal entity prefix needed)
        - Rules:
        1. Look in <h1> tags first (usually the main company name)
        2. Check the first prominent heading on the page
        3. Look in page title and meta tags
        4. If the name appears in header/footer repeatedly, it's likely correct
        5. Trim common suffixes: " | ", " - ", " | 公式サイト", " | Official"
        - Confidence scoring:
        * 1.0 if in <h1> tag or header logo
        * 0.95 if in og:site_name or page title
        * 0.85 if in copyright footer with legal entity
        * 0.75 if found in multiple places but no legal entity
        * 0.6 if inferred from header/body text
        * 0.0 if not found or too generic (like "ホーム" or "トップページ")
        - Return EXACTLY what appears on the site (don't add legal entities if not present)
        """)
        if 'email' in fields_to_extract:
            instructions.append("""
EMAIL EXTRACTION:
- Search ALL of these locations:
  1. mailto: links (<a href="mailto:...">)
  2. Visible text with @ symbol
  3. Contact sections and footers
  4. Obfuscated formats: "info [at] company.com", "info＠company.co.jp"
  5. JavaScript-assembled emails
  6. Forms with pre-filled email examples
- Return the MOST OFFICIAL looking email (typically @company-domain.co.jp or @company-domain.com)
- Avoid noreply@, test@, example@
- Confidence: 1.0 if mailto link, 0.9 if in contact section, 0.7 if in general text""")
        
        if 'industry' in fields_to_extract:
            instructions.append("""
INDUSTRY CLASSIFICATION:
- Map to ONE of these categories based on the company's PRIMARY business:
  * technology: IT, software, systems, SaaS, tech consulting
  * finance: Banking, insurance, securities, investment
  * retail: Stores, e-commerce, shops
  * healthcare: Hospitals, clinics, medical, pharmaceutical
  * education: Schools, training, learning, tutoring
  * manufacturing: Production, factories, makers
  * construction: Construction, architecture, civil engineering
  * real_estate: Real estate, property, housing
  * food: Restaurants, food service, food manufacturing
  * automotive: Cars, auto parts, auto sales
  * energy: Power, electricity, renewable energy
  * logistics: Transportation, shipping, warehouses
  * consulting: Management consulting, business advisory (コンサルティング)
  * media: Publishing, broadcasting, advertising
  * telecommunications: Telecom, mobile, ISPs
  * other_services: Wedding services, funeral services, personal services
  * other: Everything else
- Look for keywords in: meta description, title, h1, about section
- Confidence: 0.9 if explicitly stated, 0.7 if inferred from content""")
        
        user_prompt += "\n".join(instructions)
        
        # Add output format
        user_prompt += """

OUTPUT FORMAT (STRICT JSON, NO MARKDOWN):
{"""
        
        if 'company_name' in fields_to_extract:
            user_prompt += """
  "company_name": {
    "value": "full company name with legal entity",
    "confidence": 0.0-1.0,
    "source": "where you found it (e.g., 'copyright footer', 'title tag')"
  },"""
        
        if 'email' in fields_to_extract:
            user_prompt += """
  "email": {
    "value": "email@company.com or null",
    "confidence": 0.0-1.0,
    "source": "where you found it"
  },"""
        
        if 'industry' in fields_to_extract:
            user_prompt += """
  "industry": {
    "value": "category name or null",
    "confidence": 0.0-1.0,
    "source": "reasoning"
  }"""
        
        user_prompt += """
}

Return ONLY the JSON object.
The response MUST start with '{' and end with '}'."""
        
        return [
            {"role": "system", "content": PromptTemplates.SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]
    
    GROQ_TEXT_SYSTEM = """あなたは日本企業のWebサイトからメールアドレス、会社名、業界を抽出するAIアシスタントです。

重要なルール：
1. 出力は指定された3行の形式のみ
2. 説明や推論は含めない
3. 見つからない情報は「なし」と記入
4. メールアドレスは有効な形式であることを確認
5. 会社名は法人種別がある場合は含める（例：「株式会社〇〇」）
6. 業界は日本語で記入

出力形式：
会社名：<値>
メールアドレス：<値>
業界：<値>
"""
    
    @staticmethod
    def _clean_html(html: str) -> str:
        """Clean HTML to reduce token count while preserving important content."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove scripts, styles, comments
            for tag in soup(['script', 'style', 'noscript', 'iframe']):
                tag.decompose()
            
            # Get text with some structure preserved
            text_parts = []
            
            # Title
            title = soup.find('title')
            if title:
                text_parts.append(f"<title>{title.get_text()}</title>")
            
            # Meta tags
            for meta in soup.find_all('meta', content=True):
                name = meta.get('name') or meta.get('property', '')
                if any(x in name.lower() for x in ['description', 'title', 'site', 'og:']):
                    text_parts.append(f"<meta {name}=\"{meta.get('content')}\">")
            
            # Headers
            for i in range(1, 4):
                for h in soup.find_all(f'h{i}'):
                    text_parts.append(f"<h{i}>{h.get_text(strip=True)}</h{i}>")
            
            # Footer (often has company info)
            footer = soup.find('footer') or soup.find(class_=re.compile('footer', re.I))
            if footer:
                text_parts.append(f"<footer>{footer.get_text(separator=' ', strip=True)[:500]}</footer>")
            
            # Header (often has company name)
            header = soup.find('header') or soup.find(class_=re.compile('header', re.I))
            if header:
                text_parts.append(f"<header>{header.get_text(separator=' ', strip=True)[:500]}</header>")
            
            # Body text (limited)
            body_text = soup.get_text(separator=' ', strip=True)
            text_parts.append(f"<body>{body_text[:1500]}</body>")
            
            return '\n'.join(text_parts)
        except:
            # Fallback to simple cleaning
            text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.I)
            text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.I)
            text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'\s+', ' ', text)
            return text[:15000]
    

    """
    FIXED & OPTIMIZED: Groq prompt for Japanese company info extraction
    - Better email recall (header + footer)
    - Better industry inference
    - Strict, parse-safe output
    """

    @staticmethod
    def build_groq_text_prompt(html_content: str) -> list:
        """
        Build Groq text-based extraction prompt.
        
        Returns plain text response that's easier to parse than JSON.
        Format:
            会社名：<company name>
            メールアドレス：<email>
            業界：<industry>
        
        Args:
            html_content: HTML content to analyze
        
        Returns:
            List of message dicts for API call
        """
        cleaned_html = PromptTemplates._clean_html(html_content)

        user_prompt = f"""あなたは日本のWebサイトからデータを抽出する専門家です。

    以下のWebサイトの内容から、正確に以下の3つの情報を抽出してください：

    1. **会社名** - 企業の正式な名称（法人種別「株式会社」「有限会社」などがあれば含める）
    - サイトのタイトル、h1タグ、ロゴ、フッターから探す
    - 法人種別がない場合は会社のブランド名だけでOK
    - 例: "セルビスメンバーズ" / "株式会社エムシー・ケー" / "ソフトバンク"

    2. **メールアドレス** - 企業の公式メールアドレス（info@, contact@など）
    - mailto:リンク、フッター、お問い合わせページから探す
    - 見つからない場合は「なし」と記入

    3. **業界** - 企業の業界（日本語で記入）
    - 製造業、小売業、金融業、医療、教育、冠婚葬祭業など
    - メタディスクリプション、h1タグ、テキストから推測

    **出力形式（必ずこの形式で返してください）:**

    会社名：<会社名 or なし>
    メールアドレス：<メールアドレス or なし>
    業界：<業界名 or なし>

    **重要な注意事項:**
    - 出力は上記3行のみ。説明や余計なテキストは入れない
    - 値が見つからない場合は「なし」と書く
    - メールアドレスは valid @ domain.jp 形式であることを確認

    ---

    以下がWebサイトの内容です：

    {cleaned_html[:4000]}

    ---

    上記の形式で抽出結果を出力してください：
    """

        return [
            {"role": "system", "content": PromptTemplates.GROQ_TEXT_SYSTEM},
            {"role": "user", "content": user_prompt}
        ]
    
    @staticmethod
    def parse_response(response_text: str) -> Optional[Dict]:
        """Parse AI response with improved error handling."""
        if not response_text:
            return None
        
        try:
            # Remove markdown code blocks if present
            cleaned = response_text.strip()

            # Always extract first valid JSON object
            match = re.search(r'\{.*\}', cleaned, re.DOTALL)
            if not match:
                return None

            cleaned = match.group(0)
            if cleaned.count('{') != cleaned.count('}'):
                return None
            data = json.loads(cleaned)

            
            # Parse JSON
            # data = json.loads(cleaned)
            
            # Normalize structure (handle both nested and flat formats)
            normalized = {}
            
            for field in ['company_name', 'email', 'industry']:
                if field in data:
                    field_data = data[field]
                    if isinstance(field_data, dict):
                        normalized[field] = {
                            'value': field_data.get('value'),
                            'confidence': float(field_data.get('confidence', 0.5)),
                            'source': field_data.get('source', 'ai')
                        }
                    elif field_data:  # Direct value
                        normalized[field] = {
                            'value': field_data,
                            'confidence': 0.8,
                            'source': 'ai'
                        }
                    else:
                        normalized[field] = {
                            'value': None,
                            'confidence': 0.0,
                            'source': 'not_found'
                        }
            
            return normalized
            
        except json.JSONDecodeError as e:
            print(f"JSON parse error: {e}")
            print(f"Response text: {response_text[:500]}")
            return None
        except Exception as e:
            print(f"Parse error: {e}")
            return None
    
    @staticmethod
    def enforce_legal_entity(company_name: str | None, html: str) -> str | None:
        match = LEGAL_ENTITY_RE.search(html)
        if match:
            return match.group(0).replace(" ", "")
        return company_name
