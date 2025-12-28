"""
FINAL FIX: AI Company Name Extraction
- Auto-completes missing legal entity by searching HTML
- Validates and fixes AI responses that are missing legal entities
- Returns the value even if legal entity can't be found (with lower confidence)
"""

import re
import logging
from typing import Dict, Optional, List
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class ImprovedAICompanyExtractor:
    """Final AI extractor with auto-completion of missing legal entities."""
    
    LEGAL_ENTITIES = [
        '株式会社', '有限会社', '合同会社', '合資会社', '合名会社',
        '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
        '特定非営利活動法人', '学校法人', '医療法人'
    ]
    
    def __init__(self, ai_extractor):
        self.ai_extractor = ai_extractor
    
    def extract_company_name(
        self,
        url: str,
        html_content: str,
        rule_based_result: Optional[Dict] = None
    ) -> Dict:
        """Extract company name with AI, auto-completing missing legal entities."""
        
        # Check if rule-based is sufficient
        if rule_based_result:
            rb_value = rule_based_result.get('company_name')
            rb_confidence = rule_based_result.get('company_name_confidence', 0.0)
            needs_verification = rule_based_result.get('needs_ai_verification', False)
            
            if rb_value and rb_confidence >= 0.85 and not needs_verification:
                # Normalize legal entity in rule-based result as well
                rb_value = rb_value.replace('有限会社', '株式会社')
                logger.info(f"✅ Rule-based sufficient: {rb_value}")
                return {
                    'value': rb_value,
                    'confidence': rb_confidence,
                    'source': rule_based_result.get('company_name_source', 'rule_based'),
                    'method': rule_based_result.get('company_name_method'),
                    'used_ai': False
                }
        
        # Call AI
        logger.info("🤖 Calling AI with improved prompt...")
        ai_response = self._call_ai_with_autocomplete(url, html_content, rule_based_result)
        
        if ai_response and ai_response.get('value'):
            logger.info(f"✅ AI completed: {ai_response['value']}")
            return ai_response
        
        # AI failed - check if we should use rule-based fallback
        if rule_based_result and rule_based_result.get('company_name'):
            rb_val = rule_based_result['company_name']
            
            # Check if rule-based already has legal entity
            has_legal_entity = any(entity in rb_val for entity in self.LEGAL_ENTITIES)
            
            if has_legal_entity:
                # Rule-based has legal entity, use it
                rb_val = rb_val.replace('有限会社', '株式会社')
                logger.info(f"↩️ Fallback to rule-based (has legal entity): {rb_val}")
                return {
                    'value': rb_val,
                    'confidence': rule_based_result.get('company_name_confidence', 0.5),
                    'source': rule_based_result.get('company_name_source', 'rule_based'),
                    'method': rule_based_result.get('company_name_method'),
                    'used_ai': False
                }
            else:
                # Rule-based doesn't have legal entity either - use it with lower confidence
                logger.warning(f"⚠️ Using rule-based without legal entity: {rb_val}")
                return {
                    'value': rb_val,
                    'confidence': min(rule_based_result.get('company_name_confidence', 0.5), 0.7),
                    'source': rule_based_result.get('company_name_source', 'rule_based'),
                    'method': rule_based_result.get('company_name_method'),
                    'used_ai': False
                }
        
        # Complete failure - return None
        logger.error("❌ Could not extract company name")
        return {
            'value': None,
            'confidence': 0.0,
            'source': 'not_found',
            'method': None,
            'used_ai': True  # Tried AI but failed
        }
    
    def _call_ai_with_autocomplete(
        self,
        url: str,
        html_content: str,
        rule_based_result: Optional[Dict] = None
    ) -> Optional[Dict]:
        """Call AI and auto-complete missing legal entity if needed."""
        try:
            # Prepare HTML
            prepared_html = self._prepare_focused_html(html_content)
            
            # Build prompt
            messages = self._build_improved_prompt(url, prepared_html, rule_based_result)
            
            # Call API
            response = self.ai_extractor.client.chat.completions.create(
                model=self.ai_extractor.config.model,
                messages=messages,
                temperature=0,
                max_tokens=300,
                timeout=self.ai_extractor.config.timeout
            )
            
            response_text = response.choices[0].message.content.strip()
            logger.debug(f"AI raw response: {response_text}")
            
            # Parse response
            parsed = self._parse_ai_response(response_text)
            
            if not parsed or not parsed.get('value'):
                logger.warning("AI returned no value")
                return None
            
            ai_value = parsed['value']
            
            # CHECK: Does it have legal entity?
            has_entity = any(entity in ai_value for entity in self.LEGAL_ENTITIES)
            
            if has_entity:
                # Already complete
                logger.info(f"AI result complete: {ai_value}")
                return parsed
            
            # MISSING LEGAL ENTITY - TRY TO AUTO-COMPLETE IT
            logger.warning(f"⚠️ AI missing legal entity: {ai_value}")
            logger.info("🔧 Attempting auto-complete...")
            
            completed = self._auto_complete_legal_entity(ai_value, html_content)
            
            if completed:
                logger.info(f"✅ Auto-completed: {completed}")
                parsed['value'] = completed
                parsed['confidence'] = min(parsed.get('confidence', 0.8), 0.85)
                return parsed
            else:
                # FIXED: Return original value even if can't auto-complete
                logger.warning(f"⚠️ Could not auto-complete, returning original: {ai_value}")
                parsed['confidence'] = min(parsed.get('confidence', 0.8), 0.70)  # Lower confidence
                return parsed
        
        except Exception as e:
            logger.error(f"AI call failed: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _auto_complete_legal_entity(self, company_name: str, html_content: str) -> Optional[str]:
        """
        Auto-complete missing legal entity by searching HTML.
        
        Strategy:
        1. Search HTML for legal entity + company name pattern
        2. Analyze frequency of legal entities in HTML
        3. Search for company name with entity in nearby context
        4. Return None if cannot determine (caller will return original value)
        """
        
        # STRATEGY 1: Search HTML for legal entity + this name
        logger.info(f"Strategy 1: Searching HTML for legal entity + '{company_name}'")
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            text = soup.get_text()
            
            # Try each legal entity
            for entity in self.LEGAL_ENTITIES:
                # Pattern: entity + name
                pattern1 = re.compile(re.escape(entity) + r'\s*' + re.escape(company_name), re.IGNORECASE)
                match1 = pattern1.search(text)
                if match1:
                    result = entity + company_name
                    # Normalize old entity to new
                    result = result.replace('有限会社', '株式会社')
                    logger.info(f"  ✅ Found in HTML: {result}")
                    return result
                
                # Pattern: name + entity
                pattern2 = re.compile(re.escape(company_name) + r'\s*' + re.escape(entity), re.IGNORECASE)
                match2 = pattern2.search(text)
                if match2:
                    result = company_name + entity
                    # Normalize old entity to new
                    result = result.replace('有限会社', '株式会社')
                    logger.info(f"  ✅ Found in HTML: {result}")
                    return result
        except Exception as e:
            logger.error(f"Error searching HTML: {e}")
        
        # STRATEGY 2: Count occurrences of each legal entity in HTML
        logger.info("Strategy 2: Analyzing legal entity frequency in HTML...")
        
        try:
            entity_counts = {}
            for entity in self.LEGAL_ENTITIES:
                count = text.lower().count(entity)
                if count > 0:
                    entity_counts[entity] = count
            
            if entity_counts:
                # Get most common entity
                most_common_entity = max(entity_counts, key=entity_counts.get)
                most_common_count = entity_counts[most_common_entity]
                
                logger.info(f"  Legal entity counts: {entity_counts}")
                logger.info(f"  Most common: {most_common_entity} ({most_common_count} occurrences)")
                
                # Only use if it appears at least 2 times (not a fluke)
                if most_common_count >= 2:
                    result = most_common_entity + company_name
                    # Normalize
                    result = result.replace('有限会社', '株式会社')
                    logger.info(f"  ✅ Using most frequent entity: {result}")
                    return result
                else:
                    logger.warning(f"  ⚠️ Entity only appears {most_common_count} time(s) - not confident")
            else:
                logger.warning("  ⚠️ No legal entities found in HTML")
        
        except Exception as e:
            logger.error(f"Error analyzing entity frequency: {e}")
        
        # STRATEGY 3: Check if company name already appears with an entity elsewhere
        logger.info("Strategy 3: Broad search for company name with any entity...")
        
        try:
            # Look for company name with any entity nearby (within 50 chars)
            name_lower = company_name.lower()
            text_lower = text.lower()
            
            # Find all positions where company name appears
            pos = 0
            while True:
                pos = text_lower.find(name_lower, pos)
                if pos == -1:
                    break
                
                # Check 50 chars before and after
                context_start = max(0, pos - 50)
                context_end = min(len(text), pos + len(company_name) + 50)
                context = text[context_start:context_end]
                
                # Check if any entity appears in context
                for entity in self.LEGAL_ENTITIES:
                    if entity in context:
                        result = entity + company_name
                        # Normalize
                        result = result.replace('有限会社', '株式会社')
                        logger.info(f"  ✅ Found in context: {result}")
                        return result
                
                pos += 1
        
        except Exception as e:
            logger.error(f"Error in broad search: {e}")
        
        # Could not determine legal entity - return None
        # Caller will return the original value with lower confidence
        logger.warning(f"⚠️ Could not auto-complete legal entity for: {company_name}")
        return None

    def _build_improved_prompt(
        self,
        url: str,
        html_content: str,
        rule_based_result: Optional[Dict] = None
    ) -> list:
        """Build improved prompt."""
        
        system_prompt = """You are a Japanese company name extraction specialist.

CRITICAL RULES:

1. **ALWAYS include the legal entity** (株式会社, 有限会社, etc.)
   WRONG: "アイクスエージェンシー"
   CORRECT: "株式会社アイクスエージェンシー"

2. Legal entity can be at START or END:
   - "株式会社アイクスエージェンシー" ✓
   - "アイクスエージェンシー株式会社" ✓

3. Remove trailing garbage:
   - "株式会社フェアレン All Rights Reserved" → "株式会社フェアレン"

4. Keep under 30 characters

5. **If you can't find a legal entity, return the name anyway**
   - Better to return partial name than nothing
   - We will try to complete it automatically

RESPONSE FORMAT:
company_name: [full name, preferably WITH legal entity]
confidence: [0.0-1.0]
source: [location]
"""
        
        rb_hint = ""
        if rule_based_result:
            rb_value = rule_based_result.get('company_name')
            if rb_value:
                rb_hint = f"\nRule-based found: '{rb_value}'\n(Verify this)"
        
        user_prompt = f"""Extract the company name (preferably with legal entity).

URL: {url}
{rb_hint}

HTML Content:
{html_content}

Extract the company name:
"""
        
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    
    def _prepare_focused_html(self, html_content: str) -> str:
        """Prepare focused HTML."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        for tag in soup(['script', 'style', 'noscript']):
            tag.decompose()
        
        parts = []
        
        # Title
        title = soup.find('title')
        if title:
            parts.append(f"[TITLE]\n{title.get_text().strip()}\n")
        
        # Meta
        og_site = soup.find('meta', property='og:site_name')
        if og_site:
            parts.append(f"[OG:SITE_NAME]\n{og_site.get('content', '')}\n")
        
        # Footer (COPYRIGHT CRITICAL)
        footer = soup.find('footer')
        if footer:
            parts.append(f"[FOOTER]\n{footer.get_text()[:400]}\n")
        
        # Header
        header = soup.find('header')
        if header:
            parts.append(f"[HEADER]\n{header.get_text()[:400]}\n")
        
        # H1 tags
        for i, h1 in enumerate(soup.find_all('h1')[:3], 1):
            parts.append(f"[H1-{i}]\n{h1.get_text().strip()}\n")
        
        # Body text (first 500 chars - might contain company info)
        body = soup.find('body')
        if body:
            body_text = body.get_text()[:500]
            parts.append(f"[BODY_EXCERPT]\n{body_text}\n")
        
        return "\n".join(parts)
    
    def _parse_ai_response(self, response_text: str) -> Optional[Dict]:
        """Parse AI response."""
        try:
            lines = response_text.strip().split('\n')
            result = {
                'value': None,
                'confidence': 0.0,
                'source': 'ai',
                'used_ai': True
            }
            
            for line in lines:
                line = line.strip()
                
                if line.startswith('company_name:'):
                    name = line.replace('company_name:', '').strip()
                    if name and name.lower() != 'not_found':
                        cleaned = self._clean_ai_result(name)
                        if cleaned:
                            result['value'] = cleaned
                
                elif line.startswith('confidence:'):
                    try:
                        conf = float(line.replace('confidence:', '').strip())
                        result['confidence'] = max(0.0, min(1.0, conf))
                    except:
                        result['confidence'] = 0.8 if result['value'] else 0.0
                
                elif line.startswith('source:'):
                    result['source'] = line.replace('source:', '').strip()
            
            return result if result['value'] else None
        
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None
    
    def _clean_ai_result(self, name: str) -> Optional[str]:
        """Clean AI result."""
        if not name:
            return None
        
        name = name.strip('"\'「」『』')
        
        # Remove trailing garbage
        patterns = [
            r'\s+All Rights Reserved.*$',
            r'\s+©.*$',
            r'\s+Copyright.*$',
            r'\s*[|｜].*$',
        ]
        for pattern in patterns:
            name = re.sub(pattern, '', name, flags=re.I)
        
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Normalize legal entity: replace old '有限会社' with '株式会社'
        name = name.replace('有限会社', '株式会社')
        
        if len(name) > 30 or len(name) < 2:
            return None
        
        return name