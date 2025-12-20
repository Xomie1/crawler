"""
AI-Powered Extraction Module
Handles API calls to OpenAI/Groq for intelligent data extraction
"""

import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from openai import OpenAI
from config.ai_config import AIConfig, AIProviderConfig
from utils.prompt_templates import PromptTemplates
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)



class AIExtractor:
    """AI-powered extractor using OpenAI/Groq APIs."""
    
    def __init__(self, provider: str = None):
        """
        Initialize AI extractor.
        
        Args:
            provider: AI provider ('groq' or 'openai'). 
                     If None, uses AI_PROVIDER env var or defaults to 'groq'
        """
        self.config = AIConfig.get_provider_config(provider)
        self.rate_limit_delay = AIConfig.get_rate_limit_delay(self.config.name)
        self.last_request_time = 0
        
        # Initialize OpenAI client (works for both OpenAI and Groq)
        if self.config.base_url:
            self.client = OpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url
            )
        else:
            self.client = OpenAI(api_key=self.config.api_key)
        
        logger.info(f"Initialized AI extractor with provider: {self.config.name}")
        logger.info(f"Model: {self.config.model}")
    
    def extract(
        self,
        url: str,
        html_content: str,
        fields: List[str] = None,
        existing_results: Optional[Dict] = None
    ) -> Dict:
        """
        Extract information using AI.
        
        Args:
            url: Website URL
            html_content: HTML content to analyze
            fields: List of fields to extract (default: all)
            existing_results: Existing rule-based results (optional)
        
        Returns:
            Dictionary with extracted data and metadata
        """

        if fields is None:
            fields = ['company_name', 'email', 'industry']

        logger.info(f"AI extraction for {url} (fields: {', '.join(fields)})")

        try:
            self._enforce_rate_limit()

            # ============================
            # 🟢 GROQ → TWO-PHASE PIPELINE
            # ============================
            if self.config.name == "groq":
                raw_text = self.extract_groq_text(html_content, url)

                from utils.groq_normalizer import normalize_groq_text
                parsed = normalize_groq_text(raw_text, html_content)

                self._log_extraction_results(parsed)

                return {
                    "success": True,
                    "data": parsed,
                    "provider": "groq",
                    "model": self.config.model,
                    "raw_response": raw_text
                }

            # ============================
            # 🔵 OPENAI → STRICT JSON
            # ============================
            cleaned_content = self._clean_html(html_content)

            messages = PromptTemplates.build_messages(
                url=url,
                html_content=cleaned_content,
                fields_to_extract=fields,
                existing_results=existing_results
            )

            response_text = self._call_api_with_retry(messages)

            if not response_text:
                return self._create_error_result("Empty API response")

            parsed = PromptTemplates.parse_response(response_text)

            if not parsed:
                return self._create_error_result("Invalid JSON response")

            self._log_extraction_results(parsed)

            return {
                'success': True,
                'data': parsed,
                'provider': self.config.name,
                'model': self.config.model,
                'raw_response': response_text
            }

        except Exception as e:
            logger.error(f"AI extraction failed for {url}: {e}")
            return self._create_error_result(str(e))

    
    def extract_company_name(
        self,
        url: str,
        html_content: str,
        existing_result: Optional[Dict] = None
    ) -> Dict:
        """Extract company name only."""
        return self.extract(
            url, html_content, 
            fields=['company_name'],
            existing_results={'company_name': existing_result} if existing_result else None
        )
    
    def extract_email(
        self,
        url: str,
        html_content: str,
        existing_result: Optional[Dict] = None
    ) -> Dict:
        """Extract email only."""
        return self.extract(
            url, html_content,
            fields=['email'],
            existing_results={'email': existing_result} if existing_result else None
        )
    
    def extract_industry(
        self,
        url: str,
        html_content: str,
        existing_result: Optional[Dict] = None
    ) -> Dict:
        """Extract industry only."""
        return self.extract(
            url, html_content,
            fields=['industry'],
            existing_results={'industry': existing_result} if existing_result else None
        )
    
    def _call_api_with_retry(self, messages: List[Dict]) -> Optional[str]:
        """
        Call API with exponential backoff retry.
        
        Args:
            messages: Chat messages
        
        Returns:
            Response text or None if failed
        """
        max_retries = AIConfig.MAX_RETRIES
        retry_delay = AIConfig.RETRY_DELAY
        
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.config.model,
                    messages=messages,
                    max_tokens=self.config.max_tokens,
                    temperature=0,
                    timeout=self.config.timeout,
                    stop=["\n\n"]
                )
                
                return response.choices[0].message.content
                
            except Exception as e:
                logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    # Exponential backoff
                    if AIConfig.EXPONENTIAL_BACKOFF:
                        wait_time = retry_delay * (2 ** attempt)
                    else:
                        wait_time = retry_delay
                    
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached, giving up")
                    return None
        
        return None
    
    
    def _enforce_rate_limit(self):
        """Enforce rate limiting between API calls."""
        if self.rate_limit_delay > 0:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
    
    def _create_error_result(self, error_message: str) -> Dict:
        """Create error result dictionary."""
        return {
            'success': False,
            'data': {
                'company_name': {'value': None, 'confidence': 0.0, 'source': 'error'},
                'email': {'value': None, 'confidence': 0.0, 'source': 'error'},
                'industry': {'value': None, 'confidence': 0.0, 'source': 'error'}
            },
            'error': error_message,
            'provider': self.config.name,
            'model': self.config.model
        }
    
    def _clean_html(self, html: str) -> str:
        """Enhanced cleaning to ensure we fit in token limits."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove all non-textual or structural noise
        for element in soup(["script", "style", "svg", "header", "footer", "nav", "path", "link", "meta", "noscript"]):
            element.decompose()
        
        # Target the main content areas if they exist, otherwise use body
        content_area = soup.find('main') or soup.find('article') or soup.find('body')
        
        if content_area:
            text = content_area.get_text(separator=' ', strip=True)
        else:
            text = soup.get_text(separator=' ', strip=True)
            
        import re
        text = re.sub(r'\s+', ' ', text)

        # Reduce character limit slightly to leave more room for the output "completion" tokens
        return text[:40000]
    

    def extract_groq_text(self, html_content: str, url: str = None) -> str:
        """
        Extract information from HTML using Groq's text-based pipeline.
        
        Args:
            html_content: HTML content to analyze
            url: Website URL (optional, for context)
        
        Returns:
            Raw text response from Groq
        """
        messages = PromptTemplates.build_groq_text_prompt(html_content)

        logger.debug(f"Calling Groq API for URL: {url}")
        logger.debug(f"Message count: {len(messages)}")
        
        response = self.client.chat.completions.create(
            model=self.config.model,
            messages=messages,
            temperature=0,
            max_tokens=512,
            timeout=self.config.timeout
        )

        raw_response = response.choices[0].message.content.strip()
        
        # Log the raw response for debugging
        logger.info(f"Groq raw response for {url}:")
        logger.info(f"---START---\n{raw_response}\n---END---")
        
        return raw_response

    def _log_extraction_results(self, parsed: Dict):
        """Log extraction results for debugging."""
        for field in ['company_name', 'email', 'industry']:
            if field in parsed:
                data = parsed[field]
                value = data.get('value')
                confidence = data.get('confidence', 0)
                logger.info(
                    f"  {field}: {value or 'None'} "
                    f"(confidence: {confidence:.2f})"
                )


class AIExtractionCache:
    """Simple in-memory cache for AI extraction results."""
    
    def __init__(self, ttl: int = 86400):
        """
        Initialize cache.
        
        Args:
            ttl: Time to live in seconds (default: 24 hours)
        """
        self.cache: Dict[str, Tuple[Dict, float]] = {}
        self.ttl = ttl
    
    def get(self, url: str) -> Optional[Dict]:
        """Get cached result for URL."""
        if url in self.cache:
            result, timestamp = self.cache[url]
            
            # Check if expired
            if time.time() - timestamp < self.ttl:
                logger.debug(f"Cache hit for {url}")
                return result
            else:
                logger.debug(f"Cache expired for {url}")
                del self.cache[url]
        
        return None
    
    def set(self, url: str, result: Dict):
        """Cache result for URL."""
        self.cache[url] = (result, time.time())
        logger.debug(f"Cached result for {url}")
    
    def clear(self):
        """Clear all cached results."""
        self.cache.clear()
        logger.info("Cache cleared")
    
    def size(self) -> int:
        """Get cache size."""
        return len(self.cache)


# Global cache instance
_cache = AIExtractionCache() if AIConfig.ENABLE_CACHING else None


def get_cache() -> Optional[AIExtractionCache]:
    """Get global cache instance."""
    return _cache


def extract_with_cache(
    extractor: AIExtractor,
    url: str,
    html_content: str,
    fields: List[str] = None,
    existing_results: Optional[Dict] = None
) -> Dict:
    """
    Extract with caching.
    
    Args:
        extractor: AIExtractor instance
        url: Website URL
        html_content: HTML content
        fields: Fields to extract
        existing_results: Existing results
    
    Returns:
        Extraction result
    """
    cache = get_cache()
    
    # Try cache first
    if cache:
        cached = cache.get(url)
        if cached:
            logger.info(f"Using cached AI result for {url}")
            return cached
    
    # Extract fresh
    result = extractor.extract(url, html_content, fields, existing_results)
    
    # Cache result
    if cache and result.get('success'):
        cache.set(url, result)
    
    return result