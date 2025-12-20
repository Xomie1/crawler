"""
Hybrid Extraction Module - FIXED
Combines rule-based and AI extraction with intelligent fallback
"""

import logging
from typing import Dict, Optional, List
from config.ai_config import AIConfig
from crawler.ai.ai_extractor import AIExtractor, extract_with_cache

logger = logging.getLogger(__name__)


class HybridExtractor:
    """
    Hybrid extractor that uses rule-based methods first, 
    then falls back to AI when confidence is low.
    """
    
    def __init__(
        self,
        ai_provider: str = None,
        use_ai: bool = True,
        always_use_ai: bool = False,
        confidence_thresholds: Optional[Dict[str, float]] = None
    ):
        """
        Initialize hybrid extractor.
        
        Args:
            ai_provider: AI provider ('groq' or 'openai')
            use_ai: Whether to use AI at all
            always_use_ai: Always use AI, skip rule-based
            confidence_thresholds: Custom thresholds (optional)
        """
        self.use_ai = use_ai
        self.always_use_ai = always_use_ai
        self.confidence_thresholds = confidence_thresholds or AIConfig.CONFIDENCE_THRESHOLDS
        
        # Initialize AI extractor if needed
        if self.use_ai:
            try:
                self.ai_extractor = AIExtractor(provider=ai_provider)
                logger.info("AI extraction enabled")
            except Exception as e:
                logger.error(f"Failed to initialize AI extractor: {e}")
                self.use_ai = False
                self.ai_extractor = None
        else:
            self.ai_extractor = None
    
    def extract_company_name(
        self,
        url: str,
        html_content: str,
        rule_based_result: Optional[Dict] = None
    ) -> Dict:
        """
        Extract company name with hybrid approach.
        
        Args:
            url: Website URL
            html_content: HTML content
            rule_based_result: Result from rule-based extractor
        
        Returns:
            Dict with 'value', 'confidence', 'source', 'used_ai'
        """
        field = 'company_name'
        
        # If always use AI, skip rule-based
        if self.always_use_ai and self.use_ai:
            logger.info(f"Using AI-only mode for {field}")
            return self._extract_with_ai(url, html_content, field, None)
        
        # Use rule-based result if available
        if rule_based_result:
            value = rule_based_result.get('company_name')
            confidence = rule_based_result.get('company_name_confidence', 0.0)
            source = rule_based_result.get('company_name_source', 'rule_based')
            
            # Check if we should use AI
            should_use_ai = AIConfig.should_use_ai(field, confidence, value)
            
            if not should_use_ai:
                logger.info(
                    f"Rule-based {field} sufficient "
                    f"(confidence: {confidence:.2f} >= {self.confidence_thresholds[field]})"
                )
                return {
                    'value': value,
                    'confidence': confidence,
                    'source': source,
                    'used_ai': False
                }
            
            # Use AI to improve result
            if self.use_ai:
                logger.info(
                    f"Rule-based {field} confidence low "
                    f"({confidence:.2f} < {self.confidence_thresholds[field]}), using AI"
                )
                return self._extract_with_ai(url, html_content, field, rule_based_result)
            else:
                # AI not available, return rule-based result
                return {
                    'value': value,
                    'confidence': confidence,
                    'source': source,
                    'used_ai': False
                }
        
        # No rule-based result, use AI if available
        if self.use_ai:
            logger.info(f"No rule-based {field}, using AI")
            return self._extract_with_ai(url, html_content, field, None)
        else:
            # No AI available
            return {
                'value': None,
                'confidence': 0.0,
                'source': 'not_found',
                'used_ai': False
            }
    
    def extract_email(
        self,
        url: str,
        html_content: str,
        rule_based_result: Optional[Dict] = None
    ) -> Dict:
        """
        Extract email with hybrid approach.
        
        Args:
            url: Website URL
            html_content: HTML content
            rule_based_result: Result from rule-based extractor
        
        Returns:
            Dict with 'value', 'confidence', 'source', 'used_ai'
        """
        field = 'email'
        
        # If always use AI, skip rule-based
        if self.always_use_ai and self.use_ai:
            logger.info(f"Using AI-only mode for {field}")
            return self._extract_with_ai(url, html_content, field, None)
        
        # Use rule-based result if available
        if rule_based_result:
            value = rule_based_result.get('email')
            confidence = rule_based_result.get('confidence', 0.0)
            
            # Check if we should use AI
            should_use_ai = AIConfig.should_use_ai(field, confidence, value)
            
            if not should_use_ai:
                logger.info(
                    f"Rule-based {field} sufficient "
                    f"(confidence: {confidence:.2f} >= {self.confidence_thresholds.get(field, 0.5)})"
                )
                return {
                    'value': value,
                    'confidence': confidence,
                    'source': 'rule_based',
                    'used_ai': False
                }
            
            # Use AI to improve result
            if self.use_ai:
                logger.info(
                    f"Rule-based {field} confidence low or not found, using AI"
                )
                return self._extract_with_ai(url, html_content, field, {'email': value})
            else:
                return {
                    'value': value,
                    'confidence': confidence,
                    'source': 'rule_based',
                    'used_ai': False
                }
        
        # No rule-based result, use AI if available
        if self.use_ai:
            logger.info(f"No rule-based {field}, using AI")
            return self._extract_with_ai(url, html_content, field, None)
        else:
            return {
                'value': None,
                'confidence': 0.0,
                'source': 'not_found',
                'used_ai': False
            }
    
    def extract_industry(
        self,
        url: str,
        html_content: str,
        rule_based_result: Optional[str] = None
    ) -> Dict:
        """
        Extract industry with hybrid approach.
        
        Args:
            url: Website URL
            html_content: HTML content
            rule_based_result: Result from rule-based extractor (industry string)
        
        Returns:
            Dict with 'value', 'confidence', 'source', 'used_ai'
        """
        field = 'industry'
        
        # If always use AI, skip rule-based
        if self.always_use_ai and self.use_ai:
            logger.info(f"Using AI-only mode for {field}")
            return self._extract_with_ai(url, html_content, field, None)
        
        # Use rule-based result if available
        if rule_based_result:
            # Rule-based industry doesn't have confidence, assume medium confidence
            confidence = 0.65
            
            # Check if we should use AI
            should_use_ai = AIConfig.should_use_ai(field, confidence, rule_based_result)
            
            if not should_use_ai:
                logger.info(f"Rule-based {field} sufficient")
                return {
                    'value': rule_based_result,
                    'confidence': confidence,
                    'source': 'rule_based',
                    'used_ai': False
                }
            
            # Use AI to improve result
            if self.use_ai:
                logger.info(f"Rule-based {field} confidence low, using AI")
                return self._extract_with_ai(url, html_content, field, {'industry': rule_based_result})
            else:
                return {
                    'value': rule_based_result,
                    'confidence': confidence,
                    'source': 'rule_based',
                    'used_ai': False
                }
        
        # No rule-based result, use AI if available
        if self.use_ai:
            logger.info(f"No rule-based {field}, using AI")
            return self._extract_with_ai(url, html_content, field, None)
        else:
            return {
                'value': None,
                'confidence': 0.0,
                'source': 'not_found',
                'used_ai': False
            }
    
    def _extract_with_ai(
        self,
        url: str,
        html_content: str,
        field: str,
        existing_results: Optional[Dict] = None
    ) -> Dict:
        """
        Extract using AI with improved error handling.
        
        Args:
            url: Website URL
            html_content: HTML content
            field: Field to extract
            existing_results: Existing results to include in prompt
        
        Returns:
            Dict with 'value', 'confidence', 'source', 'used_ai'
        """
        try:
            # Call AI extractor
            result = extract_with_cache(
                self.ai_extractor,
                url,
                html_content,
                fields=[field],
                existing_results=existing_results
            )
            
            if result.get('success'):
                # FIXED: Handle the new response structure
                data = result.get('data', {})
                
                # The field might be directly in data or nested
                if field in data:
                    field_data = data[field]
                    
                    # Handle both dict and direct value
                    if isinstance(field_data, dict):
                        return {
                            'value': field_data.get('value'),
                            'confidence': field_data.get('confidence', 0.8),
                            'source': f"ai_{result.get('provider', 'unknown')}",
                            'used_ai': True
                        }
                    else:
                        # Direct value
                        return {
                            'value': field_data,
                            'confidence': 0.8,
                            'source': f"ai_{result.get('provider', 'unknown')}",
                            'used_ai': True
                        }
                else:
                    logger.error(f"Field '{field}' not found in AI response data")
                    return {
                        'value': None,
                        'confidence': 0.0,
                        'source': 'ai_error',
                        'used_ai': True,
                        'error': f'Field {field} not in response'
                    }
            else:
                logger.error(f"AI extraction failed: {result.get('error')}")
                return {
                    'value': None,
                    'confidence': 0.0,
                    'source': 'ai_error',
                    'used_ai': True,
                    'error': result.get('error')
                }
        except Exception as e:
            logger.error(f"AI extraction exception: {e}")
            import traceback
            traceback.print_exc()
            return {
                'value': None,
                'confidence': 0.0,
                'source': 'ai_error',
                'used_ai': True,
                'error': str(e)
            }