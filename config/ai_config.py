"""
AI Extraction Configuration
Manages API keys, model settings, and extraction parameters
UPDATED: Fixed deprecated Groq model
"""

import os
from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class AIProviderConfig:
    """Configuration for an AI provider."""
    name: str
    api_key: str
    model: str
    base_url: Optional[str] = None
    max_tokens: int = 2000
    temperature: float = 0.1
    timeout: int = 30


class AIConfig:
    """Central configuration for AI-powered extraction."""
    
    # ==================== API PROVIDERS ====================
    
    PROVIDERS = {
        'groq': {
            # UPDATED: Use current Groq model (as of Dec 2024)
            'model': 'llama-3.1-8b-instant',  # FIXED: Updated from deprecated model
            'base_url': 'https://api.groq.com/openai/v1',
            'max_tokens': 2000,
            'temperature': 0.1,
            'timeout': 30,
            'rate_limit': 30,  # requests per minute
            'rate_limit_delay': 2.0,  # seconds between requests
        },
        'openai': {
            'model': 'gpt-4o-mini',  # Cheap and fast
            'base_url': None,  # Uses default
            'max_tokens': 2000,
            'temperature': 0.1,
            'timeout': 30,
            'rate_limit': 500,  # requests per minute
            'rate_limit_delay': 0.2,  # seconds between requests
        }
    }
    
    # ==================== CONFIDENCE THRESHOLDS ====================
    
    # When rule-based confidence is below these thresholds, trigger AI
    CONFIDENCE_THRESHOLDS = {
        'company_name': 0.6,  # Use AI if company name confidence < 0.7
        'email': 0.5,         # Use AI if no email found or low confidence
        'industry': 0.6,      # Use AI if industry confidence < 0.6
    }
    
    # ==================== EXTRACTION SETTINGS ====================
    
    # Maximum HTML content length to send to AI (in characters)
    MAX_HTML_LENGTH = 8000
    
    # Whether to include metadata in AI prompts
    INCLUDE_METADATA = True
    
    # Whether to include existing rule-based results in prompt
    INCLUDE_EXISTING_RESULTS = True
    
    # JSON schema enforcement
    ENFORCE_JSON_SCHEMA = True
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0  # seconds
    EXPONENTIAL_BACKOFF = True
    
    # ==================== COST OPTIMIZATION ====================
    
    # Cache AI results (to avoid re-extracting same URLs)
    ENABLE_CACHING = True
    CACHE_TTL = 86400  # 24 hours in seconds
    
    # Batch multiple extraction tasks in single API call
    BATCH_EXTRACTIONS = True
    
    @classmethod
    def get_provider_config(cls, provider: str = None) -> AIProviderConfig:
        """
        Get provider configuration.
        
        Args:
            provider: Provider name ('groq' or 'openai'). 
                     If None, uses AI_PROVIDER env var or defaults to 'groq'
        
        Returns:
            AIProviderConfig instance
        """
        if provider is None:
            provider = os.getenv('AI_PROVIDER', 'groq').lower()
        
        if provider not in cls.PROVIDERS:
            raise ValueError(f"Unknown provider: {provider}. Must be 'groq' or 'openai'")
        
        config = cls.PROVIDERS[provider]
        
        # Get API key from environment
        api_key_env = f"{provider.upper()}_API_KEY"
        api_key = os.getenv(api_key_env)
        
        if not api_key:
            raise ValueError(
                f"API key not found. Set {api_key_env} environment variable.\n"
                f"Example: export {api_key_env}=your_key_here"
            )
        
        return AIProviderConfig(
            name=provider,
            api_key=api_key,
            model=config['model'],
            base_url=config.get('base_url'),
            max_tokens=config['max_tokens'],
            temperature=config['temperature'],
            timeout=config['timeout']
        )
    
    @classmethod
    def get_rate_limit_delay(cls, provider: str) -> float:
        """Get rate limit delay for provider."""
        config = cls.PROVIDERS.get(provider, {})
        return config.get('rate_limit_delay', 1.0)
    
    @classmethod
    def get_confidence_threshold(cls, field: str) -> float:
        """Get confidence threshold for a field."""
        return cls.CONFIDENCE_THRESHOLDS.get(field, 0.7)
    
    @classmethod
    def should_use_ai(cls, field: str, confidence: float, value: any = None) -> bool:
        """
        Determine if AI should be used for extraction.
        
        Args:
            field: Field name ('company_name', 'email', 'industry')
            confidence: Confidence score from rule-based extraction
            value: Extracted value (None means not found)
        
        Returns:
            True if AI should be used
        """
        # If no value found, always use AI
        if value is None or value == '':
            return True
        
        # If confidence below threshold, use AI
        threshold = cls.get_confidence_threshold(field)
        if confidence < threshold:
            return True
        
        return False


# ==================== HELPER FUNCTIONS ====================

def get_ai_provider() -> str:
    """Get current AI provider from environment."""
    return os.getenv('AI_PROVIDER', 'groq').lower()


def set_ai_provider(provider: str):
    """Set AI provider."""
    if provider not in AIConfig.PROVIDERS:
        raise ValueError(f"Invalid provider: {provider}")
    os.environ['AI_PROVIDER'] = provider


def get_api_key(provider: str = None) -> Optional[str]:
    """Get API key for provider."""
    if provider is None:
        provider = get_ai_provider()
    return os.getenv(f"{provider.upper()}_API_KEY")


def validate_configuration(provider: str = None) -> bool:
    """
    Validate AI configuration.
    
    Returns:
        True if configuration is valid
    """
    try:
        config = AIConfig.get_provider_config(provider)
        return bool(config.api_key)
    except Exception:
        return False