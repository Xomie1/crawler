# -*- coding: utf-8 -*-
"""
Email Configuration - Phase 2
Manages SendGrid settings, rate limits, and templates
"""

import os
from pathlib import Path
from typing import Optional

# ==================== LOAD .ENV FILE ====================
def load_dotenv():
    """Load .env file if exists (same as batch_crawler.py)."""
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")
        print("✓ Loaded .env file for email config")

load_dotenv()
# ========================================================


class EmailConfig:
    """Email sending configuration."""
    
    # SendGrid API
    SENDGRID_API_KEY: Optional[str] = os.getenv('SENDGRID_API_KEY')
    
    # Sender Information
    SENDER_EMAIL: str = os.getenv('SENDER_EMAIL', 'noreply@example.com')
    SENDER_NAME: str = os.getenv('SENDER_NAME', 'Your Company Sales Team')
    
    # Rate Limiting
    DAILY_EMAIL_LIMIT: int = int(os.getenv('DAILY_EMAIL_LIMIT', '100'))
    HOURLY_EMAIL_LIMIT: int = int(os.getenv('HOURLY_EMAIL_LIMIT', '10'))
    
    # SendGrid Template ID (optional - can use inline templates)
    SENDGRID_TEMPLATE_ID: Optional[str] = os.getenv('SENDGRID_TEMPLATE_ID')
    
    # Retry Configuration
    MAX_RETRIES: int = int(os.getenv('EMAIL_MAX_RETRIES', '3'))
    RETRY_DELAY: float = float(os.getenv('EMAIL_RETRY_DELAY', '5.0'))
    
    # Duplicate Prevention
    DUPLICATE_COOLDOWN_DAYS: int = int(os.getenv('DUPLICATE_COOLDOWN_DAYS', '30'))
    
    # Email Validation
    SKIP_INVALID_EMAILS: bool = os.getenv('SKIP_INVALID_EMAILS', 'true').lower() == 'true'
    
    # Blacklist
    BLACKLIST_DOMAINS: list = [
        'example.com',
        'test.com',
        'localhost',
    ]
    
    # Default Email Template (Japanese)
    DEFAULT_EMAIL_TEMPLATE = """
{company_name}様

お世話になっております。
{sender_name}と申します。

{message_body}

ご不明な点がございましたら、お気軽にお問い合わせください。

よろしくお願いいたします。

{sender_name}
{sender_email}
"""
    
    # Default Subject Template
    DEFAULT_SUBJECT_TEMPLATE = "{company_name}様へのご提案"
    
    @classmethod
    def validate(cls) -> tuple[bool, Optional[str]]:
        """
        Validate configuration.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not cls.SENDGRID_API_KEY:
            return False, "SENDGRID_API_KEY not set in environment variables"
        
        if not cls.SENDER_EMAIL or '@' not in cls.SENDER_EMAIL:
            return False, "Invalid SENDER_EMAIL in environment variables"
        
        if cls.DAILY_EMAIL_LIMIT <= 0:
            return False, "DAILY_EMAIL_LIMIT must be > 0"
        
        return True, None
    
    @classmethod
    def print_config(cls):
        """Print current configuration (hiding sensitive data)."""
        api_key_preview = cls.SENDGRID_API_KEY[:10] + "..." if cls.SENDGRID_API_KEY else "NOT SET"
        
        print("\n" + "=" * 70)
        print("EMAIL CONFIGURATION")
        print("=" * 70)
        print(f"SendGrid API Key:     {api_key_preview}")
        print(f"Sender Email:         {cls.SENDER_EMAIL}")
        print(f"Sender Name:          {cls.SENDER_NAME}")
        print(f"Daily Limit:          {cls.DAILY_EMAIL_LIMIT} emails")
        print(f"Hourly Limit:         {cls.HOURLY_EMAIL_LIMIT} emails")
        print(f"Template ID:          {cls.SENDGRID_TEMPLATE_ID or 'Not set (using inline)'}")
        print(f"Max Retries:          {cls.MAX_RETRIES}")
        print(f"Retry Delay:          {cls.RETRY_DELAY}s")
        print(f"Cooldown Period:      {cls.DUPLICATE_COOLDOWN_DAYS} days")
        print("=" * 70 + "\n")


# Example .env file content:
"""
# Email Configuration (Phase 2)
SENDGRID_API_KEY=SG.your_api_key_here
SENDER_EMAIL=sales@yourcompany.com
SENDER_NAME=Your Company Sales Team
DAILY_EMAIL_LIMIT=100
HOURLY_EMAIL_LIMIT=10
SENDGRID_TEMPLATE_ID=d-1*****23456789abcdef0123456789abcde
"""