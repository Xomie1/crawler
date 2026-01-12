# -*- coding: utf-8 -*-
"""
SendGrid Email Service - Phase 2 (MODIFIED to use local template)
Core email sending functionality with LOCAL HTML template instead of SendGrid template
"""

import logging
import time
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email, To, Content, Personalization
except ImportError:
    print("ERROR: sendgrid not installed. Run: pip install sendgrid")
    raise

from config.email_config import EmailConfig

logger = logging.getLogger(__name__)


class SendGridEmailService:
    """Handles email sending via SendGrid API using LOCAL HTML template."""
    
    def __init__(self, template_file: str = "email_templates/inquiry_template.html"):
        """
        Initialize SendGrid client.
        
        Args:
            template_file: Path to local HTML template file
        """
        # Validate configuration
        is_valid, error = EmailConfig.validate()
        if not is_valid:
            raise ValueError(f"Invalid email configuration: {error}")
        
        self.client = SendGridAPIClient(EmailConfig.SENDGRID_API_KEY)
        self.sender_email = EmailConfig.SENDER_EMAIL
        self.sender_name = EmailConfig.SENDER_NAME
        
        # Load local template instead of using SendGrid template ID
        self.template_file = Path(template_file)
        self.template_html = self._load_template()
        
        logger.info("SendGrid Email Service initialized")
        logger.info(f"  Sender: {self.sender_name} <{self.sender_email}>")
        logger.info(f"  Template: {self.template_file} (LOCAL)")
    
    def _load_template(self) -> str:
        """Load HTML template from local file."""
        try:
            if not self.template_file.exists():
                logger.warning(f"Template file not found: {self.template_file}")
                logger.info("Using default inline template")
                return EmailConfig.DEFAULT_EMAIL_TEMPLATE
            
            with open(self.template_file, 'r', encoding='utf-8') as f:
                template_content = f.read()
            
            logger.info(f"✅ Loaded template from {self.template_file}")
            return template_content
            
        except Exception as e:
            logger.error(f"Failed to load template: {e}")
            logger.info("Using default inline template")
            return EmailConfig.DEFAULT_EMAIL_TEMPLATE
    
    def send_email(
        self,
        to_email: str,
        to_name: Optional[str] = None,
        subject: Optional[str] = None,
        template_data: Optional[Dict] = None,
        retry: bool = True
    ) -> Dict:
        """
        Send a single email with retry logic using LOCAL template.
        
        Args:
            to_email: Recipient email address
            to_name: Recipient name (optional)
            subject: Email subject (uses default if not provided)
            template_data: Data for template variables (required)
            retry: Enable retry on failure
            
        Returns:
            Result dictionary with 'success', 'message_id', 'error'
        """
        result = {
            'success': False,
            'message_id': None,
            'error': None,
            'attempts': 0,
            'sent_at': None
        }
        
        # Validate email
        if not self._is_valid_email(to_email):
            result['error'] = f"Invalid email address: {to_email}"
            logger.warning(result['error'])
            return result
        
        # Check blacklist
        if self._is_blacklisted(to_email):
            result['error'] = f"Email domain is blacklisted: {to_email}"
            logger.warning(result['error'])
            return result
        
        # Prepare message using local template
        try:
            message = self._create_email_from_local_template(
                to_email, to_name, subject, template_data
            )
        except Exception as e:
            result['error'] = f"Failed to create message: {str(e)}"
            logger.error(result['error'])
            return result
        
        # Send with retry
        max_attempts = EmailConfig.MAX_RETRIES if retry else 1
        
        for attempt in range(1, max_attempts + 1):
            result['attempts'] = attempt
            
            try:
                logger.debug(f"Sending to {to_email} (attempt {attempt}/{max_attempts})...")
                
                response = self.client.send(message)
                
                if response.status_code in [200, 202]:
                    result['success'] = True
                    result['message_id'] = response.headers.get('X-Message-Id')
                    result['sent_at'] = datetime.utcnow().isoformat()
                    logger.info(f"✅ Email sent to {to_email} (ID: {result['message_id']})")
                    return result
                else:
                    result['error'] = f"HTTP {response.status_code}: {response.body}"
                    logger.warning(f"⚠️ Attempt {attempt} failed: {result['error']}")
                    
            except Exception as e:
                result['error'] = str(e)
                logger.warning(f"⚠️ Attempt {attempt} failed: {result['error']}")
            
            # Wait before retry (except on last attempt)
            if attempt < max_attempts:
                delay = EmailConfig.RETRY_DELAY * attempt  # Linear backoff
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)
        
        # All attempts failed
        logger.error(f"❌ Failed to send to {to_email} after {max_attempts} attempts")
        return result
    
    def send_bulk(
        self,
        recipients: List[Dict],
        subject: Optional[str] = None,
        rate_limit: int = 10
    ) -> Dict:
        """
        Send emails to multiple recipients with rate limiting.
        
        Args:
            recipients: List of dicts with 'email', 'name', 'template_data'
            subject: Email subject (uses default if not provided)
            rate_limit: Max emails per minute (to avoid rate limits)
            
        Returns:
            Summary dictionary with success/failure counts
        """
        total = len(recipients)
        results = {
            'total': total,
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        logger.info(f"\n{'='*70}")
        logger.info(f"BULK EMAIL SENDING: {total} recipients")
        logger.info(f"{'='*70}")
        
        for i, recipient in enumerate(recipients, 1):
            to_email = recipient.get('email')
            to_name = recipient.get('name')
            template_data = recipient.get('template_data', {})
            
            logger.info(f"\n[{i}/{total}] Sending to {to_name or to_email}...")
            
            result = self.send_email(
                to_email=to_email,
                to_name=to_name,
                subject=subject,
                template_data=template_data
            )
            
            if result['success']:
                results['successful'] += 1
            else:
                results['failed'] += 1
            
            results['details'].append({
                'email': to_email,
                'name': to_name,
                'success': result['success'],
                'error': result['error'],
                'message_id': result['message_id']
            })
            
            # Rate limiting (except for last item)
            if i < total:
                delay = 60.0 / rate_limit  # emails per minute
                logger.debug(f"Rate limiting: waiting {delay:.1f}s...")
                time.sleep(delay)
        
        logger.info(f"\n{'='*70}")
        logger.info(f"BULK SEND COMPLETE")
        logger.info(f"  Successful: {results['successful']}/{total}")
        logger.info(f"  Failed: {results['failed']}/{total}")
        logger.info(f"{'='*70}\n")
        
        return results
    
    def _create_email_from_local_template(
        self,
        to_email: str,
        to_name: Optional[str],
        subject: Optional[str],
        template_data: Optional[Dict]
    ) -> Mail:
        """Create email using LOCAL HTML template with variable substitution."""
        
        # Use default subject if not provided
        if not subject:
            company_name = template_data.get('company_name', '御社') if template_data else '御社'
            subject = EmailConfig.DEFAULT_SUBJECT_TEMPLATE.replace('{company_name}', company_name)
        
        # Fill template with data
        html_content = self.template_html
        
        if template_data:
            # Replace variables in HTML template with {{variable}} syntax
            for key, value in template_data.items():
                # Double braces: {{variable}}
                placeholder_double = "{{" + key + "}}"
                html_content = html_content.replace(placeholder_double, str(value))
                
                # Then single braces {variable}
                placeholder_single = f"{{{key}}}"
                html_content = html_content.replace(placeholder_single, str(value))
        
        # Create plain text version (simple fallback)
        plain_content = f"""{template_data.get('company_name', '御社') if template_data else '御社'}様

    お世話になっております。{template_data.get('sender_name', 'Crawler Team') if template_data else 'Crawler Team'}と申します。

    {template_data.get('message_body', '') if template_data else ''}

    ご不明な点がございましたら、お気軽にお問い合わせください。

    よろしくお願いいたします。

    {template_data.get('sender_name', 'Crawler Team') if template_data else 'Crawler Team'}
    {template_data.get('sender_email', 'inquiry@example.com') if template_data else 'inquiry@example.com'}
    """
        
        # Create message - KEY FIX: Use correct Content types
        message = Mail(
            from_email=Email(self.sender_email, self.sender_name),
            to_emails=To(to_email, to_name),
            subject=subject
        )
        
        # IMPORTANT: Set both content types correctly
        # Plain text should come first, HTML second (email clients prefer HTML when available)
        message.add_content(Content("text/plain", plain_content))
        message.add_content(Content("text/html", html_content))
        
        return message

    def _is_valid_email(self, email: str) -> bool:
        """Validate email format."""
        if not email or '@' not in email:
            return False
        
        parts = email.split('@')
        if len(parts) != 2:
            return False
        
        local, domain = parts
        if not local or not domain or '.' not in domain:
            return False
        
        return True
    
    def _is_blacklisted(self, email: str) -> bool:
        """Check if email domain is blacklisted."""
        domain = email.split('@')[1] if '@' in email else ''
        return domain.lower() in EmailConfig.BLACKLIST_DOMAINS
    
    def remove_from_cache(self, email: str):
        """Remove a specific email from sent cache for testing."""
        if email in self.sent_emails:
            del self.sent_emails[email]
            self._save_sent_cache()
            logger.info(f"Removed {email} from sent cache")
        else:
            logger.info(f"{email} not in cache")

    def send_test_email(self, to_email: str) -> Dict:
        """
        Send a test email (for verification).
        
        Args:
            to_email: Your email address
            
        Returns:
            Result dictionary
        """
        logger.info(f"Sending test email to {to_email}...")
        
        template_data = {
            'company_name': 'テスト株式会社',
            'sender_name': self.sender_name,
            'sender_email': self.sender_email,
            'message_body': f"""
このメールは、あなたのWebクローラーメールサービスからのテストメールです。

このメールが表示されている場合、SendGrid統合が正常に動作しています!

送信時刻: {datetime.utcnow().isoformat()}

設定:
- 送信者: {self.sender_name} <{self.sender_email}>
- テンプレート: {self.template_file} (ローカル)
- 日次制限: {EmailConfig.DAILY_EMAIL_LIMIT}

よろしくお願いいたします。
""",
            'website_url': 'https://example.com'
        }
        
        return self.send_email(
            to_email=to_email,
            subject="テストメール - Webクローラー",
            template_data=template_data
        )


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Print configuration
    EmailConfig.print_config()
    
    # Test send
    service = SendGridEmailService(template_file="email_templates/inquiry_template.html")
    
    # Send test email to yourself
    result = service.send_test_email("your-email@example.com")
    
    if result['success']:
        print(f"\n✅ Test email sent successfully!")
        print(f"   Message ID: {result['message_id']}")
    else:
        print(f"\n❌ Test email failed!")
        print(f"   Error: {result['error']}")