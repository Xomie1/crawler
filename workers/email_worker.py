# -*- coding: utf-8 -*-
"""
Email Worker
Handles email sending jobs via SendGrid
"""

import logging
import time
from datetime import datetime
from rq import get_current_job
from task_queue.config import EMAIL_RETRY
from services.sendgrid_email_service import SendGridEmailService
from services.db_service import FormSubmissionDB

logger = logging.getLogger(__name__)

# Rate limiting
_last_email_time = 0
_emails_sent_this_hour = 0
_hour_start = time.time()


def send_email_job(email, company_name, source_url=None, crawl_job_id=None, 
                   sender_name=None, message_body=None):
    """
    Send email job.
    
    Args:
        email: Recipient email
        company_name: Company name
        source_url: Source website URL
        crawl_job_id: Parent crawl job ID
        sender_name: Sender name (optional)
        message_body: Custom message (optional)
        
    Returns:
        dict: Email send result
    """
    job = get_current_job()
    job_id = job.id if job else 'local'
    
    logger.info(f"[{job_id}] Sending email to: {email} ({company_name})")
    
    try:
        # Rate limiting: respect SendGrid hourly limit
        global _last_email_time, _emails_sent_this_hour, _hour_start
        
        current_time = time.time()
        
        # Reset hourly counter
        if current_time - _hour_start > 3600:
            _emails_sent_this_hour = 0
            _hour_start = current_time
        
        # Check hourly limit
        from config.email_config import EmailConfig
        if _emails_sent_this_hour >= EmailConfig.HOURLY_EMAIL_LIMIT:
            wait_time = 3600 - (current_time - _hour_start)
            logger.warning(f"[{job_id}] Hourly limit reached, waiting {wait_time:.0f}s")
            time.sleep(wait_time)
            _emails_sent_this_hour = 0
            _hour_start = time.time()
        
        # Rate limiting between emails
        if _last_email_time > 0:
            elapsed = current_time - _last_email_time
            if elapsed < 6:  # 10 emails per minute = 6 seconds between emails
                time.sleep(6 - elapsed)
        
        _last_email_time = time.time()
        
        # Initialize email service
        email_service = SendGridEmailService()
        
        # Prepare template data
        template_data = {
            'company_name': company_name,
            'sender_name': sender_name or EmailConfig.SENDER_NAME,
            'sender_email': EmailConfig.SENDER_EMAIL,
            'message_body': message_body or f"""
{company_name}様の新製品・サービスについてお問い合わせさせていただきたくご連絡いたしました。

弊社では、企業様のビジネス成長をサポートするサービスを提供しております。

詳細につきましてはお手数ですが、お気軽にお問い合わせください。
""",
            'website_url': source_url or ''
        }
        
        # Send email
        result = email_service.send_email(
            to_email=email,
            to_name=company_name,
            subject=f"{company_name}様へのご提案",
            template_data=template_data,
            retry=False  # RQ handles retries
        )
        
        # Log to database
        try:
            db = FormSubmissionDB()
            db.log_email_send(
                recipient_email=email,
                recipient_name=company_name,
                company_name=company_name,
                subject=f"{company_name}様へのご提案",
                send_status='sent' if result['success'] else 'failed',
                message_id=result.get('message_id'),
                error_reason=result.get('error'),
                source_url=source_url
            )
            db.close()
        except Exception as e:
            logger.error(f"[{job_id}] Failed to log to DB: {e}")
        
        if result['success']:
            _emails_sent_this_hour += 1
            logger.info(f"[{job_id}] ✅ Email sent successfully")
            logger.info(f"[{job_id}]    Message ID: {result.get('message_id')}")
        else:
            logger.error(f"[{job_id}] ❌ Email send failed: {result.get('error')}")
        
        # Add metadata
        result['job_id'] = job_id
        result['crawl_job_id'] = crawl_job_id
        result['sent_at'] = datetime.utcnow().isoformat()
        
        return result
        
    except Exception as e:
        logger.error(f"[{job_id}] ❌ Email job failed: {e}")
        
        return {
            'success': False,
            'error': str(e),
            'job_id': job_id,
            'crawl_job_id': crawl_job_id,
            'email': email,
            'company_name': company_name
        }