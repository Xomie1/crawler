# -*- coding: utf-8 -*-
"""
Form Worker
Handles form submission jobs
"""

import logging
import time
from datetime import datetime
from rq import get_current_job
from task_queue.queues import captcha_queue
from task_queue.config import FORM_RETRY
from services.form_submission_service import FormSubmissionService
from services.db_service import FormSubmissionDB

logger = logging.getLogger(__name__)

# Rate limiting per domain
_domain_last_submit = {}


def submit_form_job(form_url, company_name, source_url=None, crawl_job_id=None,
                    sender_email=None, sender_name=None, phone=None, message_body=None):
    """
    Submit form job.
    
    Args:
        form_url: Form URL to submit
        company_name: Company name
        source_url: Source website URL
        crawl_job_id: Parent crawl job ID
        sender_email: Sender email (optional)
        sender_name: Sender name (optional)
        phone: Phone number (optional)
        message_body: Custom message (optional)
        
    Returns:
        dict: Form submission result
    """
    job = get_current_job()
    job_id = job.id if job else 'local'
    
    logger.info(f"[{job_id}] Submitting form: {form_url} ({company_name})")
    
    try:
        # Rate limiting per domain
        from urllib.parse import urlparse
        domain = urlparse(form_url).netloc
        
        if domain in _domain_last_submit:
            elapsed = time.time() - _domain_last_submit[domain]
            if elapsed < 5.0:  # Wait at least 5 seconds between form submissions to same domain
                wait_time = 5.0 - elapsed
                logger.info(f"[{job_id}] Rate limiting: waiting {wait_time:.1f}s for {domain}")
                time.sleep(wait_time)
        
        _domain_last_submit[domain] = time.time()
        
        # Initialize form submission service
        form_service = FormSubmissionService(
            throttle_per_site=5.0
        )
        
        # Submit inquiry
        result = form_service.submit_inquiry(
            form_url=form_url,
            company_name=company_name,
            sender_email=sender_email or 'inquiry@example.com',
            sender_name=sender_name or 'お問い合わせ',
            phone=phone,
            message_body=message_body
        )
        
        # Check for CAPTCHA
        if result.get('captcha_queued'):
            logger.warning(f"[{job_id}] 🔐 CAPTCHA detected - moved to captcha_queue")
            
            # Move to CAPTCHA queue for manual handling
            try:
                captcha_job = captcha_queue.enqueue(
                    'workers.form_worker.handle_captcha_form',
                    form_url=form_url,
                    company_name=company_name,
                    captcha_type=result.get('error', 'unknown'),
                    source_url=source_url,
                    crawl_job_id=crawl_job_id
                )
                logger.info(f"[{job_id}] Queued to CAPTCHA queue: {captcha_job.id}")
            except Exception as e:
                logger.error(f"[{job_id}] Failed to queue CAPTCHA job: {e}")
        
        # Log result
        if result['submission_status'] == 'success':
            logger.info(f"[{job_id}] ✅ Form submitted successfully")
        elif result['submission_status'] == 'captcha_blocked':
            logger.warning(f"[{job_id}] 🔐 Form blocked by CAPTCHA")
        else:
            logger.error(f"[{job_id}] ❌ Form submission failed: {result.get('error')}")
        
        # Add metadata
        result['job_id'] = job_id
        result['crawl_job_id'] = crawl_job_id
        result['submitted_at'] = datetime.utcnow().isoformat()
        
        # Cleanup
        form_service.close()
        
        return result
        
    except Exception as e:
        logger.error(f"[{job_id}] ❌ Form job failed: {e}")
        
        # Log error to database
        try:
            db = FormSubmissionDB()
            db.log_error(
                phase='phase3',
                error_type='FORM_SUBMISSION_ERROR',
                error_message=str(e),
                context=f"form_url={form_url}, company={company_name}"
            )
            db.close()
        except Exception as log_err:
            logger.error(f"[{job_id}] Failed to log error: {log_err}")
        
        return {
            'submission_status': 'error',
            'error': str(e),
            'job_id': job_id,
            'crawl_job_id': crawl_job_id,
            'form_url': form_url,
            'company_name': company_name
        }


def handle_captcha_form(form_url, company_name, captcha_type, source_url=None, crawl_job_id=None):
    """
    Handle CAPTCHA form (placeholder for manual intervention).
    
    This job just logs that manual intervention is needed.
    In the future, this could integrate with CAPTCHA solving services.
    
    Args:
        form_url: Form URL
        company_name: Company name
        captcha_type: Type of CAPTCHA detected
        source_url: Source URL
        crawl_job_id: Parent crawl job ID
        
    Returns:
        dict: CAPTCHA handling result
    """
    job = get_current_job()
    job_id = job.id if job else 'local'
    
    logger.info(f"[{job_id}] 🔐 CAPTCHA form queued for manual handling")
    logger.info(f"[{job_id}]    Form: {form_url}")
    logger.info(f"[{job_id}]    Company: {company_name}")
    logger.info(f"[{job_id}]    Type: {captcha_type}")
    
    # Log to database for export
    try:
        from crawler.captcha_queue_manager import CaptchaQueueManager
        
        captcha_manager = CaptchaQueueManager()
        captcha_manager.add_form(
            url=source_url or form_url,
            form_url=form_url,
            captcha_type=captcha_type,
            company_name=company_name,
            notes=f"Auto-detected during queue processing. Job ID: {job_id}"
        )
    except Exception as e:
        logger.error(f"[{job_id}] Failed to log CAPTCHA form: {e}")
    
    return {
        'status': 'captcha_detected',
        'form_url': form_url,
        'company_name': company_name,
        'captcha_type': captcha_type,
        'job_id': job_id,
        'crawl_job_id': crawl_job_id,
        'requires_manual': True
    }