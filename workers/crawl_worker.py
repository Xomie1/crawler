# -*- coding: utf-8 -*-
"""
Crawl Worker
Handles URL crawling jobs and auto-pushes to email/form queues
"""

import logging
import time
from datetime import datetime
from rq import get_current_job
from task_queue.queues import email_queue, form_queue, redis_conn
from task_queue.config import CRAWL_RETRY
from crawler.engine import CrawlerEngine

logger = logging.getLogger(__name__)

# Rate limiting: track last crawl time per domain
_domain_last_crawl = {}


def crawl_url_job(url, company_name=None, use_ai=True, ai_provider='groq'):
    """
    Main crawl job function.
    
    Args:
        url: URL to crawl
        company_name: Pre-provided company name (optional)
        use_ai: Enable AI extraction
        ai_provider: AI provider (groq/openai)
        
    Returns:
        dict: Crawl result
    """
    job = get_current_job()
    job_id = job.id if job else 'local'
    
    logger.info(f"[{job_id}] Starting crawl: {url}")
    
    try:
        # Rate limiting per domain
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        
        if domain in _domain_last_crawl:
            elapsed = time.time() - _domain_last_crawl[domain]
            if elapsed < 2.0:  # Wait at least 2 seconds between requests to same domain
                wait_time = 2.0 - elapsed
                logger.info(f"[{job_id}] Rate limiting: waiting {wait_time:.1f}s for {domain}")
                time.sleep(wait_time)
        
        _domain_last_crawl[domain] = time.time()
        
        # Perform crawl
        crawler = CrawlerEngine(
            root_url=url,
            use_ai_extraction=use_ai,
            ai_provider=ai_provider,
            use_enhanced_form_detection=True,
            max_form_pages=15
        )
        
        result = crawler.crawl()
        crawler.close()
        
        # Add metadata
        result['job_id'] = job_id
        result['crawled_at'] = datetime.utcnow().isoformat()
        result['queue_status'] = 'crawled'
        
        logger.info(f"[{job_id}] ✅ Crawl completed: {url}")
        logger.info(f"[{job_id}]    Email: {result.get('email', 'N/A')}")
        logger.info(f"[{job_id}]    Form: {result.get('inquiryFormUrl', 'N/A')}")
        logger.info(f"[{job_id}]    Company: {result.get('companyName', 'N/A')}")
        
        # AUTO-PUSH TO EMAIL QUEUE
        if result.get('email'):
            try:
                email_job = email_queue.enqueue(
                    'workers.email_worker.send_email_job',
                    email=result['email'],
                    company_name=result.get('companyName', company_name or 'Unknown'),
                    source_url=url,
                    crawl_job_id=job_id,
                    retry=CRAWL_RETRY
                )
                logger.info(f"[{job_id}] 📧 Queued email job: {email_job.id}")
                result['email_job_id'] = email_job.id
                result['queue_status'] = 'email_queued'
            except Exception as e:
                logger.error(f"[{job_id}] Failed to queue email: {e}")
        
        # AUTO-PUSH TO FORM QUEUE
        if result.get('inquiryFormUrl'):
            try:
                form_job = form_queue.enqueue(
                    'workers.form_worker.submit_form_job',
                    form_url=result['inquiryFormUrl'],
                    company_name=result.get('companyName', company_name or 'Unknown'),
                    source_url=url,
                    crawl_job_id=job_id,
                    retry=CRAWL_RETRY
                )
                logger.info(f"[{job_id}] 📝 Queued form job: {form_job.id}")
                result['form_job_id'] = form_job.id
                
                if not result.get('email'):
                    result['queue_status'] = 'form_queued'
            except Exception as e:
                logger.error(f"[{job_id}] Failed to queue form: {e}")
        
        # If neither email nor form, mark as completed
        if not result.get('email') and not result.get('inquiryFormUrl'):
            result['queue_status'] = 'completed_no_contact'
        
        return result
        
    except Exception as e:
        logger.error(f"[{job_id}] ❌ Crawl failed: {url}")
        logger.error(f"[{job_id}] Error: {e}")
        
        # Return error result
        return {
            'url': url,
            'job_id': job_id,
            'crawlStatus': 'error',
            'errorMessage': str(e),
            'queue_status': 'failed',
            'crawled_at': datetime.utcnow().isoformat()
        }


def crawl_batch_job(urls, use_ai=True, ai_provider='groq'):
    """
    Batch crawl multiple URLs.
    
    This queues each URL as a separate job for parallel processing.
    
    Args:
        urls: List of URLs to crawl
        use_ai: Enable AI extraction
        ai_provider: AI provider
        
    Returns:
        dict: Summary of queued jobs
    """
    job = get_current_job()
    job_id = job.id if job else 'local'
    
    logger.info(f"[{job_id}] Batch crawl: {len(urls)} URLs")
    
    from task_queue.queues import crawl_queue
    
    queued_jobs = []
    
    for url in urls:
        try:
            child_job = crawl_queue.enqueue(
                'workers.crawl_worker.crawl_url_job',
                url=url,
                use_ai=use_ai,
                ai_provider=ai_provider,
                retry=CRAWL_RETRY
            )
            queued_jobs.append(child_job.id)
        except Exception as e:
            logger.error(f"[{job_id}] Failed to queue {url}: {e}")
    
    logger.info(f"[{job_id}] ✅ Queued {len(queued_jobs)} crawl jobs")
    
    return {
        'batch_job_id': job_id,
        'total_urls': len(urls),
        'queued_jobs': len(queued_jobs),
        'job_ids': queued_jobs
    }