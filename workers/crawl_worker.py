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


def crawl_urls_job(urls, company_names=None, timeout=30, robots_policy='respect', use_playwright=True, delay=10, job_id=None):
    """
    Batch crawl job for Phase 5 API integration.
    
    Args:
        urls: List of URLs to crawl
        company_names: Optional list of company names per URL
        timeout: Timeout per request (seconds)
        robots_policy: 'respect' or 'ignore'
        use_playwright: Whether to use browser automation
        delay: Delay between requests (seconds)
        job_id: Optional job ID (if not provided, gets from RQ job)
        
    Returns:
        dict: Crawl results with success/failure status
    """
    from batch.batch_crawler import BatchCrawler
    from datetime import datetime
    import json
    from pathlib import Path
    
    # Get job ID from RQ job (this is set by the API when enqueueing)
    rq_job = get_current_job()
    if not job_id:
        job_id = rq_job.id if rq_job else 'local'
    
    # Ensure we have a valid job_id
    if not job_id or job_id == 'local':
        logger.warning(f"[{job_id}] No valid job_id provided, using RQ job ID")
        if rq_job:
            job_id = rq_job.id
    
    logger.info(f"[{job_id}] Starting batch crawl of {len(urls)} URLs")
    
    # Get database session to update progress
    db = None
    db_job = None
    try:
        from phase5.database import SessionLocal, CrawlJob as DBCrawlJob
        db = SessionLocal()
        db_job = db.query(DBCrawlJob).filter(DBCrawlJob.job_id == job_id).first()
        
        if db_job:
            # Update status to processing
            db_job.status = "processing"
            db_job.started_at = datetime.utcnow()
            db_job.progress_urls_crawled = 0
            db_job.progress_total_urls = len(urls)
            db.commit()
            logger.info(f"[{job_id}] Updated database: status=processing")
    except Exception as db_err:
        logger.warning(f"[{job_id}] Could not update database: {db_err}")
        db = None
    
    try:
        # Initialize batch crawler
        crawler = BatchCrawler(
            timeout=timeout,
            robots_policy=robots_policy,
            user_agent="DocGen-Crawler/1.0",
            delay=delay,
            use_playwright=use_playwright,
            auto_export_excel=True
        )
        
        # Process URLs one by one to track progress
        results = []
        total = len(urls)
        if company_names is None:
            company_names = [None] * total
        
        for i, (url, company_name) in enumerate(zip(urls, company_names), 1):
            try:
                # Create crawler for this URL
                url_crawler = CrawlerEngine(
                    root_url=url,
                    crawl_settings={'timeout': timeout},
                    user_agent_policy="DocGen-Crawler/1.0",
                    robots_policy=robots_policy,
                    use_enhanced_form_detection=True,
                    max_form_pages=15
                )
                url_crawler.use_playwright = use_playwright
                
                # Crawl this URL
                result = url_crawler.crawl()
                results.append(result)
                url_crawler.close()
                
                # Update database progress
                if db and db_job:
                    db_job.progress_urls_crawled = i
                    db.commit()
                    logger.debug(f"[{job_id}] Progress: {i}/{total}")
                
                # Rate limiting delay
                if i < total:
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"[{job_id}] Error crawling {url}: {e}")
                results.append({
                    'url': url,
                    'crawlStatus': 'error',
                    'errorMessage': str(e),
                    'companyName': company_name
                })
                # Still update progress
                if db and db_job:
                    db_job.progress_urls_crawled = i
                    db.commit()
        
        # Save to JSONL
        results_dir = Path("crawl_results")
        results_dir.mkdir(exist_ok=True)
        
        jsonl_file = results_dir / f"crawl_{job_id}.jsonl"
        with open(jsonl_file, 'w', encoding='utf-8') as f:
            for result in results:
                f.write(json.dumps(result, ensure_ascii=False) + '\n')
        
        # Export to Excel if auto_export is enabled
        excel_file = None
        try:
            crawler.results = results
            crawler.jsonl_file = str(jsonl_file)
            excel_file = crawler.export_to_excel()
        except Exception as excel_err:
            logger.warning(f"[{job_id}] Excel export failed: {excel_err}")
        
        # Update database with completion
        if db and db_job:
            db_job.status = "completed"
            db_job.completed_at = datetime.utcnow()
            db_job.progress_urls_crawled = len(results)
            db_job.results_jsonl_path = str(jsonl_file)
            if excel_file:
                db_job.results_excel_path = excel_file
            db_job.results_count = sum(1 for r in results if r.get('crawlStatus') == 'success')
            db_job.errors_count = sum(1 for r in results if r.get('crawlStatus') == 'error')
            if db_job.started_at:
                db_job.processing_time_seconds = int(
                    (db_job.completed_at - db_job.started_at).total_seconds()
                )
            db.commit()
            logger.info(f"[{job_id}] Updated database: status=completed")
        
        logger.info(f"[{job_id}] ✅ Crawl complete: {len(results)} URLs processed")
        
        return {
            'status': 'success',
            'job_id': job_id,
            'total_urls': len(urls),
            'results_count': len(results),
            'errors_count': sum(1 for r in results if r.get('crawlStatus') == 'error'),
            'results_jsonl_path': str(jsonl_file),
            'completed_at': datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        logger.error(f"[{job_id}] ❌ Crawl failed: {str(e)}")
        
        # Update database with error
        if db and db_job:
            db_job.status = "failed"
            db_job.error_type = "crawl_error"
            db_job.error_message = str(e)
            db_job.completed_at = datetime.utcnow()
            db.commit()
        
        return {
            'status': 'error',
            'job_id': job_id,
            'error_type': 'crawl_error',
            'error_message': str(e)
        }
    finally:
        if db:
            db.close()