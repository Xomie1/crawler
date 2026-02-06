"""
Phase 3: Form Submission Worker
Processes form submission jobs in RQ background job queue
"""
import json
import logging
import time
import re
from datetime import datetime
from typing import Dict, List, Optional

from rq import get_current_job
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from phase5.database import FormSubmissionJob, FormSubmissionLog, DATABASE_URL

logger = logging.getLogger(__name__)

# Setup database
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def process_form_submissions(job_id: str) -> Dict:
    """
    Process a form submission job.
    Handles finding forms on target URLs and submitting data.
    
    Args:
        job_id: The form submission job ID
    
    Returns:
        Dictionary with submission results
    """
    current_job = get_current_job()
    db = SessionLocal()
    
    try:
        logger.info(f"Processing form submissions: {job_id}")
        
        # Get job from database
        job = db.query(FormSubmissionJob).filter(
            FormSubmissionJob.job_id == job_id
        ).first()
        
        if not job:
            logger.error(f"Job not found: {job_id}")
            return {"status": "failed", "error": "Job not found"}
        
        # Update status to in-progress
        job.status = "in-progress"
        job.started_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Job: {job.job_name}")
        logger.info(f"Total URLs: {job.total_urls}")
        logger.info(f"Submit method: {job.submit_method}")
        logger.info(f"Use Playwright: {job.use_playwright}")
        
        # Parse target URLs
        target_urls = job.target_urls
        if isinstance(target_urls, str):
            target_urls = json.loads(target_urls)
        
        # Parse form data
        form_data = job.form_data
        if isinstance(form_data, str):
            form_data = json.loads(form_data)
        
        # Process each URL
        successful = 0
        failed = 0
        captcha = 0
        
        for index, url in enumerate(target_urls):
            try:
                # Simulate form discovery and submission
                # In production, this would use actual form submission logic
                
                # Determine submission result (simulated)
                submission_result = _simulate_form_submission(
                    url,
                    form_data,
                    job.submit_method,
                    job.ignore_captcha
                )
                
                # Log submission attempt
                log_entry = FormSubmissionLog(
                    job_id=job.job_id,
                    url=url,
                    form_action=submission_result.get("form_action"),
                    form_method=submission_result.get("form_method", "POST"),
                    form_type=submission_result.get("form_type"),
                    detected_fields=submission_result.get("detected_fields"),
                    submitted_fields=list(form_data.keys()),
                    submission_status=submission_result.get("status"),
                    status_code=submission_result.get("status_code"),
                    error_message=submission_result.get("error_message"),
                    response_preview=submission_result.get("response_preview"),
                    captcha_detected=submission_result.get("captcha_detected", False),
                    captcha_type=submission_result.get("captcha_type"),
                    submitted_at=datetime.utcnow() if submission_result.get("status") != "not_found" else None,
                )
                
                db.add(log_entry)
                
                # Update counters
                if submission_result.get("status") == "success":
                    successful += 1
                elif submission_result.get("captcha_detected"):
                    captcha += 1
                else:
                    failed += 1
                
                db.commit()
                
                # Update job progress
                job.successful_submissions = successful
                job.failed_submissions = failed
                job.captcha_detected = captcha
                db.commit()
                
                logger.info(f"[{index+1}/{len(target_urls)}] {url}: {submission_result.get('status')}")
                
                # Rate limiting delay
                if index < len(target_urls) - 1:
                    time.sleep(job.delay)
            
            except Exception as e:
                logger.error(f"Error processing {url}: {str(e)}")
                failed += 1
                
                # Log failed submission
                log_entry = FormSubmissionLog(
                    job_id=job.job_id,
                    url=url,
                    submission_status="failed",
                    error_message=str(e),
                    submitted_at=datetime.utcnow(),
                )
                db.add(log_entry)
                db.commit()
        
        # Mark job as completed
        job.status = "completed"
        job.successful_submissions = successful
        job.failed_submissions = failed
        job.captcha_detected = captcha
        job.completed_at = datetime.utcnow()
        job.processing_time_seconds = int(
            (job.completed_at - job.started_at).total_seconds()
        )
        
        db.commit()
        
        logger.info(f"✅ Form submissions completed!")
        logger.info(f"Successful: {successful}, Failed: {failed}, CAPTCHA: {captcha}")
        
        return {
            "status": "completed",
            "job_id": job_id,
            "successful": successful,
            "failed": failed,
            "captcha": captcha,
            "processing_time": job.processing_time_seconds
        }
    
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        job = db.query(FormSubmissionJob).filter(
            FormSubmissionJob.job_id == job_id
        ).first()
        if job:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            db.commit()
        
        return {
            "status": "failed",
            "job_id": job_id,
            "error": str(e)
        }
    
    finally:
        db.close()


def _simulate_form_submission(
    url: str,
    form_data: Dict,
    submit_method: str,
    ignore_captcha: bool
) -> Dict:
    """
    Simulate form submission.
    In production, this would use actual HTTP/Playwright logic.
    """
    
    # Simulate form detection
    import random
    
    # Random outcomes for simulation
    rand = random.random()
    
    if rand > 0.85:  # 15% CAPTCHA detection
        return {
            "status": "captcha",
            "captcha_detected": True,
            "captcha_type": random.choice(["reCAPTCHA", "hCaptcha", "Cloudflare"]),
            "form_type": "contact",
            "form_action": f"{url}/submit",
            "form_method": "POST",
            "error_message": "CAPTCHA detected",
        }
    elif rand > 0.80:  # 5% form not found
        return {
            "status": "not_found",
            "captcha_detected": False,
            "form_type": None,
            "error_message": "No form found on page",
            "status_code": 404,
        }
    elif rand > 0.70:  # 10% submission failure
        return {
            "status": "failed",
            "captcha_detected": False,
            "form_type": random.choice(["contact", "inquiry", "comment"]),
            "form_action": f"{url}/submit",
            "form_method": "POST",
            "error_message": "Server error: 500",
            "status_code": 500,
        }
    else:  # 70% successful submission
        return {
            "status": "success",
            "captcha_detected": False,
            "form_type": random.choice(["contact", "inquiry", "comment"]),
            "form_action": f"{url}/submit",
            "form_method": "POST",
            "detected_fields": ["email", "name", "message", "subject"],
            "status_code": 200,
            "response_preview": "Thank you for your submission. We will get back to you soon.",
        }
