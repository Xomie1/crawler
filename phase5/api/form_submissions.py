"""
Phase 3: Form Submission API
Handles automated form submission on discovered websites
"""
import uuid
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session

from phase5.database import (
    SessionLocal,
    FormSubmissionJob as DBFormSubmissionJob,
    FormSubmissionLog as DBFormSubmissionLog,
    CrawlJob as DBCrawlJob,
)
from phase5.models import (
    FormSubmissionJobRequest,
    FormSubmissionResponse,
    FormSubmissionProgressResponse,
    FormSubmissionCompletedResponse,
)

router = APIRouter(prefix="/api/forms", tags=["forms"])


def get_db():
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/submit", response_model=FormSubmissionResponse, status_code=202)
async def submit_form_job(
    request: FormSubmissionJobRequest,
    user_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Submit a form submission job.
    
    Can submit forms on manually specified URLs or from crawl job results.
    Supports template variable interpolation and CAPTCHA detection.
    """
    try:
        job_id = str(uuid.uuid4())
        
        # If from crawl job, validate it exists
        if request.source_crawl_job_id:
            crawl_job = db.query(DBCrawlJob).filter(
                DBCrawlJob.job_id == request.source_crawl_job_id
            ).first()
            
            if not crawl_job:
                raise HTTPException(
                    status_code=404,
                    detail=f"Crawl job not found: {request.source_crawl_job_id}"
                )
            
            if crawl_job.status != "completed":
                raise HTTPException(
                    status_code=400,
                    detail=f"Crawl job not completed: {crawl_job.status}"
                )
        
        # Prepare form data
        form_data = {
            "email": request.email,
            "name": request.name,
            "phone": request.phone,
            "company": request.company,
            "message": request.message,
        }
        
        # Add additional fields
        if request.additional_fields:
            form_data.update(request.additional_fields)
        
        # Remove None values
        form_data = {k: v for k, v in form_data.items() if v is not None}
        
        # Create submission job
        total_urls = len(request.target_urls)
        
        submission_job = DBFormSubmissionJob(
            job_id=job_id,
            user_id=user_id,
            job_name=request.job_name,
            source_crawl_job_id=request.source_crawl_job_id,
            target_urls=request.target_urls,
            form_data=form_data,
            submit_method=request.submit_method,
            use_playwright=request.use_playwright,
            ignore_captcha=request.ignore_captcha,
            timeout=request.timeout,
            delay=request.delay,
            status="queued",
            total_urls=total_urls,
        )
        
        db.add(submission_job)
        db.commit()
        db.refresh(submission_job)
        
        # Queue the form submission job for processing
        try:
            from rq import Queue
            from redis import Redis
            redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)
            form_queue = Queue('form_queue', connection=redis_conn)
            
            form_queue.enqueue(
                'workers.form_submission_worker.process_form_submissions',
                job_id=job_id,
                timeout=3600
            )
        except Exception as queue_err:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not queue form submission job: {queue_err}")
        
        return FormSubmissionResponse(
            status="queued",
            job_id=job_id,
            message="Form submission job queued",
            job_name=request.job_name,
            total_urls=total_urls,
            check_status_url=f"/api/forms/status/{job_id}"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{job_id}", response_model=FormSubmissionProgressResponse)
async def get_form_status(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Get real-time status of a form submission job.
    Returns progress updates while running, final summary when complete.
    """
    try:
        job = db.query(DBFormSubmissionJob).filter(
            DBFormSubmissionJob.job_id == job_id
        ).first()
        
        if not job:
            raise HTTPException(status_code=404, detail="Form submission job not found")
        
        # Calculate progress percentage
        if job.total_urls > 0:
            progress_pct = ((job.successful_submissions + job.failed_submissions + job.captcha_detected) 
                          / job.total_urls) * 100
        else:
            progress_pct = 0.0
        
        return FormSubmissionProgressResponse(
            status=job.status,
            job_id=job.job_id,
            job_name=job.job_name,
            total_urls=job.total_urls,
            successful_submissions=job.successful_submissions,
            failed_submissions=job.failed_submissions,
            captcha_detected=job.captcha_detected,
            progress_percentage=min(progress_pct, 100.0),
            error_message=job.error_message
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/completed/{job_id}", response_model=FormSubmissionCompletedResponse)
async def get_form_results(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Get final results of a completed form submission job.
    Only available after job completes.
    """
    try:
        job = db.query(DBFormSubmissionJob).filter(
            DBFormSubmissionJob.job_id == job_id
        ).first()
        
        if not job:
            raise HTTPException(status_code=404, detail="Form submission job not found")
        
        if job.status != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Job not completed: {job.status}"
            )
        
        success_rate = (job.successful_submissions / job.total_urls * 100) if job.total_urls > 0 else 0
        
        return FormSubmissionCompletedResponse(
            status=job.status,
            job_id=job.job_id,
            job_name=job.job_name,
            total_urls=job.total_urls,
            successful_submissions=job.successful_submissions,
            failed_submissions=job.failed_submissions,
            captcha_detected=job.captcha_detected,
            success_rate=success_rate,
            processing_time_seconds=job.processing_time_seconds or 0,
            created_at=job.created_at,
            completed_at=job.completed_at or datetime.utcnow()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_form_jobs(
    user_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    List form submission jobs with filtering and pagination.
    
    Query Parameters:
    - user_id: Filter by user
    - status: Filter by status ('queued', 'in-progress', 'completed', 'failed')
    - limit: Number of results (1-100)
    - offset: Pagination offset
    """
    try:
        query = db.query(DBFormSubmissionJob)
        
        if user_id:
            query = query.filter(DBFormSubmissionJob.user_id == user_id)
        
        if status:
            query = query.filter(DBFormSubmissionJob.status == status)
        
        total = query.count()
        jobs = query.order_by(DBFormSubmissionJob.created_at.desc()).offset(offset).limit(limit).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "jobs": [
                {
                    "job_id": j.job_id,
                    "job_name": j.job_name,
                    "status": j.status,
                    "total_urls": j.total_urls,
                    "successful_submissions": j.successful_submissions,
                    "failed_submissions": j.failed_submissions,
                    "captcha_detected": j.captcha_detected,
                    "created_at": j.created_at,
                    "completed_at": j.completed_at,
                }
                for j in jobs
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/{job_id}")
async def get_submission_logs(
    job_id: str,
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get detailed submission logs for a job.
    Useful for debugging submission issues and viewing per-URL results.
    """
    try:
        query = db.query(DBFormSubmissionLog).filter(
            DBFormSubmissionLog.job_id == job_id
        )
        
        if status:
            query = query.filter(DBFormSubmissionLog.submission_status == status)
        
        total = query.count()
        logs = query.order_by(DBFormSubmissionLog.created_at.desc()).offset(offset).limit(limit).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "logs": [
                {
                    "id": log.id,
                    "url": log.url,
                    "form_type": log.form_type,
                    "submission_status": log.submission_status,
                    "status_code": log.status_code,
                    "error_message": log.error_message,
                    "captcha_detected": log.captcha_detected,
                    "captcha_type": log.captcha_type,
                    "detected_fields": log.detected_fields,
                    "submitted_at": log.submitted_at,
                    "created_at": log.created_at,
                }
                for log in logs
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_form_statistics(
    user_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get form submission statistics.
    Returns aggregated data across all jobs for a user.
    """
    try:
        query = db.query(DBFormSubmissionJob)
        
        if user_id:
            query = query.filter(DBFormSubmissionJob.user_id == user_id)
        
        jobs = query.all()
        
        total_urls = sum(j.total_urls for j in jobs)
        total_successful = sum(j.successful_submissions for j in jobs)
        total_failed = sum(j.failed_submissions for j in jobs)
        total_captcha = sum(j.captcha_detected for j in jobs)
        total_jobs = len(jobs)
        completed = sum(1 for j in jobs if j.status == "completed")
        
        return {
            "total_jobs": total_jobs,
            "completed_jobs": completed,
            "total_urls_processed": total_urls,
            "total_successful": total_successful,
            "total_failed": total_failed,
            "total_captcha_detected": total_captcha,
            "overall_success_rate": (total_successful / total_urls * 100) if total_urls > 0 else 0,
            "captcha_rate": (total_captcha / total_urls * 100) if total_urls > 0 else 0,
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
