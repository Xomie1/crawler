"""
Web crawler API endpoints for Phase 1.
Handles URL crawling, file uploads, and progress tracking.
"""
import os
import json
import uuid
import tempfile
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form
from sqlalchemy.orm import Session
from rq import Queue
from redis import Redis

from ..models import (
    CrawlJobSubmitRequest,
    CrawlProgressResponse,
    CrawlCompletedResponse,
)
from ..database import get_db, CrawlJob as DBCrawlJob

router = APIRouter(prefix="/api/crawler", tags=["crawler"])

# Initialize Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)
crawl_queue = Queue('crawl_queue', connection=redis_conn)


@router.post("/submit", status_code=202)
async def submit_crawl_job(
    request: CrawlJobSubmitRequest,
    db: Session = Depends(get_db)
):
    """
    Submit a web crawling job.
    
    Returns job_id to check status later.
    """
    try:
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Extract URLs and company names
        urls = [item.url for item in request.urls]
        company_names = [item.company_name for item in request.urls]
        
        # Queue the crawl job
        try:
            rq_job = crawl_queue.enqueue(
                'workers.crawl_worker.crawl_urls_job',
                urls,
                company_names,
                request.timeout,
                request.robots_policy,
                request.use_playwright,
                request.delay,
                job_id=job_id,
                timeout=3600,  # 1 hour max
                result_ttl=3600,  # Keep result for 1 hour
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={
                    "error_type": "queue_error",
                    "message": f"Failed to queue crawl job: {str(e)}"
                }
            )
        
        # Store job in database
        db_job = DBCrawlJob(
            id=str(uuid.uuid4()),
            job_id=job_id,
            user_id=request.user_id,
            urls_list=urls,
            company_names=company_names,
            timeout=request.timeout,
            robots_policy=request.robots_policy,
            use_playwright=request.use_playwright,
            delay=request.delay,
            status="queued",
            progress_total_urls=len(urls),
            progress_urls_crawled=0,
            created_at=datetime.utcnow()
        )
        db.add(db_job)
        db.commit()
        
        return {
            "status": "queued",
            "job_id": job_id,
            "message": f"Crawl job queued with {len(urls)} URLs",
            "total_urls": len(urls),
            "check_status_url": f"/api/crawler/status/{job_id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to submit crawl job: {str(e)}"
            }
        )


@router.post("/upload-excel", status_code=202)
async def upload_excel_and_crawl(
    file: UploadFile = File(...),
    user_id: Optional[str] = None,
    timeout: int = 30,
    robots_policy: str = "respect",
    use_playwright: bool = True,
    delay: int = 10,
    db: Session = Depends(get_db)
):
    """
    Upload Excel file with URLs and start crawl job.
    
    Excel format expected:
    - Column A: URLs
    - Column B (optional): Company Names
    """
    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="pandas not installed. Required for Excel processing."
        )
    
    try:
        # Validate file is Excel
        if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
            raise HTTPException(
                status_code=400,
                detail="File must be .xlsx, .xls, or .csv"
            )
        
        # Read Excel file
        content = await file.read()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        temp_file.write(content)
        temp_file.close()
        
        # Parse Excel
        if file.filename.endswith('.csv'):
            df = pd.read_csv(temp_file.name)
        else:
            df = pd.read_excel(temp_file.name)
        
        # Extract URLs and company names
        urls = []
        company_names = []
        
        for idx, row in df.iterrows():
            # Try different column names
            url = None
            for col in ['url', 'URL', 'Url', 'Website', 'website', 'Link', 'link']:
                if col in df.columns:
                    url = row[col]
                    break
            
            if not url or pd.isna(url):
                continue
            
            urls.append(str(url).strip())
            
            # Try to get company name
            company = None
            for col in ['company', 'Company', 'Company Name', 'company_name', 'Name', 'name']:
                if col in df.columns and pd.notna(row[col]):
                    company = str(row[col]).strip()
                    break
            
            company_names.append(company)
        
        # Clean up temp file
        os.unlink(temp_file.name)
        
        if not urls:
            raise HTTPException(
                status_code=400,
                detail="No valid URLs found in Excel file"
            )
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Queue the crawl job
        try:
            rq_job = crawl_queue.enqueue(
                'workers.crawl_worker.crawl_urls_job',
                urls,
                company_names,
                timeout,
                robots_policy,
                use_playwright,
                delay,
                job_id=job_id,
                timeout=3600,
                result_ttl=3600,
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={
                    "error_type": "queue_error",
                    "message": f"Failed to queue crawl: {str(e)}"
                }
            )
        
        # Store job in database
        db_job = DBCrawlJob(
            id=str(uuid.uuid4()),
            job_id=job_id,
            user_id=user_id,
            input_file_name=file.filename,
            urls_list=urls,
            company_names=company_names,
            timeout=timeout,
            robots_policy=robots_policy,
            use_playwright=use_playwright,
            delay=delay,
            status="queued",
            progress_total_urls=len(urls),
            progress_urls_crawled=0,
            created_at=datetime.utcnow()
        )
        db.add(db_job)
        db.commit()
        
        return {
            "status": "queued",
            "job_id": job_id,
            "message": f"Crawl job queued with {len(urls)} URLs from {file.filename}",
            "total_urls": len(urls),
            "check_status_url": f"/api/crawler/status/{job_id}"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to process upload: {str(e)}"
            }
        )


@router.get("/status/{job_id}")
async def get_crawl_status(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Check the status of a crawl job.
    
    Returns different response based on status:
    - queued/processing: progress info
    - completed: result URLs and download links
    - failed: error details
    """
    try:
        # First try to find in database
        db_job = db.query(DBCrawlJob).filter(
            DBCrawlJob.job_id == job_id
        ).first()
        
        if not db_job:
            # Try to get from queue if not in database yet
            try:
                rq_job = crawl_queue.fetch_job(job_id)
                if not rq_job:
                    raise HTTPException(status_code=404, detail="Job not found")
                
                # Job is in queue but not in database yet
                if rq_job.is_queued:
                    return CrawlProgressResponse(
                        status="queued",
                        job_id=job_id,
                        urls_crawled=0,
                        total_urls=1,
                        percentage=0,
                        estimated_time_remaining_seconds=30
                    )
                elif rq_job.is_started:
                    return CrawlProgressResponse(
                        status="processing",
                        job_id=job_id,
                        urls_crawled=0,
                        total_urls=1,
                        percentage=0,
                        estimated_time_remaining_seconds=60
                    )
            except Exception as redis_err:
                # Redis might not be available, return queued status
                return CrawlProgressResponse(
                    status="queued",
                    job_id=job_id,
                    urls_crawled=0,
                    total_urls=1,
                    percentage=0,
                    estimated_time_remaining_seconds=30
                )
        
        # Return status from database
        if db_job.status == "completed":
            return CrawlCompletedResponse(
                status="completed",
                job_id=job_id,
                urls_crawled=db_job.progress_urls_crawled,
                total_urls=db_job.progress_total_urls,
                successful=db_job.results_count,
                failed=db_job.errors_count,
                results_excel_url=f"/api/crawler/download/{os.path.basename(db_job.results_excel_path)}" if db_job.results_excel_path else None,
                results_jsonl_url=f"/api/crawler/download/{os.path.basename(db_job.results_jsonl_path)}" if db_job.results_jsonl_path else None,
                created_at=db_job.created_at,
                completed_at=db_job.completed_at or datetime.utcnow(),
                processing_time_seconds=db_job.processing_time_seconds or 0
            )
        elif db_job.status == "failed":
            return CrawlProgressResponse(
                status="failed",
                job_id=job_id,
                urls_crawled=db_job.progress_urls_crawled,
                total_urls=db_job.progress_total_urls,
                percentage=int((db_job.progress_urls_crawled / max(1, db_job.progress_total_urls)) * 100),
                error={
                    "type": db_job.error_type or "unknown_error",
                    "message": db_job.error_message or "Crawl job failed"
                }
            )
        else:
            # queued or processing
            percentage = int((db_job.progress_urls_crawled / max(1, db_job.progress_total_urls)) * 100)
            return CrawlProgressResponse(
                status=db_job.status,
                job_id=job_id,
                urls_crawled=db_job.progress_urls_crawled,
                total_urls=db_job.progress_total_urls,
                percentage=percentage,
                estimated_time_remaining_seconds=(db_job.progress_total_urls - db_job.progress_urls_crawled) * db_job.delay
            )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in crawler status endpoint: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to check status: {str(e)}"
            }
        )


@router.get("/download/{filename}")
async def download_crawl_result(filename: str):
    """Download crawl results (Excel or JSONL)."""
    from fastapi.responses import FileResponse
    
    # Security: validate filename only contains safe characters
    if not all(c.isalnum() or c in '._-' for c in filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    
    # Construct file path - check multiple possible locations
    possible_paths = [
        os.path.join("crawl_results", filename),
        os.path.join("generated_docs", filename),
        filename
    ]
    
    file_path = None
    for path in possible_paths:
        if os.path.exists(path):
            file_path = path
            break
    
    if not file_path:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Determine media type
    if filename.endswith('.xlsx'):
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif filename.endswith('.xls'):
        media_type = "application/vnd.ms-excel"
    elif filename.endswith('.csv'):
        media_type = "text/csv"
    elif filename.endswith('.jsonl'):
        media_type = "application/jsonl"
    else:
        media_type = "application/octet-stream"
    
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename
    )


@router.get("/list")
async def list_crawl_jobs(
    user_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """List crawl jobs for a user."""
    try:
        query = db.query(DBCrawlJob)
        
        if user_id:
            query = query.filter(DBCrawlJob.user_id == user_id)
        
        total = query.count()
        jobs = query.order_by(DBCrawlJob.created_at.desc()).limit(limit).offset(offset).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "jobs": [
                {
                    "job_id": job.job_id,
                    "status": job.status,
                    "urls_total": job.progress_total_urls,
                    "urls_crawled": job.progress_urls_crawled,
                    "results_count": job.results_count,
                    "created_at": job.created_at,
                    "completed_at": job.completed_at
                }
                for job in jobs
            ]
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to list jobs: {str(e)}"
            }
        )
