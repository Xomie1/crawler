"""
Phase 2: Email Campaigns API
Handles email campaign creation, execution, and tracking via SendGrid
"""
import uuid
import os
import json
import tempfile
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Query, UploadFile, File, Form
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from phase5.database import (
    SessionLocal,
    EmailCampaignJob as DBEmailCampaignJob,
    EmailSendLog as DBEmailSendLog,
    CrawlJob as DBCrawlJob,
)
from phase5.models import (
    EmailCampaignSubmitRequest,
    EmailCampaignResponse,
    EmailProgressResponse,
    EmailCompletedResponse,
)

router = APIRouter(prefix="/api/email", tags=["email"])


def get_db():
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/submit", response_model=EmailCampaignResponse, status_code=202)
async def submit_email_campaign(
    request: EmailCampaignSubmitRequest,
    user_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Submit a new email campaign.
    
    Supports:
    - Bulk campaigns from crawl results (Phase 1 integration)
    - Custom recipient lists
    - Template variables: {{email}}, {{name}}, {{company_name}}
    - Dry run mode for testing
    """
    try:
        job_id = str(uuid.uuid4())
        
        # If bulk_from_crawl, validate the crawl job exists
        if request.campaign_type == "bulk_from_crawl":
            if not request.source_crawl_job_id:
                raise HTTPException(
                    status_code=400,
                    detail="source_crawl_job_id required for bulk_from_crawl campaigns"
                )
            
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
        
        # Create campaign job record
        total_recipients = len(request.recipients)
        
        campaign = DBEmailCampaignJob(
            job_id=job_id,
            user_id=user_id,
            campaign_name=request.campaign_name,
            campaign_type=request.campaign_type,
            source_crawl_job_id=request.source_crawl_job_id,
            sender_email=request.sender_email,
            sender_name=request.sender_name or "Sales Team",
            subject_template=request.subject_template,
            message_template=request.message_template,
            reply_to_email=request.reply_to_email,
            recipients_list=[r.dict() for r in request.recipients],
            total_recipients=total_recipients,
            skip_duplicates=request.skip_duplicates,
            rate_limit_per_hour=request.rate_limit_per_hour,
            dry_run=request.dry_run,
            status="queued",
        )
        
        db.add(campaign)
        db.commit()
        db.refresh(campaign)
        
        # Queue the campaign for processing
        try:
            from rq import Queue
            from redis import Redis
            redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)
            email_queue = Queue('email_queue', connection=redis_conn)
            
            email_queue.enqueue(
                'workers.email_campaign_worker.process_email_campaign',
                job_id=job_id,
                timeout=3600
            )
        except Exception as queue_err:
            logger.warning(f"Could not queue email campaign: {queue_err}")
        
        return EmailCampaignResponse(
            status="queued",
            job_id=job_id,
            message="Campaign queued for sending" if not request.dry_run else "Dry run campaign queued",
            campaign_name=request.campaign_name,
            total_recipients=total_recipients,
            dry_run=request.dry_run,
            check_status_url=f"/api/email/status/{job_id}"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload-file", response_model=EmailCampaignResponse, status_code=202)
async def upload_file_and_create_campaign(
    file: UploadFile = File(...),
    user_id: Optional[str] = Form(None),
    campaign_name: str = Form(...),
    sender_email: str = Form(...),
    sender_name: Optional[str] = Form("Sales Team"),
    subject_template: str = Form(...),
    message_template: str = Form(...),
    reply_to_email: Optional[str] = Form(None),
    skip_duplicates: bool = Form(True),
    rate_limit_per_hour: int = Form(10),
    dry_run: bool = Form(False),
    db: Session = Depends(get_db)
):
    """
    Upload a file (xlsx, jsonl, or csv) and create an email campaign from it.
    
    File formats supported:
    - JSONL: Each line should have 'email', 'companyName' (or 'company_name'), and optionally 'name'
    - Excel/CSV: Should have columns: 'email', 'company' (or 'company_name', 'companyName'), and optionally 'name'
    """
    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="pandas not installed. Required for file processing."
        )
    
    try:
        # Validate file format
        if not file.filename.endswith(('.xlsx', '.xls', '.csv', '.jsonl')):
            raise HTTPException(
                status_code=400,
                detail="File must be .xlsx, .xls, .csv, or .jsonl"
            )
        
        # Read file content
        content = await file.read()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1])
        temp_file.write(content)
        temp_file.close()
        
        # Parse file based on format
        recipients = []
        
        if file.filename.endswith('.jsonl'):
            # Parse JSONL
            with open(temp_file.name, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            data = json.loads(line)
                            email = data.get('email') or data.get('Email') or data.get('EMAIL')
                            if not email:
                                continue
                            
                            recipients.append({
                                "email": email,
                                "name": data.get('name') or data.get('Name') or data.get('companyName') or data.get('company_name') or data.get('Company'),
                                "company_name": data.get('companyName') or data.get('company_name') or data.get('Company') or data.get('company')
                            })
                        except json.JSONDecodeError:
                            continue
        else:
            # Parse Excel/CSV
            if file.filename.endswith('.csv'):
                df = pd.read_csv(temp_file.name, encoding='utf-8-sig')
            else:
                df = pd.read_excel(temp_file.name)
            
            # Find email column
            email_col = None
            for col in ['email', 'Email', 'EMAIL', 'メール', 'mail']:
                if col in df.columns:
                    email_col = col
                    break
            
            if not email_col:
                raise HTTPException(
                    status_code=400,
                    detail="No email column found. Expected: 'email', 'Email', or 'EMAIL'"
                )
            
            # Find name and company columns
            name_col = None
            for col in ['name', 'Name', 'NAME', '名前', 'companyName', 'company_name']:
                if col in df.columns:
                    name_col = col
                    break
            
            company_col = None
            for col in ['company', 'Company', 'COMPANY', '会社名', 'companyName', 'company_name', 'Company Name']:
                if col in df.columns:
                    company_col = col
                    break
            
            # Build recipients list
            for idx, row in df.iterrows():
                email = row[email_col]
                if pd.isna(email) or not email:
                    continue
                
                recipients.append({
                    "email": str(email).strip(),
                    "name": str(row[name_col]).strip() if name_col and pd.notna(row[name_col]) else None,
                    "company_name": str(row[company_col]).strip() if company_col and pd.notna(row[company_col]) else None
                })
        
        # Clean up temp file
        os.unlink(temp_file.name)
        
        if not recipients:
            raise HTTPException(
                status_code=400,
                detail="No valid email addresses found in file"
            )
        
        # Create campaign job
        job_id = str(uuid.uuid4())
        
        campaign = DBEmailCampaignJob(
            job_id=job_id,
            user_id=user_id,
            campaign_name=campaign_name,
            campaign_type="custom",
            source_crawl_job_id=None,
            sender_email=sender_email,
            sender_name=sender_name or "Sales Team",
            subject_template=subject_template,
            message_template=message_template,
            reply_to_email=reply_to_email,
            recipients_list=recipients,
            total_recipients=len(recipients),
            skip_duplicates=skip_duplicates,
            rate_limit_per_hour=rate_limit_per_hour,
            dry_run=dry_run,
            status="queued",
        )
        
        db.add(campaign)
        db.commit()
        db.refresh(campaign)
        
        # Queue the campaign for processing
        try:
            from rq import Queue
            from redis import Redis
            redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)
            email_queue = Queue('email_queue', connection=redis_conn)
            
            email_queue.enqueue(
                'workers.email_campaign_worker.process_email_campaign',
                job_id=job_id,
                timeout=3600
            )
        except Exception as queue_err:
            logger.warning(f"Could not queue email campaign: {queue_err}")
        
        return EmailCampaignResponse(
            status="queued",
            job_id=job_id,
            message="Campaign queued for sending" if not dry_run else "Dry run campaign queued",
            campaign_name=campaign_name,
            total_recipients=len(recipients),
            dry_run=dry_run,
            check_status_url=f"/api/email/status/{job_id}"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{job_id}", response_model=EmailProgressResponse)
async def get_email_status(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Get real-time status of an email campaign.
    Returns progress updates while running, final summary when complete.
    """
    try:
        campaign = db.query(DBEmailCampaignJob).filter(
            DBEmailCampaignJob.job_id == job_id
        ).first()
        
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        
        # Calculate progress percentage
        if campaign.total_recipients > 0:
            progress_pct = ((campaign.emails_sent + campaign.emails_failed) / campaign.total_recipients) * 100
        else:
            progress_pct = 0.0
        
        return EmailProgressResponse(
            status=campaign.status,
            job_id=campaign.job_id,
            campaign_name=campaign.campaign_name,
            emails_sent=campaign.emails_sent,
            emails_failed=campaign.emails_failed,
            emails_bounced=campaign.emails_bounced,
            total_recipients=campaign.total_recipients,
            progress_percentage=min(progress_pct, 100.0),
            error_message=campaign.error_message,
            dry_run=campaign.dry_run
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/completed/{job_id}", response_model=EmailCompletedResponse)
async def get_email_results(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Get final results of a completed email campaign.
    Only available after campaign completes.
    """
    try:
        campaign = db.query(DBEmailCampaignJob).filter(
            DBEmailCampaignJob.job_id == job_id
        ).first()
        
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        
        if campaign.status != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Campaign not completed: {campaign.status}"
            )
        
        return EmailCompletedResponse(
            status=campaign.status,
            job_id=campaign.job_id,
            campaign_name=campaign.campaign_name,
            emails_sent=campaign.emails_sent,
            emails_failed=campaign.emails_failed,
            emails_bounced=campaign.emails_bounced,
            total_recipients=campaign.total_recipients,
            processing_time_seconds=campaign.processing_time_seconds or 0,
            dry_run=campaign.dry_run,
            created_at=campaign.created_at,
            completed_at=campaign.completed_at or datetime.utcnow()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_campaigns(
    user_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    List email campaigns with filtering and pagination.
    
    Query Parameters:
    - user_id: Filter by user (optional)
    - status: Filter by status ('queued', 'in-progress', 'completed', 'failed')
    - limit: Number of results (1-100)
    - offset: Pagination offset
    """
    try:
        query = db.query(DBEmailCampaignJob)
        
        if user_id:
            query = query.filter(DBEmailCampaignJob.user_id == user_id)
        
        if status:
            query = query.filter(DBEmailCampaignJob.status == status)
        
        total = query.count()
        campaigns = query.order_by(DBEmailCampaignJob.created_at.desc()).offset(offset).limit(limit).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "campaigns": [
                {
                    "job_id": c.job_id,
                    "campaign_name": c.campaign_name,
                    "status": c.status,
                    "total_recipients": c.total_recipients,
                    "emails_sent": c.emails_sent,
                    "emails_failed": c.emails_failed,
                    "dry_run": c.dry_run,
                    "created_at": c.created_at,
                    "completed_at": c.completed_at,
                }
                for c in campaigns
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/send-logs/{campaign_job_id}")
async def get_send_logs(
    campaign_job_id: str,
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get detailed send logs for a campaign.
    Useful for debugging delivery issues.
    """
    try:
        query = db.query(DBEmailSendLog).filter(
            DBEmailSendLog.campaign_job_id == campaign_job_id
        )
        
        if status:
            query = query.filter(DBEmailSendLog.send_status == status)
        
        total = query.count()
        logs = query.order_by(DBEmailSendLog.created_at.desc()).offset(offset).limit(limit).all()
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "logs": [
                {
                    "id": log.id,
                    "recipient_email": log.recipient_email,
                    "recipient_name": log.recipient_name,
                    "company_name": log.company_name,
                    "send_status": log.send_status,
                    "error_message": log.error_message,
                    "opened": log.opened,
                    "clicked": log.clicked,
                    "bounced": log.bounced,
                    "sent_at": log.sent_at,
                    "opened_at": log.opened_at,
                    "clicked_at": log.clicked_at,
                    "created_at": log.created_at,
                }
                for log in logs
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_email_statistics(
    user_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get email campaign statistics.
    Returns aggregated data across all campaigns for a user.
    """
    try:
        query = db.query(DBEmailCampaignJob)
        
        if user_id:
            query = query.filter(DBEmailCampaignJob.user_id == user_id)
        
        campaigns = query.all()
        
        total_recipients = sum(c.total_recipients for c in campaigns)
        total_sent = sum(c.emails_sent for c in campaigns)
        total_failed = sum(c.emails_failed for c in campaigns)
        total_bounced = sum(c.emails_bounced for c in campaigns)
        total_campaigns = len(campaigns)
        completed = sum(1 for c in campaigns if c.status == "completed")
        
        return {
            "total_campaigns": total_campaigns,
            "completed_campaigns": completed,
            "total_recipients": total_recipients,
            "total_sent": total_sent,
            "total_failed": total_failed,
            "total_bounced": total_bounced,
            "success_rate": (total_sent / total_recipients * 100) if total_recipients > 0 else 0,
            "failure_rate": (total_failed / total_recipients * 100) if total_recipients > 0 else 0,
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
