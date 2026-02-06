"""
Phase 2: Email Campaign Worker
Processes email campaigns in RQ background job queue
"""
import re
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from rq import get_current_job
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from phase5.database import EmailCampaignJob, EmailSendLog, DATABASE_URL

logger = logging.getLogger(__name__)

# Setup database
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def process_email_campaign(job_id: str) -> Dict:
    """
    Process an email campaign job.
    Handles sending emails with rate limiting and duplicate prevention.
    
    Args:
        job_id: The campaign job ID
    
    Returns:
        Dictionary with campaign results
    """
    current_job = get_current_job()
    db = SessionLocal()
    
    try:
        logger.info(f"Processing email campaign: {job_id}")
        
        # Get campaign from database
        campaign = db.query(EmailCampaignJob).filter(
            EmailCampaignJob.job_id == job_id
        ).first()
        
        if not campaign:
            logger.error(f"Campaign not found: {job_id}")
            return {"status": "failed", "error": "Campaign not found"}
        
        # Update status to in-progress
        campaign.status = "in-progress"
        campaign.started_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Campaign: {campaign.campaign_name}")
        logger.info(f"Total recipients: {campaign.total_recipients}")
        logger.info(f"Dry run: {campaign.dry_run}")
        logger.info(f"Rate limit: {campaign.rate_limit_per_hour}/hour")
        
        # Parse recipients
        recipients = campaign.recipients_list
        if isinstance(recipients, str):
            recipients = json.loads(recipients)
        
        # Process emails
        emails_sent = 0
        emails_failed = 0
        emails_bounced = 0
        error_message = None
        
        # Calculate delay between emails for rate limiting
        seconds_per_email = 3600 / campaign.rate_limit_per_hour  # emails per hour
        
        for index, recipient in enumerate(recipients):
            try:
                # Check for duplicates if enabled
                if campaign.skip_duplicates:
                    existing = db.query(EmailSendLog).filter(
                        EmailSendLog.recipient_email == recipient.get("email"),
                        EmailSendLog.send_status == "sent"
                    ).first()
                    
                    if existing:
                        logger.info(f"Skipping duplicate email: {recipient.get('email')}")
                        continue
                
                # Prepare email content
                subject = _interpolate_template(
                    campaign.subject_template,
                    recipient
                )
                message = _interpolate_template(
                    campaign.message_template,
                    recipient
                )
                
                # Send email (dry run or real)
                if campaign.dry_run:
                    logger.info(f"[DRY RUN] Would send to: {recipient.get('email')}")
                    send_status = "sent"
                    error_msg = None
                    sendgrid_msg_id = f"dry-run-{index}"
                else:
                    # TODO: Integrate with SendGrid service when ready
                    # For now, simulate sending
                    send_status = "sent"
                    error_msg = None
                    sendgrid_msg_id = f"mock-{index}"
                    logger.info(f"Sending to: {recipient.get('email')}")
                
                # Log send attempt
                log_entry = EmailSendLog(
                    campaign_job_id=campaign.job_id,
                    recipient_email=recipient.get("email"),
                    recipient_name=recipient.get("name"),
                    company_name=recipient.get("company_name"),
                    subject=subject,
                    message_preview=message[:200] if message else None,
                    send_status=send_status,
                    error_message=error_msg,
                    sendgrid_message_id=sendgrid_msg_id,
                    sent_at=datetime.utcnow() if send_status == "sent" else None,
                )
                db.add(log_entry)
                
                if send_status == "sent":
                    emails_sent += 1
                else:
                    emails_failed += 1
                
                db.commit()
                
                # Update campaign progress
                campaign.emails_sent = emails_sent
                campaign.emails_failed = emails_failed
                campaign.emails_bounced = emails_bounced
                db.commit()
                
                # Rate limiting delay
                if index < len(recipients) - 1:
                    time.sleep(seconds_per_email)
            
            except Exception as e:
                logger.error(f"Error sending to {recipient.get('email')}: {str(e)}")
                emails_failed += 1
                
                # Log failed send
                log_entry = EmailSendLog(
                    campaign_job_id=campaign.job_id,
                    recipient_email=recipient.get("email"),
                    recipient_name=recipient.get("name"),
                    company_name=recipient.get("company_name"),
                    send_status="failed",
                    error_message=str(e),
                )
                db.add(log_entry)
                db.commit()
        
        # Mark campaign as completed
        campaign.status = "completed"
        campaign.emails_sent = emails_sent
        campaign.emails_failed = emails_failed
        campaign.emails_bounced = emails_bounced
        campaign.completed_at = datetime.utcnow()
        campaign.processing_time_seconds = int(
            (campaign.completed_at - campaign.started_at).total_seconds()
        )
        
        db.commit()
        
        logger.info(f"✅ Campaign completed!")
        logger.info(f"Sent: {emails_sent}, Failed: {emails_failed}, Bounced: {emails_bounced}")
        
        return {
            "status": "completed",
            "job_id": job_id,
            "emails_sent": emails_sent,
            "emails_failed": emails_failed,
            "emails_bounced": emails_bounced,
            "processing_time": campaign.processing_time_seconds
        }
    
    except Exception as e:
        logger.error(f"Campaign failed with error: {str(e)}")
        campaign = db.query(EmailCampaignJob).filter(
            EmailCampaignJob.job_id == job_id
        ).first()
        if campaign:
            campaign.status = "failed"
            campaign.error_message = str(e)
            campaign.completed_at = datetime.utcnow()
            db.commit()
        
        return {
            "status": "failed",
            "job_id": job_id,
            "error": str(e)
        }
    
    finally:
        db.close()


def _interpolate_template(template: str, context: Dict) -> str:
    """
    Replace template variables with actual values.
    Supports {{email}}, {{name}}, {{company_name}}, etc.
    """
    result = template
    
    for key, value in context.items():
        pattern = r'{{[\s]*' + re.escape(key) + r'[\s]*}}'
        result = re.sub(pattern, str(value) if value else '', result, flags=re.IGNORECASE)
    
    # Replace any remaining variables with empty string
    result = re.sub(r'{{[^}]*}}', '', result)
    
    return result
