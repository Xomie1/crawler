"""
Database models and ORM setup for Phase 5.
"""
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Text, Boolean, JSON, BIGINT, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from uuid import uuid4

# Database URL from environment or default to SQLite
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./phase5.db")

# For SQLite, we need to create the database path if it doesn't exist
if DATABASE_URL.startswith("sqlite:///"):
    db_path = DATABASE_URL.replace("sqlite:///", "")
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class DocumentGenerationRequest(Base):
    """Tracks all document generation requests."""
    __tablename__ = "document_generation_requests"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    user_id = Column(String(255), index=True, nullable=True)
    document_type = Column(String(50), nullable=False)  # 'prenuptial', 'divorce'
    
    # Request data
    parties_json = Column(JSON, nullable=False)
    options_json = Column(JSON, nullable=True)
    custom_values_json = Column(JSON, nullable=True)
    metadata_json = Column(JSON, nullable=True)
    
    # Status tracking
    status = Column(String(50), nullable=False, index=True)  # 'queued', 'processing', 'completed', 'failed'
    error_type = Column(String(100), nullable=True)
    error_message = Column(Text, nullable=True)
    
    # File output
    pdf_path = Column(String(500), nullable=True)
    docx_path = Column(String(500), nullable=True)
    pdf_bytes = Column(BIGINT, nullable=True)
    docx_bytes = Column(BIGINT, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    processing_time_seconds = Column(BIGINT, nullable=True)
    
    def __repr__(self):
        return f"<DocumentGenerationRequest(job_id={self.job_id}, status={self.status})>"


class DocumentEmailLog(Base):
    """Tracks emails sent with generated documents."""
    __tablename__ = "document_email_logs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    document_job_id = Column(String(255), nullable=False, index=True)  # FK to DocumentGenerationRequest
    email_job_id = Column(String(255), nullable=True)  # RQ job ID for email
    recipient_email = Column(String(255), nullable=False, index=True)
    document_type = Column(String(50), nullable=True)
    
    # Email options
    include_pdf = Column(Boolean, default=True)
    include_docx = Column(Boolean, default=True)
    
    # Email status
    send_status = Column(String(50), nullable=True, index=True)  # 'sent', 'failed', 'bounced'
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    sent_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<DocumentEmailLog(document_job_id={self.document_job_id}, recipient={self.recipient_email})>"


class EmailCampaignJob(Base):
    """Tracks email campaign jobs (Phase 2)."""
    __tablename__ = "email_campaign_jobs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    user_id = Column(String(255), index=True, nullable=True)
    
    # Campaign settings
    campaign_name = Column(String(255), nullable=False)
    campaign_type = Column(String(50), nullable=False)  # 'bulk_from_crawl', 'custom'
    source_crawl_job_id = Column(String(255), nullable=True)  # FK to CrawlJob if from crawl results
    
    # Email configuration
    sender_email = Column(String(255), nullable=False)
    sender_name = Column(String(255), nullable=True)
    subject_template = Column(String(500), nullable=False)
    message_template = Column(Text, nullable=False)
    reply_to_email = Column(String(255), nullable=True)
    
    # Recipients data
    recipients_list = Column(JSON, nullable=False)  # List of recipient objects with email, name, company
    total_recipients = Column(Integer, default=0)
    
    # Campaign settings
    skip_duplicates = Column(Boolean, default=True)
    rate_limit_per_hour = Column(Integer, default=10)
    dry_run = Column(Boolean, default=False)
    
    # Status tracking
    status = Column(String(50), nullable=False, index=True)  # 'queued', 'in-progress', 'completed', 'failed'
    emails_sent = Column(Integer, default=0)
    emails_failed = Column(Integer, default=0)
    emails_bounced = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    processing_time_seconds = Column(BIGINT, nullable=True)
    
    def __repr__(self):
        return f"<EmailCampaignJob(job_id={self.job_id}, status={self.status})>"


class EmailSendLog(Base):
    """Logs individual email sends (Phase 2)."""
    __tablename__ = "email_send_logs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    campaign_job_id = Column(String(255), nullable=False, index=True)  # FK to EmailCampaignJob
    email_job_id = Column(String(255), nullable=True)  # RQ job ID
    
    # Recipient info
    recipient_email = Column(String(255), nullable=False, index=True)
    recipient_name = Column(String(255), nullable=True)
    company_name = Column(String(255), nullable=True)
    
    # Email content
    subject = Column(String(500), nullable=False)
    message_preview = Column(Text, nullable=True)
    
    # Send status
    send_status = Column(String(50), nullable=False, index=True)  # 'sent', 'failed', 'bounced', 'queued'
    error_message = Column(Text, nullable=True)
    sendgrid_message_id = Column(String(255), nullable=True)
    
    # Tracking
    opened = Column(Boolean, default=False)
    clicked = Column(Boolean, default=False)
    bounced = Column(Boolean, default=False)
    opened_at = Column(DateTime, nullable=True)
    clicked_at = Column(DateTime, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    sent_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<EmailSendLog(campaign={self.campaign_job_id}, recipient={self.recipient_email})>"


class CrawlJob(Base):
    """Tracks web crawling jobs (Phase 1)."""
    __tablename__ = "crawl_jobs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    user_id = Column(String(255), index=True, nullable=True)
    
    # Input data
    input_file_path = Column(String(500), nullable=True)  # Path to uploaded Excel file
    input_file_name = Column(String(255), nullable=True)  # Original filename
    urls_list = Column(JSON, nullable=False)  # List of URLs to crawl
    company_names = Column(JSON, nullable=True)  # Optional company names per URL
    
    # Crawler settings
    timeout = Column(Integer, default=30)
    robots_policy = Column(String(50), default="respect")
    user_agent = Column(String(255), default="CrawlerBot/1.0")
    delay = Column(Integer, default=10)
    use_playwright = Column(Boolean, default=True)
    
    # Status tracking
    status = Column(String(50), nullable=False, index=True)  # 'queued', 'processing', 'completed', 'failed'
    progress_urls_crawled = Column(Integer, default=0)
    progress_total_urls = Column(Integer, default=0)
    error_type = Column(String(100), nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Output data
    results_jsonl_path = Column(String(500), nullable=True)
    results_excel_path = Column(String(500), nullable=True)
    results_count = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    processing_time_seconds = Column(BIGINT, nullable=True)
    
    def __repr__(self):
        return f"<CrawlJob(job_id={self.job_id}, status={self.status})>"




class FormSubmissionJob(Base):
    """Tracks form submission jobs (Phase 3)."""
    __tablename__ = "form_submission_jobs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    user_id = Column(String(255), index=True, nullable=True)
    
    # Submission configuration
    job_name = Column(String(255), nullable=False)
    source_crawl_job_id = Column(String(255), nullable=True)  # FK to CrawlJob if from crawl results
    target_urls = Column(JSON, nullable=False)  # List of URLs to submit forms on
    
    # Form field values to submit
    form_data = Column(JSON, nullable=False)  # {email, phone, name, company, etc.}
    
    # Submission settings
    submit_method = Column(String(50), default="auto")  # 'post', 'browser', 'auto'
    use_playwright = Column(Boolean, default=True)
    ignore_captcha = Column(Boolean, default=False)
    timeout = Column(Integer, default=30)
    delay = Column(Integer, default=5)  # Delay between submissions
    
    # Status tracking
    status = Column(String(50), nullable=False, index=True)  # 'queued', 'processing', 'completed', 'failed'
    total_urls = Column(Integer, default=0)
    successful_submissions = Column(Integer, default=0)
    failed_submissions = Column(Integer, default=0)
    captcha_detected = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    processing_time_seconds = Column(BIGINT, nullable=True)
    
    def __repr__(self):
        return f"<FormSubmissionJob(job_id={self.job_id}, status={self.status})>"


class FormSubmissionLog(Base):
    """Logs individual form submission attempts (Phase 3)."""
    __tablename__ = "form_submission_logs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    job_id = Column(String(255), nullable=False, index=True)  # FK to FormSubmissionJob
    
    # Target information
    url = Column(String(500), nullable=False)
    form_action = Column(String(500), nullable=True)
    form_method = Column(String(10), nullable=True)
    
    # Form detection & submission
    form_type = Column(String(100), nullable=True)  # 'contact', 'inquiry', 'comment', etc.
    detected_fields = Column(JSON, nullable=True)  # Fields found in form
    submitted_fields = Column(JSON, nullable=True)  # Fields actually submitted
    
    # Submission result
    submission_status = Column(String(50), nullable=False, index=True)  # 'success', 'failed', 'captcha', 'not_found'
    status_code = Column(Integer, nullable=True)  # HTTP response code
    error_message = Column(Text, nullable=True)
    response_preview = Column(Text, nullable=True)  # First 500 chars of response
    
    # CAPTCHA handling
    captcha_detected = Column(Boolean, default=False)
    captcha_type = Column(String(50), nullable=True)  # 'reCAPTCHA', 'hCaptcha', etc.
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    submitted_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<FormSubmissionLog(url={self.url}, status={self.submission_status})>"


# Create indexes for common queries
Index('idx_user_created', DocumentGenerationRequest.user_id, DocumentGenerationRequest.created_at)
Index('idx_document_type', DocumentGenerationRequest.document_type)
Index('idx_email_status', DocumentEmailLog.send_status)
Index('idx_crawl_user_created', CrawlJob.user_id, CrawlJob.created_at)
Index('idx_crawl_status', CrawlJob.status)
Index('idx_campaign_user_created', EmailCampaignJob.user_id, EmailCampaignJob.created_at)
Index('idx_campaign_status', EmailCampaignJob.status)
Index('idx_email_log_campaign', EmailSendLog.campaign_job_id)
Index('idx_email_log_status', EmailSendLog.send_status)
Index('idx_form_submission_user', FormSubmissionJob.user_id, FormSubmissionJob.created_at)
Index('idx_form_submission_status', FormSubmissionJob.status)
Index('idx_form_log_job', FormSubmissionLog.job_id)
Index('idx_form_log_status', FormSubmissionLog.submission_status)


def init_db():
    """Initialize database and create all tables."""
    Base.metadata.create_all(bind=engine)
    print("✅ Database initialized successfully")


def get_db():
    """Dependency for FastAPI to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
