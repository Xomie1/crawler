"""
Pydantic models for document generation API validation.
"""
from typing import List, Optional, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field, validator


class Party(BaseModel):
    """Party information for document generation."""
    name: str = Field(..., min_length=2, description="Party name")
    address: str = Field(..., min_length=5, description="Party address")
    role: Literal["party_a", "party_b"] = Field(..., description="Party role")

    class Config:
        schema_extra = {
            "example": {
                "name": "山田太郎",
                "address": "東京都渋谷区道玄坂1-2-3",
                "role": "party_a"
            }
        }


class DocumentOptions(BaseModel):
    """Optional document generation settings."""
    property_separation: Optional[bool] = False
    alimony: Optional[bool] = False
    children: Optional[bool] = False
    custom_date: Optional[str] = None  # YYYY-MM-DD format

    @validator('custom_date')
    def validate_date(cls, v):
        if v:
            try:
                datetime.strptime(v, '%Y-%m-%d')
            except ValueError:
                raise ValueError('custom_date must be in YYYY-MM-DD format')
        return v

    class Config:
        schema_extra = {
            "example": {
                "property_separation": True,
                "alimony": True,
                "children": False,
                "custom_date": "2025-01-27"
            }
        }


class DocumentGenerationRequest(BaseModel):
    """Request to generate a document."""
    document_type: Literal["prenuptial", "divorce"] = Field(..., description="Type of document")
    parties: List[Party] = Field(..., min_items=2, max_items=2, description="Exactly 2 parties required")
    options: Optional[DocumentOptions] = Field(default_factory=DocumentOptions)
    custom_values: Optional[Dict[str, str]] = {}
    metadata: Optional[Dict[str, Any]] = {}

    @validator('parties')
    def validate_parties(cls, v):
        if len(v) != 2:
            raise ValueError("Exactly 2 parties required")
        roles = [p.role for p in v]
        if not ('party_a' in roles and 'party_b' in roles):
            raise ValueError("Parties must have roles 'party_a' and 'party_b'")
        return v

    class Config:
        schema_extra = {
            "example": {
                "document_type": "prenuptial",
                "parties": [
                    {"name": "山田太郎", "address": "東京都渋谷区道玄坂1-2-3", "role": "party_a"},
                    {"name": "佐藤花子", "address": "東京都新宿区新宿1-1-1", "role": "party_b"}
                ],
                "options": {
                    "property_separation": True,
                    "alimony": True,
                    "children": False
                },
                "metadata": {
                    "user_id": "user123",
                    "notes": "Client request"
                }
            }
        }


# Response Models

class DocumentGenerationResponse(BaseModel):
    """Response after submitting document generation request."""
    status: str = "queued"
    job_id: str
    message: str
    estimated_processing_time_seconds: int = 30
    check_status_url: str

    class Config:
        schema_extra = {
            "example": {
                "status": "queued",
                "job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "message": "Document generation queued successfully",
                "estimated_processing_time_seconds": 30,
                "check_status_url": "/api/documents/status/ea6ee932-a244-47b8-bda6-30161e211f80"
            }
        }


class DocumentResult(BaseModel):
    """Generated document files and metadata."""
    pdf_path: str
    pdf_url: str
    docx_path: str
    docx_url: str
    generation_time_seconds: int
    file_sizes: Dict[str, int]  # {pdf_bytes, docx_bytes}

    class Config:
        schema_extra = {
            "example": {
                "pdf_path": "/generated_docs/prenuptial_20250127_154530.pdf",
                "pdf_url": "/api/documents/download/prenuptial_20250127_154530.pdf",
                "docx_path": "/generated_docs/prenuptial_20250127_154530.docx",
                "docx_url": "/api/documents/download/prenuptial_20250127_154530.docx",
                "generation_time_seconds": 28,
                "file_sizes": {"pdf_bytes": 524288, "docx_bytes": 262144}
            }
        }


class DocumentProgressResponse(BaseModel):
    """Status response for processing document."""
    status: Literal["processing", "queued"]
    job_id: str
    progress: Optional[Dict[str, Any]] = None
    queue_position: Optional[int] = None
    estimated_time_remaining_seconds: Optional[int] = None

    class Config:
        schema_extra = {
            "example": {
                "status": "processing",
                "job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "progress": {
                    "stage": "generating_docx",
                    "percentage": 50
                },
                "queue_position": 0,
                "estimated_time_remaining_seconds": 15
            }
        }


class DocumentCompletedResponse(BaseModel):
    """Status response for completed document."""
    status: str = "completed"
    job_id: str
    document_type: str
    result: DocumentResult
    created_at: datetime
    completed_at: datetime

    class Config:
        schema_extra = {
            "example": {
                "status": "completed",
                "job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "document_type": "prenuptial",
                "result": {
                    "pdf_path": "/generated_docs/prenuptial_20250127_154530.pdf",
                    "pdf_url": "/api/documents/download/prenuptial_20250127_154530.pdf",
                    "docx_path": "/generated_docs/prenuptial_20250127_154530.docx",
                    "docx_url": "/api/documents/download/prenuptial_20250127_154530.docx",
                    "generation_time_seconds": 28,
                    "file_sizes": {"pdf_bytes": 524288, "docx_bytes": 262144}
                },
                "created_at": "2025-01-27T15:45:30Z",
                "completed_at": "2025-01-27T15:45:58Z"
            }
        }


class DocumentFailedResponse(BaseModel):
    """Status response for failed document."""
    status: str = "failed"
    job_id: str
    error: Dict[str, str]
    failed_at: datetime

    class Config:
        schema_extra = {
            "example": {
                "status": "failed",
                "job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "error": {
                    "type": "validation_error",
                    "message": "Party address is too short"
                },
                "failed_at": "2025-01-27T15:45:35Z"
            }
        }


class DocumentStatusResponse(BaseModel):
    """Union response type for document status checks."""
    # Handled by discriminated union in endpoint
    pass


class DocumentListItem(BaseModel):
    """Single document item in list response."""
    job_id: str
    document_type: str
    status: str
    party_names: List[str]
    created_at: datetime
    completed_at: Optional[datetime]
    file_urls: Optional[Dict[str, str]] = None

    class Config:
        schema_extra = {
            "example": {
                "job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "document_type": "prenuptial",
                "status": "completed",
                "party_names": ["山田太郎", "佐藤花子"],
                "created_at": "2025-01-27T15:45:30Z",
                "completed_at": "2025-01-27T15:45:58Z",
                "file_urls": {
                    "pdf": "/api/documents/download/prenuptial_20250127_154530.pdf",
                    "docx": "/api/documents/download/prenuptial_20250127_154530.docx"
                }
            }
        }


class DocumentListResponse(BaseModel):
    """List of documents."""
    status: str = "success"
    total_count: int
    limit: int
    offset: int
    documents: List[DocumentListItem]


class EmailDocumentRequest(BaseModel):
    """Request to email generated documents."""
    recipient_email: str = Field(..., description="Email recipient")
    include_pdf: bool = True
    include_docx: bool = True
    message: Optional[str] = None
    sender_name: Optional[str] = "Legal Document Service"


class EmailDocumentResponse(BaseModel):
    """Response after queuing email."""
    status: str = "queued"
    message: str
    email_job_id: str
    estimated_delivery_minutes: int = 2


class ErrorResponse(BaseModel):
    """Generic error response."""
    status: str = "error"
    error_type: str
    message: str
    details: Optional[List[str]] = None
    request_id: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "status": "error",
                "error_type": "validation_error",
                "message": "Invalid document data",
                "details": [
                    "parties[0].name is required",
                    "document_type must be 'prenuptial' or 'divorce'"
                ],
                "request_id": "req_123456"
            }
        }

# ============ PHASE 1 CRAWLER MODELS ============

class CrawlUrlItem(BaseModel):
    """Single URL to crawl with optional company name."""
    url: str = Field(..., min_length=5, description="URL to crawl")
    company_name: Optional[str] = Field(None, description="Optional company name")


class CrawlJobSubmitRequest(BaseModel):
    """Request to submit a crawl job."""
    urls: List[CrawlUrlItem] = Field(..., min_items=1, description="List of URLs to crawl")
    user_id: Optional[str] = None
    timeout: int = Field(30, ge=5, le=300)
    robots_policy: Literal["respect", "ignore"] = "respect"
    use_playwright: bool = True
    delay: int = Field(10, ge=0, le=60)
    
    class Config:
        schema_extra = {
            "example": {
                "urls": [
                    {"url": "https://example.com", "company_name": "Example Corp"},
                    {"url": "https://another.com"}
                ],
                "timeout": 30,
                "robots_policy": "respect",
                "use_playwright": True,
                "delay": 10
            }
        }


class CrawlProgressResponse(BaseModel):
    """Status response for processing crawl."""
    status: Literal["queued", "processing", "completed", "failed"]
    job_id: str
    urls_crawled: int
    total_urls: int
    percentage: int
    estimated_time_remaining_seconds: Optional[int] = None
    error: Optional[Dict[str, str]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "status": "processing",
                "job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "urls_crawled": 45,
                "total_urls": 100,
                "percentage": 45,
                "estimated_time_remaining_seconds": 300
            }
        }


class CrawlResultItem(BaseModel):
    """Single crawl result."""
    url: str
    status: str
    title: Optional[str] = None
    description: Optional[str] = None
    content_type: Optional[str] = None
    forms_detected: int = 0
    links_found: int = 0
    error: Optional[str] = None
    crawled_at: Optional[str] = None


class CrawlCompletedResponse(BaseModel):
    """Status response for completed crawl."""
    status: Literal["completed"]
    job_id: str
    urls_crawled: int
    total_urls: int
    successful: int
    failed: int
    results_excel_url: Optional[str] = None
    results_jsonl_url: Optional[str] = None
    created_at: datetime
    completed_at: datetime
    processing_time_seconds: int
    
    class Config:
        schema_extra = {
            "example": {
                "status": "completed",
                "job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "urls_crawled": 100,
                "total_urls": 100,
                "successful": 95,
                "failed": 5,
                "results_excel_url": "/api/crawler/download/crawl_results_20250127.xlsx",
                "results_jsonl_url": "/api/crawler/download/crawl_results_20250127.jsonl",
                "created_at": "2025-01-27T10:00:00Z",
                "completed_at": "2025-01-27T10:15:30Z",
                "processing_time_seconds": 930
            }
        }


# ============== PHASE 3: FORM SUBMISSION ==============

class FormFieldData(BaseModel):
    """Form field to submit."""
    name: str = Field(..., description="Field name")
    value: str = Field(..., description="Field value")

    class Config:
        schema_extra = {
            "example": {
                "name": "email",
                "value": "contact@company.com"
            }
        }


class FormSubmissionJobRequest(BaseModel):
    """Request to submit forms on target URLs."""
    job_name: str = Field(..., min_length=3, description="Name of the submission job")
    source_crawl_job_id: Optional[str] = None  # For submitting forms found during crawl
    target_urls: List[str] = Field(..., min_items=1, description="URLs where to submit forms")
    
    # Form data to submit
    email: str = Field(..., description="Email address to submit")
    name: Optional[str] = None
    phone: Optional[str] = None
    company: Optional[str] = None
    message: Optional[str] = None
    additional_fields: Optional[Dict[str, str]] = {}
    
    # Submission settings
    submit_method: Literal["auto", "post", "browser"] = "auto"
    use_playwright: bool = True
    ignore_captcha: bool = False
    timeout: int = 30
    delay: int = 5

    class Config:
        schema_extra = {
            "example": {
                "job_name": "Q1 Form Outreach",
                "target_urls": ["https://example1.com/contact", "https://example2.jp/inquiry"],
                "email": "sales@ourcompany.com",
                "name": "Sales Team",
                "phone": "+1-555-0100",
                "company": "Our Company Inc",
                "message": "Interested in partnership opportunities",
                "submit_method": "auto",
                "use_playwright": True,
                "ignore_captcha": False,
                "timeout": 30,
                "delay": 5
            }
        }


class FormSubmissionResponse(BaseModel):
    """Response after submitting form submission job."""
    status: str
    job_id: str
    message: str
    job_name: str
    total_urls: int
    check_status_url: str

    class Config:
        schema_extra = {
            "example": {
                "status": "queued",
                "job_id": "form-job-uuid-here",
                "message": "Form submission job queued",
                "job_name": "Q1 Form Outreach",
                "total_urls": 5,
                "check_status_url": "/api/forms/status/form-job-uuid-here"
            }
        }


class FormSubmissionProgressResponse(BaseModel):
    """Real-time progress of form submissions."""
    status: str  # 'queued', 'in-progress', 'completed', 'failed'
    job_id: str
    job_name: str
    total_urls: int
    successful_submissions: int
    failed_submissions: int
    captcha_detected: int
    progress_percentage: float
    error_message: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "status": "in-progress",
                "job_id": "form-job-uuid",
                "job_name": "Q1 Form Outreach",
                "total_urls": 50,
                "successful_submissions": 30,
                "failed_submissions": 5,
                "captcha_detected": 15,
                "progress_percentage": 60.0
            }
        }


class FormSubmissionCompletedResponse(BaseModel):
    """Final results of form submission job."""
    status: str
    job_id: str
    job_name: str
    total_urls: int
    successful_submissions: int
    failed_submissions: int
    captcha_detected: int
    success_rate: float
    processing_time_seconds: int
    created_at: datetime
    completed_at: datetime

    class Config:
        schema_extra = {
            "example": {
                "status": "completed",
                "job_id": "form-job-uuid",
                "job_name": "Q1 Form Outreach",
                "total_urls": 50,
                "successful_submissions": 35,
                "failed_submissions": 5,
                "captcha_detected": 10,
                "success_rate": 70.0,
                "processing_time_seconds": 600,
                "created_at": "2025-01-27T10:00:00Z",
                "completed_at": "2025-01-27T10:10:00Z"
            }
        }


# ============== PHASE 2: EMAIL CAMPAIGNS ==============

class EmailRecipient(BaseModel):
    """Email recipient information."""
    email: str = Field(..., description="Recipient email address")
    name: Optional[str] = None
    company_name: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "email": "contact@example.com",
                "name": "John Doe",
                "company_name": "Example Corp"
            }
        }


class EmailCampaignSubmitRequest(BaseModel):
    """Request to start an email campaign."""
    campaign_name: str = Field(..., min_length=3, description="Name of the campaign")
    campaign_type: Literal["bulk_from_crawl", "custom"] = Field(..., description="Campaign type")
    source_crawl_job_id: Optional[str] = None  # For bulk_from_crawl type
    recipients: List[EmailRecipient] = Field(..., description="List of recipients")
    
    # Email configuration
    sender_email: str = Field(..., description="Sender email address")
    sender_name: Optional[str] = None
    subject_template: str = Field(..., description="Email subject (can have {{variables}})")
    message_template: str = Field(..., description="Email message (can have {{variables}})")
    reply_to_email: Optional[str] = None
    
    # Campaign settings
    skip_duplicates: bool = True
    rate_limit_per_hour: int = 10
    dry_run: bool = False

    class Config:
        schema_extra = {
            "example": {
                "campaign_name": "Q1 Outreach 2025",
                "campaign_type": "bulk_from_crawl",
                "source_crawl_job_id": "ea6ee932-a244-47b8-bda6-30161e211f80",
                "recipients": [
                    {"email": "contact@company1.com", "name": "John Smith", "company_name": "Company 1"},
                    {"email": "info@company2.jp", "name": "田中太郎", "company_name": "Company 2"}
                ],
                "sender_email": "sales@yourcompany.com",
                "sender_name": "Sales Team",
                "subject_template": "Hi {{name}}, check out our services",
                "message_template": "We help companies like {{company_name}} grow their business...",
                "skip_duplicates": True,
                "rate_limit_per_hour": 10,
                "dry_run": False
            }
        }


class EmailCampaignResponse(BaseModel):
    """Response after submitting email campaign."""
    status: str
    job_id: str
    message: str
    campaign_name: str
    total_recipients: int
    dry_run: bool
    check_status_url: str

    class Config:
        schema_extra = {
            "example": {
                "status": "queued",
                "job_id": "ca7ff043-b355-48c9-cee7-40272f311f91",
                "message": "Campaign queued for sending",
                "campaign_name": "Q1 Outreach 2025",
                "total_recipients": 50,
                "dry_run": False,
                "check_status_url": "/api/email/status/ca7ff043-b355-48c9-cee7-40272f311f91"
            }
        }


class EmailProgressResponse(BaseModel):
    """Real-time progress update for email campaign."""
    status: str  # 'queued', 'in-progress', 'completed', 'failed'
    job_id: str
    campaign_name: str
    emails_sent: int
    emails_failed: int
    emails_bounced: int
    total_recipients: int
    progress_percentage: float
    error_message: Optional[str] = None
    dry_run: bool

    class Config:
        schema_extra = {
            "example": {
                "status": "in-progress",
                "job_id": "ca7ff043-b355-48c9-cee7-40272f311f91",
                "campaign_name": "Q1 Outreach 2025",
                "emails_sent": 25,
                "emails_failed": 2,
                "emails_bounced": 0,
                "total_recipients": 50,
                "progress_percentage": 54.0,
                "dry_run": False
            }
        }


class EmailCompletedResponse(BaseModel):
    """Response after email campaign completes."""
    status: str
    job_id: str
    campaign_name: str
    emails_sent: int
    emails_failed: int
    emails_bounced: int
    total_recipients: int
    processing_time_seconds: int
    dry_run: bool
    created_at: datetime
    completed_at: datetime

    class Config:
        schema_extra = {
            "example": {
                "status": "completed",
                "job_id": "ca7ff043-b355-48c9-cee7-40272f311f91",
                "campaign_name": "Q1 Outreach 2025",
                "emails_sent": 48,
                "emails_failed": 2,
                "emails_bounced": 0,
                "total_recipients": 50,
                "processing_time_seconds": 180,
                "dry_run": False,
                "created_at": "2025-01-27T10:00:00Z",
                "completed_at": "2025-01-27T10:03:00Z"
            }
        }