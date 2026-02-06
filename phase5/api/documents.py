"""
Document generation API endpoints for Phase 5.
"""
import os
import json
import uuid
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from rq import Queue
from redis import Redis

from ..models import (
    DocumentGenerationRequest,
    DocumentGenerationResponse,
    DocumentCompletedResponse,
    DocumentFailedResponse,
    DocumentProgressResponse,
    DocumentListResponse,
    DocumentListItem,
    EmailDocumentRequest,
    EmailDocumentResponse,
    ErrorResponse
)
from ..database import get_db, DocumentGenerationRequest as DBDocumentGenerationRequest, DocumentEmailLog as DBDocumentEmailLog

router = APIRouter(prefix="/api/documents", tags=["documents"])

# Initialize Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0, decode_responses=True)
pdf_queue = Queue('pdf_queue', connection=redis_conn)


@router.post("/generate", response_model=DocumentGenerationResponse, status_code=202)
async def generate_document(
    request: DocumentGenerationRequest,
    db: Session = Depends(get_db)
):
    """
    Submit a document generation request.
    
    Returns job_id to check status later.
    """
    try:
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Convert request to dict for RQ job
        document_input_dict = {
            "document_type": request.document_type,
            "parties": [p.dict() for p in request.parties],
            "options": request.options.dict() if request.options else {},
            "custom_values": request.custom_values or {},
        }
        
        # Queue the PDF generation job
        try:
            rq_job = pdf_queue.enqueue(
                'workers.pdf_worker.generate_pdf_job',
                document_input_dict,
                "generated_docs",  # output_dir
                job_id,  # job_id parameter (worker will also get it from RQ)
                job_id=job_id,  # RQ job_id
                timeout=600,  # 10 minutes
                result_ttl=3600,  # Keep result for 1 hour
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={
                    "error_type": "queue_error",
                    "message": f"Failed to queue document generation: {str(e)}"
                }
            )
        
        # Store request in database
        db_request = DBDocumentGenerationRequest(
            id=str(uuid.uuid4()),
            job_id=job_id,
            user_id=request.metadata.get("user_id") if request.metadata else None,
            document_type=request.document_type,
            parties_json=[p.dict() for p in request.parties],
            options_json=request.options.dict() if request.options else {},
            custom_values_json=request.custom_values or {},
            metadata_json=request.metadata or {},
            status="queued",
            created_at=datetime.utcnow()
        )
        db.add(db_request)
        db.commit()
        
        return DocumentGenerationResponse(
            status="queued",
            job_id=job_id,
            message="Document generation queued successfully",
            estimated_processing_time_seconds=30,
            check_status_url=f"/api/documents/status/{job_id}"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to process request: {str(e)}"
            }
        )


@router.get("/status/{job_id}")
async def get_document_status(
    job_id: str,
    db: Session = Depends(get_db)
):
    """
    Check the status of a document generation request.
    
    Returns different response based on status:
    - queued/processing: progress info
    - completed: result with PDF/DOCX URLs
    - failed: error details
    """
    try:
        # First try to find in database
        db_request = db.query(DBDocumentGenerationRequest).filter(
            DBDocumentGenerationRequest.job_id == job_id
        ).first()
        
        if not db_request:
            # Try to get from queue if not in database yet
            try:
                rq_job = pdf_queue.fetch_job(job_id)
                if not rq_job:
                    raise HTTPException(status_code=404, detail="Job not found")
                
                # Job is in queue but not in database yet
                if rq_job.is_queued:
                    return DocumentProgressResponse(
                        status="queued",
                        job_id=job_id,
                        queue_position=0,
                        estimated_time_remaining_seconds=30
                    )
                elif rq_job.is_started:
                    return DocumentProgressResponse(
                        status="processing",
                        job_id=job_id,
                        progress={"stage": "generating_document", "percentage": 50},
                        estimated_time_remaining_seconds=30
                    )
                elif rq_job.is_failed:
                    return DocumentFailedResponse(
                        status="failed",
                        job_id=job_id,
                        error={
                            "type": "generation_error",
                            "message": rq_job.exc_info or "Job failed"
                        },
                        failed_at=rq_job.ended_at or datetime.utcnow()
                    )
            except Exception as redis_err:
                # Redis might not be available, return queued status
                return DocumentProgressResponse(
                    status="queued",
                    job_id=job_id,
                    queue_position=0,
                    estimated_time_remaining_seconds=30
                )
        
        # Return status from database
        if db_request.status == "completed":
            return DocumentCompletedResponse(
                status="completed",
                job_id=job_id,
                document_type=db_request.document_type,
                result={
                    "pdf_path": db_request.pdf_path or "",
                    "pdf_url": f"/api/documents/download/{os.path.basename(db_request.pdf_path or '')}",
                    "docx_path": db_request.docx_path or "",
                    "docx_url": f"/api/documents/download/{os.path.basename(db_request.docx_path or '')}",
                    "generation_time_seconds": db_request.processing_time_seconds or 0,
                    "file_sizes": {
                        "pdf_bytes": db_request.pdf_bytes or 0,
                        "docx_bytes": db_request.docx_bytes or 0
                    }
                },
                created_at=db_request.created_at or datetime.utcnow(),
                completed_at=db_request.completed_at or datetime.utcnow()
            )
        elif db_request.status == "failed":
            return DocumentFailedResponse(
                status="failed",
                job_id=job_id,
                error={
                    "type": db_request.error_type or "unknown_error",
                    "message": db_request.error_message or "Document generation failed"
                },
                failed_at=db_request.completed_at or datetime.utcnow()
            )
        else:
            # queued or processing
            return DocumentProgressResponse(
                status=db_request.status,
                job_id=job_id,
                progress={"stage": "generating_document", "percentage": 50} if db_request.status == "processing" else None,
                queue_position=0,
                estimated_time_remaining_seconds=30
            )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in status endpoint: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to check status: {str(e)}"
            }
        )


@router.get("/download/{filename}")
async def download_document(filename: str):
    """
    Download a generated PDF or DOCX file.
    """
    from fastapi.responses import FileResponse
    
    # Security: validate filename only contains safe characters
    if not all(c.isalnum() or c in '._-' for c in filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    
    # Construct file path
    file_path = os.path.join("generated_docs", filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    # Determine media type
    if filename.endswith('.pdf'):
        media_type = "application/pdf"
    elif filename.endswith('.docx'):
        media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    else:
        media_type = "application/octet-stream"
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type=media_type
    )


@router.get("/list", response_model=DocumentListResponse)
async def list_documents(
    user_id: str = Query(..., description="User ID"),
    document_type: Optional[str] = Query(None, description="Filter by document type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    List all documents for a user with optional filtering.
    """
    try:
        query = db.query(DBDocumentGenerationRequest).filter(
            DBDocumentGenerationRequest.user_id == user_id
        )
        
        if document_type:
            query = query.filter(DBDocumentGenerationRequest.document_type == document_type)
        
        if status:
            query = query.filter(DBDocumentGenerationRequest.status == status)
        
        # Count total
        total_count = query.count()
        
        # Get paginated results
        documents = query.order_by(
            DBDocumentGenerationRequest.created_at.desc()
        ).offset(offset).limit(limit).all()
        
        items = []
        for doc in documents:
            file_urls = None
            if doc.status == "completed" and doc.pdf_path and doc.docx_path:
                file_urls = {
                    "pdf": f"/api/documents/download/{os.path.basename(doc.pdf_path)}",
                    "docx": f"/api/documents/download/{os.path.basename(doc.docx_path)}"
                }
            
            party_names = [p.get("name", "") for p in doc.parties_json] if doc.parties_json else []
            
            items.append(DocumentListItem(
                job_id=doc.job_id,
                document_type=doc.document_type,
                status=doc.status,
                party_names=party_names,
                created_at=doc.created_at,
                completed_at=doc.completed_at,
                file_urls=file_urls
            ))
        
        return DocumentListResponse(
            status="success",
            total_count=total_count,
            limit=limit,
            offset=offset,
            documents=items
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to list documents: {str(e)}"
            }
        )


@router.post("/send-email/{job_id}", response_model=EmailDocumentResponse, status_code=202)
async def send_document_email(
    job_id: str,
    request: EmailDocumentRequest,
    db: Session = Depends(get_db)
):
    """
    Email the generated documents to a recipient.
    Integrates with Phase 2 (SendGrid email service).
    """
    try:
        # Get document from database
        db_request = db.query(DBDocumentGenerationRequest).filter(
            DBDocumentGenerationRequest.job_id == job_id
        ).first()
        
        if not db_request:
            raise HTTPException(status_code=404, detail="Document not found")
        
        if db_request.status != "completed":
            raise HTTPException(
                status_code=400,
                detail="Cannot email incomplete documents"
            )
        
        # Queue email job (would integrate with Phase 2 sendgrid_email_service)
        email_job_id = str(uuid.uuid4())
        
        # TODO: Queue to email_queue when Phase 2 integration is ready
        # For now, just log in database
        email_log = DBDocumentEmailLog(
            id=str(uuid.uuid4()),
            document_job_id=job_id,
            email_job_id=email_job_id,
            recipient_email=request.recipient_email,
            document_type=db_request.document_type,
            include_pdf=request.include_pdf,
            include_docx=request.include_docx,
            send_status="queued",
            created_at=datetime.utcnow()
        )
        db.add(email_log)
        db.commit()
        
        return EmailDocumentResponse(
            status="queued",
            message="Email queued for sending",
            email_job_id=email_job_id,
            estimated_delivery_minutes=2
        )
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error_type": "server_error",
                "message": f"Failed to queue email: {str(e)}"
            }
        )


# Health check
@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Document Generation API (Phase 5)",
        "redis_connected": redis_conn.ping()
    }
