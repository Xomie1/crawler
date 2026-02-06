# -*- coding: utf-8 -*-
"""
PDF Worker
Handles PDF/Word document generation jobs using PDFdocsEngine
"""

import logging
import os
from datetime import datetime
from rq import get_current_job
from PDFdocsEngine.engine import DocumentEngine
from PDFdocsEngine.models import DocumentInput

logger = logging.getLogger(__name__)


def generate_pdf_job(document_input_dict, output_dir="generated_docs", job_id=None):
    """
    Generate PDF/Word document from form submission data.
    
    Args:
        document_input_dict: dict, containing:
            - document_type: "prenuptial" or "divorce"
            - parties: List of party dicts with name, address, role
            - options: Document options (property_separation, alimony, children)
            - custom_values: Custom key-value pairs for placeholders
            - include_signatures: bool (optional, default True)
            - include_witnesses: bool (optional, default False)
            - filename: str (optional)
        output_dir: Directory to save generated documents
        job_id: Optional job ID (if not provided, gets from RQ job)
            
    Returns:
        dict: paths to generated files or error info
    """
    rq_job = get_current_job()
    if not job_id:
        job_id = rq_job.id if rq_job else 'local'
    
    # Get database session to update progress
    db = None
    db_request = None
    try:
        from phase5.database import SessionLocal, DocumentGenerationRequest as DBDocumentGenerationRequest
        db = SessionLocal()
        db_request = db.query(DBDocumentGenerationRequest).filter(
            DBDocumentGenerationRequest.job_id == job_id
        ).first()
        
        if db_request:
            # Update status to processing
            db_request.status = "processing"
            db_request.started_at = datetime.utcnow()
            db.commit()
            logger.info(f"[{job_id}] Updated database: status=processing")
    except Exception as db_err:
        logger.warning(f"[{job_id}] Could not update database: {db_err}")
        db = None
    
    try:
        logger.info(f"[{job_id}] Starting PDF generation for document type: {document_input_dict.get('document_type')}")
        
        # Validate input
        document_input = DocumentInput.from_json(document_input_dict)
        
        # Initialize engine
        engine = DocumentEngine(output_dir=output_dir)
        
        # Generate document
        result = engine.generate(document_input_dict)
        
        logger.info(
            f"[{job_id}] ✅ PDF generated successfully\n"
            f"  PDF: {result['pdf_path']}\n"
            f"  DOCX: {result['docx_path']}"
        )
        
        # Update database with completion
        if db and db_request:
            db_request.status = "completed"
            db_request.completed_at = datetime.utcnow()
            db_request.pdf_path = result['pdf_path']
            db_request.docx_path = result.get('docx_path')
            
            # Calculate file sizes
            try:
                if os.path.exists(result['pdf_path']):
                    db_request.pdf_bytes = os.path.getsize(result['pdf_path'])
                if result.get('docx_path') and os.path.exists(result['docx_path']):
                    db_request.docx_bytes = os.path.getsize(result['docx_path'])
            except Exception:
                pass
            
            if db_request.started_at:
                db_request.processing_time_seconds = int(
                    (db_request.completed_at - db_request.started_at).total_seconds()
                )
            db.commit()
            logger.info(f"[{job_id}] Updated database: status=completed")
        
        return {
            "status": "success",
            "pdf_path": result['pdf_path'],
            "docx_path": result['docx_path'],
            "document_type": result['document_type'],
            "generated_at": datetime.now().isoformat()
        }
        
    except ValueError as e:
        logger.error(f"[{job_id}] ❌ Validation error: {str(e)}")
        
        # Update database with error
        if db and db_request:
            db_request.status = "failed"
            db_request.error_type = "validation_error"
            db_request.error_message = str(e)
            db_request.completed_at = datetime.utcnow()
            db.commit()
        
        return {
            "status": "error",
            "error_type": "validation_error",
            "error_message": str(e),
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"[{job_id}] ❌ PDF generation failed: {str(e)}", exc_info=True)
        
        # Update database with error
        if db and db_request:
            db_request.status = "failed"
            db_request.error_type = "generation_error"
            db_request.error_message = str(e)
            db_request.completed_at = datetime.utcnow()
            db.commit()
        
        return {
            "status": "error",
            "error_type": "generation_error",
            "error_message": str(e),
            "timestamp": datetime.now().isoformat()
        }
    finally:
        if db:
            db.close()