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


def generate_pdf_job(document_input_dict, output_dir="generated_docs", **kwargs):
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
            
    Returns:
        dict: paths to generated files or error info
    """
    job = get_current_job()
    job_id = job.id if job else 'local'
    
    try:
        logger.info(f"[{job_id}] Starting PDF generation for document type: {document_input_dict.get('document_type')}")
        # Ignore any unexpected keyword arguments such as 'timeout' that
        # might be passed by older enqueue calls or external schedulers.
        
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
        
        return {
            "status": "success",
            "pdf_path": result['pdf_path'],
            "docx_path": result['docx_path'],
            "document_type": result['document_type'],
            "generated_at": datetime.now().isoformat()
        }
        
    except ValueError as e:
        logger.error(f"[{job_id}] ❌ Validation error: {str(e)}")
        return {
            "status": "error",
            "error_type": "validation_error",
            "error_message": str(e),
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"[{job_id}] ❌ PDF generation failed: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error_type": "generation_error",
            "error_message": str(e),
            "timestamp": datetime.now().isoformat()
        }
