"""
Simple FastAPI server exposing HTTP endpoints for all phases:
- Phase 1: enqueue crawl jobs, list crawl results
- Phase 2: launch email campaigns
- Phase 3: launch form submissions
- Phase 4: enqueue PDF generation
- Phase 5: metrics & queue stats
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
import tempfile
import functools

from fastapi import FastAPI, HTTPException, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

# Simple in-memory cache for metrics
_cache = {}
_cache_times = {}

def cached(ttl_seconds: int = 30):
    """Simple cache decorator for API responses."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}:{str(kwargs)}"
            now = datetime.now()
            
            if cache_key in _cache:
                if now - _cache_times.get(cache_key, now) < timedelta(seconds=ttl_seconds):
                    return _cache[cache_key]
            
            result = await func(*args, **kwargs)
            _cache[cache_key] = result
            _cache_times[cache_key] = now
            return result
        
        return wrapper
    return decorator

from task_queue.queues import (
    crawl_queue,
    email_queue,
    form_queue,
    pdf_queue,
    get_queue_stats,
)
from task_queue.config import CRAWL_RETRY
from workers.crawl_worker import crawl_batch_job
from services.db_service import FormSubmissionDB
from services.email_campaign_manager import EmailCampaignManager
from services.form_submission_service import FormSubmissionService

app = FastAPI(title="Crawler Control Panel API")

# CORS so Next.js dev server can call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "web" / "templates"))

static_dir = BASE_DIR / "web" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok", "service": "crawler-api"}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Landing page linking to each phase."""
    return templates.TemplateResponse("index.html", {"request": request})


# ========== PHASE 1: CRAWL ==========


@app.post("/api/phase1/enqueue")
async def enqueue_crawls(urls: List[str]):
    """Enqueue a list of URLs for crawling."""
    if not urls:
        raise HTTPException(status_code=400, detail="No URLs provided")

    job_ids: List[str] = []
    for url in urls:
        job = crawl_queue.enqueue(
            "workers.crawl_worker.crawl_url_job",
            url=url,
            use_ai=True,
            ai_provider="groq",
            retry=CRAWL_RETRY,
        )
        job_ids.append(job.id)

    return {"queued": len(job_ids), "job_ids": job_ids}


@app.post("/api/phase1/enqueue_upload")
async def enqueue_upload(file: UploadFile = File(...)):
    """Enqueue URLs from uploaded file (XLSX, CSV, JSONL)."""
    try:
        import pandas as pd
        import json
        
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(suffix=Path(file.filename).suffix if file.filename else '.tmp', delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        urls = []
        company_names = []
        
        try:
            if file.filename.endswith('.jsonl'):
                # Parse JSONL
                with open(tmp_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            data = json.loads(line)
                            if 'url' in data:
                                urls.append(data['url'])
                                company_names.append(data.get('company_name'))
            elif file.filename.endswith(('.xlsx', '.xls')):
                # Parse Excel
                df = pd.read_excel(tmp_path)
                urls = df.iloc[:, 0].dropna().astype(str).tolist()
                if len(df.columns) > 1:
                    company_names = df.iloc[:, 1].fillna('').astype(str).tolist()
            elif file.filename.endswith('.csv'):
                # Parse CSV
                df = pd.read_csv(tmp_path)
                urls = df.iloc[:, 0].dropna().astype(str).tolist()
                if len(df.columns) > 1:
                    company_names = df.iloc[:, 1].fillna('').astype(str).tolist()
            else:
                raise HTTPException(status_code=400, detail="Unsupported file format. Use XLSX, CSV, or JSONL")
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        
        if not urls:
            raise HTTPException(status_code=400, detail="No URLs found in file")
        
        # Enqueue each URL
        job_ids = []
        for url in urls:
            job = crawl_queue.enqueue(
                "workers.crawl_worker.crawl_url_job",
                url=url,
                use_ai=True,
                ai_provider="groq",
                retry=CRAWL_RETRY,
            )
            job_ids.append(job.id)
        
        return {
            "queued": len(job_ids),
            "job_ids": job_ids,
            "parsed_count": len(urls),
            "preview": urls[:5]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/api/phase1/crawls")
async def list_crawls(
    base_url: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """List crawl results from the unified crawl_results table."""
    db = FormSubmissionDB()
    results = db.get_crawl_results(
        base_url=base_url,
        crawl_status=status,
        limit=limit,
        offset=offset,
    )
    return {"results": results}


@app.get("/phase1", response_class=HTMLResponse)
async def phase1_page(request: Request):
    """Web UI for Phase 1."""
    return templates.TemplateResponse("phase1.html", {"request": request})


# ========== PHASE 2: EMAIL ==========


@app.post("/api/phase2/email_campaign")
async def run_email_campaign(crawl_results_file: str = Form(...), dry_run: bool = Form(False)):
    """
    Run an email campaign from a crawl results JSONL file.
    In the UI this will typically point to an uploaded file path.
    """
    manager = EmailCampaignManager()
    summary = manager.run_campaign(crawl_results_file=crawl_results_file, dry_run=dry_run)
    return summary


@app.post("/api/phase2/email_from_file")
async def email_from_file(
    file: UploadFile = File(...),
    subject_template: str = Form(...),
    message_template: str = Form(...),
    dry_run: bool = Form(False),
    test_recipient: Optional[str] = Form(None),
):
    """Run email campaign from uploaded file with custom templates."""
    try:
        import pandas as pd
        import json
        
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(suffix=Path(file.filename).suffix if file.filename else '.tmp', delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        crawl_results = []
        
        try:
            if file.filename.endswith('.jsonl'):
                with open(tmp_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            crawl_results.append(json.loads(line))
            elif file.filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(tmp_path)
                crawl_results = df.to_dict('records')
            elif file.filename.endswith('.csv'):
                df = pd.read_csv(tmp_path)
                crawl_results = df.to_dict('records')
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        
        if not crawl_results:
            raise HTTPException(status_code=400, detail="No data found in file")
        
        # Filter to records with email
        targets = [r for r in crawl_results if r.get('email')]
        
        if not targets:
            return {"queued": 0, "skipped": len(crawl_results), "reason": "No records with emails"}
        
        # Enqueue email jobs
        job_ids = []
        for target in targets:
            job = email_queue.enqueue(
                "workers.email_worker.send_email_job",
                email=target.get('email'),
                company_name=target.get('company_name', 'Company'),
                subject_template=subject_template,
                message_template=message_template,
                test_recipient=test_recipient,
                dry_run=dry_run,
            )
            job_ids.append(job.id)
        
        return {
            "queued": len(job_ids),
            "skipped": len(crawl_results) - len(targets),
            "dry_run": dry_run,
            "preview": [t.get('email') for t in targets[:5]]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Email campaign setup failed: {str(e)}")


@app.get("/phase2", response_class=HTMLResponse)
async def phase2_page(request: Request):
    """Web UI for Phase 2."""
    return templates.TemplateResponse("phase2.html", {"request": request})


# ========== PHASE 3: FORM SUBMISSIONS ==========


@app.post("/api/phase3/submit_forms")
async def submit_forms_from_crawls(limit: int = Form(100)):
    """
    Submit forms in bulk using the latest crawl_results in DB.
    Skips records that already have emails.
    """
    db = FormSubmissionDB()
    crawls = db.get_crawl_results(limit=limit)

    service = FormSubmissionService()
    try:
        results = service.submit_bulk_inquiries(
            crawls,
            skip_with_email=True,
        )
    finally:
        service.close()

    return {"total": len(results), "results": results}


@app.post("/api/phase3/forms_from_file")
async def forms_from_file(
    file: UploadFile = File(...),
    message_choice: str = Form(...),
    custom_message: Optional[str] = Form(None),
):
    """Submit forms from uploaded file with message choice."""
    try:
        import pandas as pd
        import json
        
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(suffix=Path(file.filename).suffix if file.filename else '.tmp', delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        crawl_results = []
        
        try:
            if file.filename.endswith('.jsonl'):
                with open(tmp_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            crawl_results.append(json.loads(line))
            elif file.filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(tmp_path)
                crawl_results = df.to_dict('records')
            elif file.filename.endswith('.csv'):
                df = pd.read_csv(tmp_path)
                crawl_results = df.to_dict('records')
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        
        if not crawl_results:
            raise HTTPException(status_code=400, detail="No data found in file")
        
        # Filter to records with form URLs
        targets = [r for r in crawl_results if r.get('inquiry_form_url')]
        
        if not targets:
            return {"queued": 0, "skipped": len(crawl_results), "reason": "No records with form URLs"}
        
        # Enqueue form jobs
        job_ids = []
        message_template = custom_message if message_choice == 'custom' and custom_message else None
        
        for target in targets:
            job = form_queue.enqueue(
                "workers.form_worker.submit_form_job",
                form_url=target.get('inquiry_form_url'),
                company_name=target.get('company_name', 'Company'),
                message_template=message_template,
            )
            job_ids.append(job.id)
        
        return {
            "queued": len(job_ids),
            "skipped": len(crawl_results) - len(targets),
            "preview": [t.get('inquiry_form_url') for t in targets[:5]]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Form submission setup failed: {str(e)}")


@app.get("/phase3", response_class=HTMLResponse)
async def phase3_page(request: Request):
    """Web UI for Phase 3."""
    return templates.TemplateResponse("phase3.html", {"request": request})


# ========== CAMPAIGNS & ERROR TRACKING ==========


@app.get("/api/campaigns")
async def get_campaigns(limit: int = 20):
    """Get recent campaigns with per-phase progress (cached 30s)."""
    cache_key = f"campaigns:{limit}"
    now = datetime.now()
    
    if cache_key in _cache and now - _cache_times.get(cache_key, now) < timedelta(seconds=30):
        return _cache[cache_key]
    
    db = FormSubmissionDB()
    try:
        campaigns = db.get_campaigns(limit)
        result = {"campaigns": campaigns, "total": len(campaigns)}
        _cache[cache_key] = result
        _cache_times[cache_key] = now
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch campaigns: {str(e)}")


@app.post("/api/campaigns")
async def create_campaign(data: Dict[str, Any]):
    """Create a new campaign."""
    name = data.get("name", "Unnamed Campaign")
    notes = data.get("notes", "")
    db = FormSubmissionDB()
    try:
        campaign_id = db.create_campaign(name, notes)
        return {"campaign_id": campaign_id, "name": name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create campaign: {str(e)}")


@app.get("/api/errors")
async def get_errors(phase: str = "all", limit: int = 50):
    """Get recent errors from a specific phase."""
    db = FormSubmissionDB()
    try:
        errors = db.get_errors(phase, limit)
        return {"errors": errors, "total": len(errors)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch errors: {str(e)}")


@app.post("/api/errors/export")
async def export_errors(data: Dict[str, Any]):
    """Export errors as CSV."""
    phase = data.get("phase", "all")
    db = FormSubmissionDB()
    try:
        csv_data = db.export_errors_csv(phase)
        return {
            "filename": f"errors_{phase}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "data": csv_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export errors: {str(e)}")


# ========== PHASE 4: PDF GENERATION ==========


@app.post("/api/phase4/generate_pdf")
async def generate_pdf(document_input: Dict[str, Any]):
    """
    Enqueue a PDF generation job from JSON-like DocumentInput.
    This is meant to be called by the UI form for prenuptial/divorce.
    """
    job = pdf_queue.enqueue(
        "workers.pdf_worker.generate_pdf_job",
        document_input,
    )
    return {"job_id": job.id}


@app.get("/phase4", response_class=HTMLResponse)
async def phase4_page(request: Request):
    """Web UI for Phase 4: simple form for prenuptial/divorce input."""
    return templates.TemplateResponse("phase4.html", {"request": request})


# ========== PHASE 5: METRICS & MONITORING ==========


@app.get("/api/metrics/queues")
@cached(ttl_seconds=10)
async def queue_metrics():
    """Get RQ queue statistics (cached 10s)."""
    return get_queue_stats()


@app.get("/api/metrics/email")
@cached(ttl_seconds=30)
async def email_metrics():
    """Get email statistics from DB (cached 30s)."""
    db = FormSubmissionDB()
    return db.get_email_statistics()


@app.get("/api/metrics/forms")
@cached(ttl_seconds=30)
async def form_metrics():
    """Get form submission statistics from DB (cached 30s)."""
    db = FormSubmissionDB()
    return db.get_statistics()


@app.get("/api/phase1/export")
async def export_crawl_results():
    """Export crawl results as CSV."""
    try:
        db = CrawlResultDB()
        results = db.get_all_results()
        
        # Generate CSV
        import csv
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=['company_name', 'base_url', 'email', 'contact_form_url', 'industry', 'crawl_status', 'http_status', 'robots_allowed', 'last_crawled_at', 'error_message'])
        writer.writeheader()
        
        for result in results:
            writer.writerow({
                'company_name': result.get('company_name', ''),
                'base_url': result.get('base_url', ''),
                'email': result.get('email', ''),
                'contact_form_url': result.get('contact_form_url', ''),
                'industry': result.get('industry', ''),
                'crawl_status': result.get('crawl_status', ''),
                'http_status': result.get('http_status', ''),
                'robots_allowed': result.get('robots_allowed', ''),
                'last_crawled_at': result.get('last_crawled_at', ''),
                'error_message': result.get('error_message', ''),
            })
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=crawl_results.csv"}
        )
    except Exception as e:
        return {"error": str(e)}, 500


@app.get("/api/campaigns/export")
async def export_campaign_results():
    """Export campaign results as CSV."""
    try:
        db = CrawlResultDB()  # Using same DB for now
        campaigns = db.get_campaigns()
        
        import csv
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=['id', 'name', 'created_at', 'phase1_complete', 'phase2_complete', 'phase3_complete', 'phase4_complete', 'status'])
        writer.writeheader()
        
        for campaign in campaigns:
            writer.writerow({
                'id': campaign.get('id', ''),
                'name': campaign.get('name', ''),
                'created_at': campaign.get('created_at', ''),
                'phase1_complete': campaign.get('phase1_complete', 0),
                'phase2_complete': campaign.get('phase2_complete', 0),
                'phase3_complete': campaign.get('phase3_complete', 0),
                'phase4_complete': campaign.get('phase4_complete', 0),
                'status': campaign.get('status', ''),
            })
        
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=campaign_results.csv"}
        )
    except Exception as e:
        return {"error": str(e)}, 500


@app.get("/api/results/export")
async def export_all_results(phase: str = "all"):
    """Export all results or filter by phase."""
    try:
        db = CrawlResultDB()
        results = db.get_all_results()
        
        import csv
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=['phase', 'type', 'company_name', 'base_url', 'email', 'contact_form_url', 'status', 'timestamp'])
        writer.writeheader()
        
        for result in results:
            if phase != "all" and not str(result.get('phase', '')).startswith(phase):
                continue
            
            writer.writerow({
                'phase': result.get('phase', ''),
                'type': result.get('crawl_status', 'unknown'),
                'company_name': result.get('company_name', ''),
                'base_url': result.get('base_url', ''),
                'email': result.get('email', ''),
                'contact_form_url': result.get('contact_form_url', ''),
                'status': result.get('crawl_status', ''),
                'timestamp': result.get('last_crawled_at', ''),
            })
        
        filename = f"results_{'all' if phase == 'all' else phase}.csv"
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        return {"error": str(e)}, 500


@app.get("/phase5", response_class=HTMLResponse)
async def phase5_page(request: Request):
    """Web UI dashboard for Phase 5 metrics."""
    return templates.TemplateResponse("phase5.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000, log_level="info")
