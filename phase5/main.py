"""
FastAPI application entry point for Phase 5.
Document generation management UI and API.
"""
import os
import uuid
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging

from .database import init_db
from .api import documents, crawler, email_campaigns, form_submissions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Document Generation API - Phase 5",
    description="Management UI and API for legal document generation",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request ID middleware for debugging
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request ID to all responses for debugging."""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database on application startup."""
    logger.info("Starting up Document Generation API (Phase 5)...")
    init_db()
    logger.info("✅ Database initialized")


# Include API routes
app.include_router(documents.router)
app.include_router(crawler.router)
app.include_router(email_campaigns.router)
app.include_router(form_submissions.router)


# Root endpoint - redirect to docs
@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with links to API docs."""
    return """
    <html>
        <head>
            <title>Document Generation API - Phase 5</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                h1 { color: #333; }
                a { color: #0066cc; text-decoration: none; margin: 10px 0; display: inline-block; }
                a:hover { text-decoration: underline; }
                .section { margin: 20px 0; padding: 20px; background: #f5f5f5; border-radius: 5px; }
            </style>
        </head>
        <body>
            <h1>📄 Document Generation API - Phase 5</h1>
            <p>Legal document (prenuptial agreements, divorce settlements) generation service.</p>
            
            <div class="section">
                <h2>API Documentation</h2>
                <a href="/docs">📚 Interactive API Docs (Swagger)</a><br>
                <a href="/redoc">📖 Alternative Docs (ReDoc)</a>
            </div>
            
            <div class="section">
                <h2>Quick Links</h2>
                <a href="/form">📋 Document Generation Form</a><br>
                <a href="/api/documents/health">🏥 Health Check</a>
            </div>
            
            <div class="section">
                <h2>Endpoints Summary</h2>
                <ul>
                    <li><strong>POST /api/documents/generate</strong> - Submit document request</li>
                    <li><strong>GET /api/documents/status/{job_id}</strong> - Check generation status</li>
                    <li><strong>GET /api/documents/download/{filename}</strong> - Download PDF/DOCX</li>
                    <li><strong>GET /api/documents/list</strong> - List user's documents</li>
                    <li><strong>POST /api/documents/{job_id}/send-email</strong> - Email documents</li>
                </ul>
            </div>
            
            <div class="section">
                <h2>Status</h2>
                <p>Phase 5 Integration: In Progress</p>
                <p>Database: Initialized</p>
                <p>Queue System: Connected to pdf_queue (RQ)</p>
            </div>
        </body>
    </html>
    """


# HTML Form endpoint
@app.get("/form", response_class=HTMLResponse)
async def document_form():
    """Serve the document generation form."""
    return """
    <html>
        <head>
            <title>Generate Legal Document</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 600px; margin: 40px auto; }
                .form-group { margin: 20px 0; }
                label { display: block; margin-bottom: 5px; font-weight: bold; }
                input, select, textarea { 
                    width: 100%; 
                    padding: 8px; 
                    border: 1px solid #ddd; 
                    border-radius: 4px;
                    box-sizing: border-box;
                }
                button { 
                    background: #0066cc; 
                    color: white; 
                    padding: 10px 20px; 
                    border: none; 
                    border-radius: 4px; 
                    cursor: pointer;
                    font-size: 16px;
                }
                button:hover { background: #0052a3; }
                .status { margin-top: 20px; padding: 15px; background: #f5f5f5; border-radius: 4px; }
                .error { color: red; }
                .success { color: green; }
                .loading { display: none; }
                h1 { color: #333; }
                .section { margin: 30px 0; padding: 20px; border: 1px solid #ddd; border-radius: 4px; }
            </style>
        </head>
        <body>
            <h1>📄 Legal Document Generator</h1>
            
            <form id="documentForm">
                <div class="section">
                    <h2>Document Type</h2>
                    <div class="form-group">
                        <label>Document Type:</label>
                        <select name="document_type" required>
                            <option value="">-- Select --</option>
                            <option value="prenuptial">Prenuptial Agreement</option>
                            <option value="divorce">Divorce Settlement</option>
                        </select>
                    </div>
                </div>
                
                <div class="section">
                    <h2>Party A (First Party)</h2>
                    <div class="form-group">
                        <label>Name:</label>
                        <input type="text" name="party_a_name" placeholder="Full name" required>
                    </div>
                    <div class="form-group">
                        <label>Address:</label>
                        <input type="text" name="party_a_address" placeholder="Full address" required>
                    </div>
                </div>
                
                <div class="section">
                    <h2>Party B (Second Party)</h2>
                    <div class="form-group">
                        <label>Name:</label>
                        <input type="text" name="party_b_name" placeholder="Full name" required>
                    </div>
                    <div class="form-group">
                        <label>Address:</label>
                        <input type="text" name="party_b_address" placeholder="Full address" required>
                    </div>
                </div>
                
                <div class="section">
                    <h2>Options</h2>
                    <div class="form-group">
                        <label>
                            <input type="checkbox" name="property_separation">
                            Property Separation
                        </label>
                    </div>
                    <div class="form-group">
                        <label>
                            <input type="checkbox" name="alimony">
                            Alimony
                        </label>
                    </div>
                    <div class="form-group">
                        <label>
                            <input type="checkbox" name="children">
                            Children Custody
                        </label>
                    </div>
                </div>
                
                <button type="submit">Generate Document</button>
            </form>
            
            <div id="status" class="status" style="display: none;">
                <div id="statusMessage"></div>
                <div class="loading">
                    <p>⏳ Generating document...</p>
                    <p><strong>Job ID:</strong> <span id="jobId"></span></p>
                    <p><strong>Status:</strong> <span id="jobStatus">processing</span></p>
                </div>
                <div id="result" style="display: none;">
                    <p style="color: green;"><strong>✅ Document Generated!</strong></p>
                    <p>
                        <a href="#" id="downloadPdf" style="color: #0066cc; text-decoration: none; margin-right: 10px;">
                            📄 Download PDF
                        </a>
                        <a href="#" id="downloadDocx" style="color: #0066cc; text-decoration: none;">
                            📝 Download DOCX
                        </a>
                    </p>
                </div>
            </div>
            
            <script>
                const form = document.getElementById('documentForm');
                const statusDiv = document.getElementById('status');
                const statusMessage = document.getElementById('statusMessage');
                const resultDiv = document.getElementById('result');
                const loadingDiv = document.querySelector('.loading');
                
                form.addEventListener('submit', async (e) => {
                    e.preventDefault();
                    
                    const formData = new FormData(form);
                    
                    // Build request payload
                    const payload = {
                        document_type: formData.get('document_type'),
                        parties: [
                            {
                                name: formData.get('party_a_name'),
                                address: formData.get('party_a_address'),
                                role: 'party_a'
                            },
                            {
                                name: formData.get('party_b_name'),
                                address: formData.get('party_b_address'),
                                role: 'party_b'
                            }
                        ],
                        options: {
                            property_separation: formData.get('property_separation') === 'on',
                            alimony: formData.get('alimony') === 'on',
                            children: formData.get('children') === 'on'
                        },
                        metadata: {
                            user_id: 'user_' + Date.now()
                        }
                    };
                    
                    try {
                        // Submit request
                        const response = await fetch('/api/documents/generate', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(payload)
                        });
                        
                        const data = await response.json();
                        
                        if (!response.ok) {
                            statusMessage.innerHTML = '<p class="error">❌ Error: ' + (data.detail?.message || 'Failed to generate document') + '</p>';
                            statusDiv.style.display = 'block';
                            loadingDiv.style.display = 'none';
                            return;
                        }
                        
                        const jobId = data.job_id;
                        document.getElementById('jobId').textContent = jobId;
                        statusDiv.style.display = 'block';
                        loadingDiv.style.display = 'block';
                        resultDiv.style.display = 'none';
                        
                        // Poll for status
                        pollStatus(jobId);
                        
                    } catch (error) {
                        statusMessage.innerHTML = '<p class="error">❌ Error: ' + error.message + '</p>';
                        statusDiv.style.display = 'block';
                        loadingDiv.style.display = 'none';
                    }
                });
                
                async function pollStatus(jobId) {
                    const maxAttempts = 120; // 2 minutes max
                    let attempts = 0;
                    
                    const poll = async () => {
                        try {
                            const response = await fetch('/api/documents/status/' + jobId);
                            const data = await response.json();
                            
                            document.getElementById('jobStatus').textContent = data.status;
                            
                            if (data.status === 'completed') {
                                loadingDiv.style.display = 'none';
                                resultDiv.style.display = 'block';
                                
                                const pdfUrl = data.result.pdf_url;
                                const docxUrl = data.result.docx_url;
                                
                                document.getElementById('downloadPdf').href = pdfUrl;
                                document.getElementById('downloadDocx').href = docxUrl;
                                
                            } else if (data.status === 'failed') {
                                loadingDiv.style.display = 'none';
                                statusMessage.innerHTML = '<p class="error">❌ Error: ' + (data.error?.message || 'Unknown error') + '</p>';
                                
                            } else if (attempts < maxAttempts) {
                                attempts++;
                                setTimeout(poll, 2000); // Poll every 2 seconds
                            }
                        } catch (error) {
                            if (attempts < maxAttempts) {
                                attempts++;
                                setTimeout(poll, 2000);
                            }
                        }
                    };
                    
                    poll();
                }
            </script>
        </body>
    </html>
    """


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler."""
    request_id = getattr(request.state, 'request_id', 'unknown')
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "error_type": exc.detail.get("error_type", "http_error") if isinstance(exc.detail, dict) else "http_error",
            "message": exc.detail.get("message", exc.detail) if isinstance(exc.detail, dict) else str(exc.detail),
            "request_id": request_id
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler."""
    request_id = getattr(request.state, 'request_id', 'unknown')
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "error_type": "server_error",
            "message": "Internal server error",
            "request_id": request_id
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
