#!/usr/bin/env python
"""
Phase 5 FastAPI Application Launcher
Starts the document generation API server.
"""
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

if __name__ == "__main__":
    import uvicorn
    
    # Check dependencies
    try:
        import fastapi
        import sqlalchemy
        import redis
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Install with: pip install -r requirements.txt")
        sys.exit(1)
    
    # Launch FastAPI app
    print("🚀 Starting Document Generation API (Phase 5)...")
    print("📚 API Docs available at: http://localhost:8000/docs")
    print("📋 Document Form available at: http://localhost:8000/form")
    print()
    
    uvicorn.run(
        "phase5.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
