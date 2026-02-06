#!/usr/bin/env python3
"""
Start DocGen Phase 5 Backend
Run from project root: python start_backend.py
"""

import os
import sys
import subprocess

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    print("🚀 Starting DocGen Phase 5 Backend...")
    
    # Check if phase5 directory exists
    if not os.path.exists("phase5"):
        print("❌ phase5 directory not found!")
        print("Run this script from the project root directory")
        sys.exit(1)
    
    # Check requirements.txt
    if not os.path.exists("requirements.txt"):
        print("❌ requirements.txt not found!")
        sys.exit(1)
    
    print("📦 Installing dependencies...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
        cwd="."
    )
    if result.returncode != 0:
        print("❌ Failed to install dependencies")
        sys.exit(1)
    
    # Start the server
    print("✅ Starting FastAPI server on http://localhost:8000")
    print("   Press Ctrl+C to stop\n")
    
    try:
        import uvicorn
        from phase5.main import app
        
        # Run with proper module path for reload to work
        uvicorn.run(
            "phase5.main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            reload_dirs=["phase5"]
        )
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're running from the project root directory")
        sys.exit(1)

if __name__ == "__main__":
    main()
