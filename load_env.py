"""
Load environment variables from .env file
Run this before batch_crawler.py if having issues
"""

import os
from pathlib import Path

def load_env_file():
    """Load .env file from current directory."""
    env_file = Path(__file__).parent / '.env'
    
    if not env_file.exists():
        print(f"❌ .env file not found at: {env_file}")
        return False
    
    print(f"📄 Loading .env from: {env_file}")
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Parse KEY=VALUE
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                # Remove quotes if present
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                
                os.environ[key] = value
                print(f"  ✓ Loaded: {key}")
    
    print("✅ Environment variables loaded successfully")
    return True

if __name__ == '__main__':
    load_env_file()
    
    # Verify critical variables
    print("\n🔍 Checking critical variables:")
    for var in ['GROQ_API_KEY', 'OPENAI_API_KEY', 'AI_PROVIDER']:
        value = os.getenv(var)
        if value:
            print(f"  ✓ {var}: {'*' * 10} (length: {len(value)})")
        else:
            print(f"  ✗ {var}: Not set")