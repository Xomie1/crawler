"""
Phase 1 Setup & Validation Script
Validates environment, dependencies, and runs quick tests.
"""

import sys
import os
import subprocess
from pathlib import Path
import importlib


class Colors:
    """ANSI color codes."""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(text):
    """Print section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}\n")


def print_success(text):
    """Print success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_warning(text):
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")


def print_error(text):
    """Print error message."""
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def check_python_version():
    """Check Python version."""
    print_header("Checking Python Version")
    
    version = sys.version_info
    version_str = f"{version.major}.{version.minor}.{version.micro}"
    
    if version.major >= 3 and version.minor >= 8:
        print_success(f"Python {version_str} (Requirement: Python 3.8+)")
        return True
    else:
        print_error(f"Python {version_str} - Requires Python 3.8 or higher")
        return False


def check_dependencies():
    """Check required dependencies."""
    print_header("Checking Dependencies")
    
    required = [
        ('requests', 'requests'),
        ('bs4', 'beautifulsoup4'),
        ('lxml', 'lxml'),
        ('pandas', 'pandas'),
        ('tqdm', 'tqdm'),
        ('google.auth', 'google-auth'),
    ]
    
    optional = [
        ('playwright', 'playwright'),
        ('dns', 'dnspython'),
    ]
    
    all_good = True
    
    print("Required Packages:")
    for module_name, package_name in required:
        try:
            importlib.import_module(module_name)
            print_success(f"{package_name} installed")
        except ImportError:
            print_error(f"{package_name} NOT installed")
            all_good = False
    
    print("\nOptional Packages:")
    for module_name, package_name in optional:
        try:
            importlib.import_module(module_name)
            print_success(f"{package_name} installed (optional)")
        except ImportError:
            print_warning(f"{package_name} not installed (optional)")
    
    if not all_good:
        print_error("\nMissing required dependencies!")
        print(f"\n{Colors.YELLOW}Run: pip install -r requirements.txt{Colors.END}")
        return False
    
    return True


def check_file_structure():
    """Check project file structure."""
    print_header("Checking File Structure")
    
    required_files = [
        'crawler/__init__.py',
        'crawler/engine.py',
        'crawler/fetcher.py',
        'crawler/parser.py',
        'crawler/robots.py',
        'crawler/storage.py',
        'crawler/email_extractor.py',
        'crawler/company_name_extractor.py',
        'crawler/industry_extractor.py',
        'crawler/contact_form_detector.py',
        'utils/__init__.py',
        'utils/logger.py',
        'utils/sheets.py',
        'test_crawler.py',
        'pattern_detector.py',
        'batch_crawler.py',
    ]
    
    all_good = True
    
    for file_path in required_files:
        if Path(file_path).exists():
            print_success(f"{file_path}")
        else:
            print_error(f"{file_path} NOT FOUND")
            all_good = False
    
    return all_good


def check_google_credentials():
    """Check Google Sheets credentials."""
    print_header("Checking Google Sheets Setup")
    
    if Path('credentials.json').exists():
        print_success("credentials.json found")
        print(f"{Colors.YELLOW}Remember to share your Google Sheet with the service account email{Colors.END}")
        return True
    else:
        print_warning("credentials.json NOT found")
        print("\nGoogle Sheets export will not work without credentials.")
        print("To set up:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create/select project")
        print("3. Enable Google Sheets API")
        print("4. Create Service Account")
        print("5. Download JSON key as 'credentials.json'")
        return False


def run_quick_test():
    """Run a quick test crawl."""
    print_header("Running Quick Test")
    
    print("Testing crawler on a single website...\n")
    
    try:
        # Import here to avoid errors if dependencies not installed
        from crawler.engine import CrawlerEngine
        
        test_url = "https://www.konanhanbai.jp/"
        
        print(f"Crawling: {test_url}")
        
        crawler = CrawlerEngine(
            root_url=test_url,
            crawl_settings={'timeout': 30},
            user_agent_policy="CrawlerBot/1.0 (Setup Test)",
            robots_policy="respect"
        )
        
        result = crawler.crawl()
        crawler.close()
        
        print("\nResults:")
        print(f"  Status: {result.get('crawl_status')}")
        print(f"  HTTP Status: {result.get('http_status')}")
        print(f"  Email: {result.get('email') or 'Not found'}")
        print(f"  Form: {result.get('inquiry_form_url') or 'Not found'}")
        print(f"  Company: {result.get('company_name') or 'Not found'}")
        print(f"  Industry: {result.get('industry') or 'Not found'}")
        
        if result.get('crawl_status') == 'success':
            print_success("\nQuick test PASSED!")
            return True
        else:
            print_warning(f"\nTest completed with status: {result.get('crawl_status')}")
            if result.get('error_message'):
                print(f"Error: {result.get('error_message')}")
            return False
            
    except Exception as e:
        print_error(f"\nQuick test FAILED: {e}")
        return False


def create_sample_input():
    """Create sample input file."""
    print_header("Creating Sample Input File")
    
    try:
        import pandas as pd
        
        sample_data = {
            'トップページURL': [
                'https://www.konanhanbai.jp/',
                'http://www.wedding-b.com/',
                'http://mcc-muguet.jp/',
            ],
            '法人名': [
                'コナン販売株式会社',
                '株式会社ウエディング・ベル',
                '株式会社エムシー・くりえーと',
            ],
            '小業種': [
                'ITコンサルティング',
                'ブライダル',
                'その他スクール',
            ],
            'IB管理番号': [
                '45',
                '123',
                '2534',
            ]
        }
        
        df = pd.DataFrame(sample_data)
        df.to_excel('sample_input.xlsx', index=False)
        
        print_success("Created sample_input.xlsx")
        print("Use this file to test the batch crawler:")
        print(f"  {Colors.YELLOW}python batch_crawler.py sample_input.xlsx --limit 3{Colors.END}")
        
        return True
        
    except Exception as e:
        print_error(f"Failed to create sample input: {e}")
        return False


def print_next_steps():
    """Print next steps."""
    print_header("Next Steps")
    
    print("1. Run Full Test Suite:")
    print(f"   {Colors.YELLOW}python test_crawler.py{Colors.END}")
    print()
    print("2. Analyze Website Patterns:")
    print(f"   {Colors.YELLOW}python pattern_detector.py test_results.json{Colors.END}")
    print()
    print("3. Run Batch Crawler (Test Mode):")
    print(f"   {Colors.YELLOW}python batch_crawler.py sample_input.xlsx --limit 3{Colors.END}")
    print()
    print("4. Run Production Crawl:")
    print(f"   {Colors.YELLOW}python batch_crawler.py your_input.xlsx{Colors.END}")
    print()
    print("5. Export to Google Sheets:")
    print(f"   {Colors.YELLOW}python batch_crawler.py your_input.xlsx --sheets-id YOUR_SHEET_ID{Colors.END}")
    print()


def main():
    """Main setup validation."""
    print(f"\n{Colors.BOLD}Phase 1 Crawler - Setup & Validation{Colors.END}")
    
    checks = []
    
    # Run all checks
    checks.append(("Python Version", check_python_version()))
    checks.append(("Dependencies", check_dependencies()))
    checks.append(("File Structure", check_file_structure()))
    checks.append(("Google Credentials", check_google_credentials()))
    
    # Summary
    print_header("Validation Summary")
    
    for name, passed in checks:
        if passed:
            print_success(f"{name}: OK")
        else:
            print_error(f"{name}: FAILED")
    
    all_passed = all(passed for _, passed in checks[:-1])  # Exclude Google creds (optional)
    
    if all_passed:
        print_success("\n✓ All required checks passed!")
        
        # Create sample input
        create_sample_input()
        
        # Run quick test
        test_passed = run_quick_test()
        
        if test_passed:
            print_success("\n✓ Setup validation COMPLETE!")
            print_next_steps()
        else:
            print_warning("\n⚠ Setup validation completed with warnings")
            print_next_steps()
    else:
        print_error("\n✗ Setup validation FAILED")
        print("\nPlease fix the issues above and run setup again.")
        sys.exit(1)


if __name__ == "__main__":
    main()