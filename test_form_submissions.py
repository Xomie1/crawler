#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script for form submissions with 5 URLs
"""

import logging
import sys
import io
import json
import csv
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from typing import List, Dict 

# Fix Windows console encoding for Unicode characters
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from services.form_submission_service import FormSubmissionService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Test form submissions with 5 URLs."""
    
    # Test URLs - Replace these with actual form URLs you want to test
    # Load from both sources
    def load_test_urls(jsonl_file: str, csv_file: str) -> List[Dict]:
        """Load URLs from JSONL (priority) then CSV."""
        test_urls = []
        
        # Try JSONL first
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        if data.get('crawlStatus') == 'success' and data.get('inquiryFormUrl'):
                            test_urls.append({
                                'form_url': data['inquiryFormUrl'],
                                'company_name': data.get('companyName', 'Unknown'),
                                'sender_email': 'test@example.com',
                                'sender_name': 'テスト太郎',
                                'phone': '03-1234-5678'
                            })
            logger.info(f"✅ Loaded {len(test_urls)} URLs from JSONL")
        except Exception as e:
            logger.warning(f"Could not load JSONL: {e}")
        
        # Fall back to CSV if JSONL empty
        if not test_urls:
            try:
                import csv
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        form_url = row.get('問い合わせURL') or row.get('inquiryFormUrl')
                        company = row.get('企業名') or row.get('companyName')
                        
                        if form_url and company:
                            test_urls.append({
                                'form_url': form_url,
                                'company_name': company,
                                'sender_email': 'test@example.com',
                                'sender_name': 'テスト太郎',
                                'phone': '03-1234-5678'
                            })
                logger.info(f"✅ Loaded {len(test_urls)} URLs from CSV")
            except Exception as e:
                logger.warning(f"Could not load CSV: {e}")
        
        return test_urls

    # Load crawl results
    test_urls = load_test_urls(
        'crawl_results_20251225_153940.jsonl',
        '離婚カウンセラー　お問合せあり_20251224.csv'
    )

    if not test_urls:
        logger.error("No URLs loaded from crawl results!")
        sys.exit(1)
    
    
    print("=" * 70)
    print("FORM SUBMISSION TEST")
    print("=" * 70)
    print(f"Testing {len(test_urls)} form(s)")
    print("=" * 70)
    print()
    
    # Initialize service
    service = FormSubmissionService(
        db_path="test_form_submissions.db",
        throttle_per_site=2.0,  # 2 seconds between submissions
        submission_log_file="test_submission_log.jsonl"
    )
    
    results = []
    
    try:
        for i, test_case in enumerate(test_urls, 1):
            print(f"\n[{i}/{len(test_urls)}] Testing: {test_case['form_url']}")
            print("-" * 70)
            
            try:
                result = service.submit_inquiry(
                    form_url=test_case['form_url'],
                    company_name=test_case['company_name'],
                    sender_email=test_case.get('sender_email', 'test@example.com'),
                    sender_name=test_case.get('sender_name', 'Test User'),
                    phone=test_case.get('phone', None),
                    message_body=test_case.get('message_body', None)
                )
                
                results.append(result)
                
                # Print result
                status = result['submission_status']
                if status == 'success':
                    print("[SUCCESS]")
                elif status == 'captcha_blocked':
                    print("[CAPTCHA BLOCKED] (queued)")
                elif status == 'failed':
                    print("[FAILED]")
                else:
                    print(f"[{status.upper()}]")
                
                if result.get('error'):
                    print(f"   Error: {result['error']}")
                
                if result.get('submission_result'):
                    submission = result['submission_result']
                    print(f"   Method: {submission.get('submission_method', 'unknown')}")
                    print(f"   HTTP Status: {submission.get('response_status', 'N/A')}")
                    if submission.get('verification_confidence') is not None:
                        print(f"   Confidence: {submission.get('verification_confidence', 0):.2f}")
                
            except Exception as e:
                logger.error(f"Error testing {test_case['form_url']}: {e}")
                import traceback
                traceback.print_exc()
                results.append({
                    'form_url': test_case['form_url'],
                    'submission_status': 'error',
                    'error': str(e)
                })
            
            print()
    
    finally:
        # Print summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        total = len(results)
        successful = sum(1 for r in results if r.get('submission_status') == 'success')
        failed = sum(1 for r in results if r.get('submission_status') == 'failed')
        captcha_blocked = sum(1 for r in results if r.get('submission_status') == 'captcha_blocked')
        errors = sum(1 for r in results if r.get('submission_status') == 'error')
        
        print(f"Total:      {total}")
        print(f"Successful: {successful} ({successful/total*100:.1f}%)")
        print(f"Failed:     {failed}")
        print(f"CAPTCHA:    {captcha_blocked}")
        print(f"Errors:     {errors}")
        print("=" * 70)
        
        # Database statistics
        try:
            stats = service.db.get_statistics()
            if stats:
                print("\nDatabase Statistics:")
                print(f"  Total submissions in DB: {stats.get('total', 0)}")
                print(f"  Success rate: {stats.get('success_rate', 0):.2f}%")
                print(f"  By status: {stats.get('by_status', {})}")
                print(f"  By mode: {stats.get('by_mode', {})}")
        except Exception as e:
            logger.error(f"Error getting DB stats: {e}")
        
        # Close service
        service.close()
        
        print("\n[COMPLETE] Test finished!")
        print(f"   Log file: test_submission_log.jsonl")
        print(f"   Database: test_form_submissions.db")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

