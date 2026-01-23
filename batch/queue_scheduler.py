#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Queue Scheduler - Master Entry Point
Loads URLs from Excel/CSV/JSONL and queues them for processing
"""

import argparse
import logging
import sys
import time
import json
from pathlib import Path
from datetime import datetime

from task_queue.queues import crawl_queue, get_queue_stats, clear_all_queues
from task_queue.config import CRAWL_RETRY
from batch.batch_crawler import load_urls_from_excel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def load_urls_from_jsonl(input_file, limit=None):
    """
    Load URLs from JSONL file.
    
    Args:
        input_file: Path to JSONL file
        limit: Limit number of URLs (optional)
        
    Returns:
        tuple: (urls list, company_names list)
    """
    urls = []
    company_names = []
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if limit and len(urls) >= limit:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    
                    # Support various field name conventions
                    url = data.get('url') or data.get('URL') or data.get('link')
                    company = data.get('company') or data.get('company_name') or data.get('name') or ''
                    
                    if not url:
                        logger.warning(f"Line {line_num}: No URL found in record")
                        continue
                    
                    urls.append(url)
                    company_names.append(company)
                
                except json.JSONDecodeError as e:
                    logger.warning(f"Line {line_num}: Invalid JSON - {e}")
                    continue
    
    except Exception as e:
        logger.error(f"Error reading JSONL file: {e}")
        raise
    
    return urls, company_names


def load_urls(input_file, limit=None):
    """
    Load URLs from Excel, CSV, or JSONL file.
    
    Args:
        input_file: Path to input file (.xlsx, .csv, or .jsonl)
        limit: Limit number of URLs (optional)
        
    Returns:
        tuple: (urls list, company_names list)
    """
    file_path = Path(input_file)
    
    if file_path.suffix.lower() == '.jsonl':
        return load_urls_from_jsonl(input_file, limit=limit)
    else:
        # Use existing Excel/CSV loader
        return load_urls_from_excel(input_file, limit=limit)


def queue_urls_for_crawling(
    input_file,
    mode='all',
    use_ai=True,
    ai_provider='groq',
    limit=None,
    dry_run=False,
    resume=False
):
    """
    Queue URLs from Excel/CSV/JSONL file.
    
    Args:
        input_file: Path to Excel/CSV/JSONL file
        mode: 'all', 'crawl-only', 'email-only', 'form-only'
        use_ai: Enable AI extraction
        ai_provider: AI provider (groq/openai)
        limit: Limit number of URLs (optional)
        dry_run: Don't actually queue, just show what would be queued
        resume: Skip already queued URLs
        
    Returns:
        dict: Summary of queued jobs
    """
    
    print("=" * 70)
    print("QUEUE SCHEDULER")
    print("=" * 70)
    print(f"Input file: {input_file}")
    print(f"Mode: {mode}")
    print(f"AI enabled: {use_ai} ({ai_provider})")
    print(f"Dry run: {dry_run}")
    print("=" * 70)
    
    # Load URLs
    urls, company_names = load_urls(input_file, limit=limit)
    
    if not urls:
        logger.error("No URLs found in input file")
        return {'error': 'No URLs found'}
    
    print(f"\n✔ Loaded {len(urls)} URLs from {input_file}")
    
    if dry_run:
        print("\n🏃 DRY RUN MODE - Not actually queuing")
        print(f"Would queue {len(urls)} URLs to crawl_queue")
        return {
            'dry_run': True,
            'would_queue': len(urls),
            'mode': mode
        }
    
    # Queue URLs
    print(f"\n📤 Queuing {len(urls)} URLs...")
    
    queued_count = 0
    skipped_count = 0
    error_count = 0
    
    start_time = time.time()
    
    for i, (url, company_name) in enumerate(zip(urls, company_names), 1):
        try:
            # Check if already queued (resume mode)
            if resume:
                # TODO: Check if URL already processed in DB
                pass
            
            # Queue the crawl job
            job = crawl_queue.enqueue(
                'workers.crawl_worker.crawl_url_job',
                url=url,
                company_name=company_name,
                use_ai=use_ai,
                ai_provider=ai_provider,
                retry=CRAWL_RETRY,
                job_timeout='10m'
            )
            
            queued_count += 1
            
            if i % 100 == 0 or i == len(urls):
                elapsed = time.time() - start_time
                rate = queued_count / elapsed if elapsed > 0 else 0
                print(f"  [{i}/{len(urls)}] Queued {queued_count} jobs ({rate:.1f} jobs/sec)")
        
        except Exception as e:
            logger.error(f"Failed to queue {url}: {e}")
            error_count += 1
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("QUEUEING COMPLETE")
    print("=" * 70)
    print(f"Total URLs: {len(urls)}")
    print(f"Queued: {queued_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    print(f"Time: {elapsed:.1f}s")
    print("=" * 70)
    
    # Show queue stats
    print("\nQueue Status:")
    stats = get_queue_stats()
    for queue_name, queue_stats in stats.items():
        print(f"  {queue_name}: {queue_stats['count']} jobs")
    
    print("\nNext Steps:")
    print("  1. Start workers: rq worker crawl_queue email_queue form_queue")
    print("  2. Monitor: python scripts/monitor_queues.py")
    print("  3. Dashboard: rq-dashboard (visit http://localhost:9181)")
    
    return {
        'total': len(urls),
        'queued': queued_count,
        'skipped': skipped_count,
        'errors': error_count,
        'elapsed': elapsed
    }


def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(
        description='Queue URLs for crawling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Queue all URLs for full processing (crawl → email → form)
  python -m batch.queue_scheduler.py input.xlsx
  
  # Queue from JSONL file
  python -m batch.queue_scheduler.py urls.jsonl
  
  # Dry run (don't actually queue)
  python -m batch.queue_scheduler.py input.xlsx --dry-run
  
  # Queue only first 100 URLs
  python -m batch.queue_scheduler.py input.xlsx --limit 100
  
  # Queue without AI extraction
  python -m batch.queue_scheduler.py input.xlsx --no-ai

  # Clear all queues (reset)
  python -m batch.queue_scheduler.py --clear-queues
  
Supported file formats:
  - .xlsx (Excel)
  - .csv (CSV)
  - .jsonl (JSON Lines, with 'url' and optional 'company'/'company_name' fields)
        """
    )
    
    parser.add_argument(
        'input_file',
        nargs='?',
        help='Input file with URLs (.xlsx, .csv, or .jsonl)'
    )
    
    parser.add_argument(
        '--mode',
        choices=['all', 'crawl-only', 'email-only', 'form-only'],
        default='all',
        help='Processing mode (default: all)'
    )
    
    parser.add_argument(
        '--no-ai',
        action='store_true',
        help='Disable AI extraction'
    )
    
    parser.add_argument(
        '--ai-provider',
        choices=['groq', 'openai'],
        default='groq',
        help='AI provider (default: groq)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of URLs to queue'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be queued without actually queuing'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Skip already processed URLs'
    )
    
    parser.add_argument(
        '--clear-queues',
        action='store_true',
        help='Clear all queues (for testing/reset)'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show queue statistics and exit'
    )
    
    args = parser.parse_args()
    
    # Handle special commands
    if args.clear_queues:
        print("⚠️  Clearing all queues...")
        clear_all_queues()
        return 0
    
    if args.stats:
        print("Queue Statistics:")
        stats = get_queue_stats()
        for queue_name, queue_stats in stats.items():
            print(f"\n{queue_name}:")
            for key, value in queue_stats.items():
                print(f"  {key}: {value}")
        return 0
    
    # Validate input file
    if not args.input_file:
        parser.print_help()
        return 1
    
    if not Path(args.input_file).exists():
        logger.error(f"Input file not found: {args.input_file}")
        return 1
    
    # Queue URLs
    result = queue_urls_for_crawling(
        input_file=args.input_file,
        mode=args.mode,
        use_ai=not args.no_ai,
        ai_provider=args.ai_provider,
        limit=args.limit,
        dry_run=args.dry_run,
        resume=args.resume
    )
    
    if 'error' in result:
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())