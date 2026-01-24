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

from task_queue.queues import crawl_queue, pdf_queue, get_queue_stats, clear_all_queues
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


def queue_urls(input_file, limit=None):
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


def queue_pdf_jobs_from_jsonl(
    input_file,
    limit=None,
    filter_status=None,
    dry_run=False
):
    """
    Queue PDF generation jobs from submission JSONL file.
    
    Args:
        input_file: Path to submission_log.jsonl
        limit: Limit number of jobs (optional)
        filter_status: Filter by status (e.g., 'success')
        dry_run: Don't actually queue
        
    Returns:
        dict: Summary of queued jobs
    """
    
    file_path = Path(input_file)
    
    if not file_path.exists():
        logger.error(f"❌ File not found: {input_file}")
        return {'error': f'File not found: {input_file}'}
    
    stats = {'total': 0, 'queued': 0, 'failed': 0, 'skipped': 0}
    
    logger.info(f"📖 Reading submissions from: {input_file}")
    
    start_time = time.time()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if limit and stats['total'] >= limit:
                break
            
            try:
                data = json.loads(line)
                stats['total'] += 1
                
                # Apply status filter
                if filter_status and data.get('status') != filter_status:
                    stats['skipped'] += 1
                    continue
                
                # Skip if already processed
                if data.get('pdf_generated'):
                    stats['skipped'] += 1
                    continue
                
                # Check if has required fields (support both formats)
                # Format 1: parties array
                if data.get('parties') and isinstance(data.get('parties'), list) and len(data.get('parties')) >= 2:
                    parties = data['parties']
                # Format 2: flat fields (party1_name, party2_name, etc.)
                elif data.get('party1_name') and data.get('party2_name'):
                    parties = [
                        {
                            'name': data['party1_name'],
                            'address': data.get('party1_address', ''),
                            'role': data.get('party1_role', 'party1')
                        },
                        {
                            'name': data['party2_name'],
                            'address': data.get('party2_address', ''),
                            'role': data.get('party2_role', 'party2')
                        }
                    ]
                else:
                    logger.debug(f"Line {line_num}: Missing required party fields, skipping")
                    stats['skipped'] += 1
                    continue
                
                if dry_run:
                    logger.info(f"Line {line_num}: [DRY RUN] Would queue PDF for {data.get('document_type')}")
                    stats['queued'] += 1
                    continue
                
                # Transform to document input format
                document_input = {
                    'document_type': data.get('document_type', 'prenuptial'),
                    'parties': parties,
                    'options': {
                        'property_separation': data.get('property_separation', False),
                        'alimony': data.get('alimony', False),
                        'children': data.get('children', False)
                    },
                    'custom_values': data.get('custom_values', {}),
                    'include_signatures': data.get('include_signatures', True),
                    'include_witnesses': data.get('include_witnesses', False),
                }
                
                # Enqueue job
                job = pdf_queue.enqueue(
                    'workers.pdf_worker.generate_pdf_job',
                    document_input,
                    job_timeout='10m'
                )
                
                stats['queued'] += 1
                
                if stats['queued'] % 100 == 0 or stats['queued'] == 1:
                    logger.info(f"  Queued {stats['queued']} PDF jobs...")
            
            except json.JSONDecodeError as e:
                logger.error(f"Line {line_num}: Invalid JSON - {e}")
                stats['failed'] += 1
            except Exception as e:
                logger.error(f"Line {line_num}: Failed to queue - {e}")
                stats['failed'] += 1
    
    elapsed = time.time() - start_time
    
    return {
        'total': stats['total'],
        'queued': stats['queued'],
        'skipped': stats['skipped'],
        'failed': stats['failed'],
        'elapsed': elapsed,
        'dry_run': dry_run
    }


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
  python -m batch.queue_scheduler input.xlsx
  
  # Queue from JSONL file
  python -m batch.queue_scheduler urls.jsonl
  
  # Dry run (don't actually queue)
  python -m batch.queue_scheduler input.xlsx --dry-run
  
  # Queue only first 100 URLs
  python -m batch.queue_scheduler input.xlsx --limit 100
  
  # Queue without AI extraction
  python -m batch.queue_scheduler input.xlsx --no-ai

  # Queue PDF generation jobs from submission JSONL
  python -m batch.queue_scheduler --queue-pdf submission_log.jsonl
  
  # Queue only successful submissions for PDF generation
  python -m batch.queue_scheduler --queue-pdf submission_log.jsonl --pdf-status success
  
  # Clear all queues (reset)
  python -m batch.queue_scheduler --clear-queues
  
  # Show queue statistics
  python -m batch.queue_scheduler --stats
  
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
    
    parser.add_argument(
        '--queue-pdf',
        metavar='JSONL_FILE',
        help='Queue PDF generation jobs from submission JSONL file'
    )
    
    parser.add_argument(
        '--pdf-status',
        default=None,
        help='Filter submissions by status when queuing PDFs (e.g., "success")'
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
    
    if args.queue_pdf:
        print("=" * 70)
        print("PDF QUEUE SCHEDULER")
        print("=" * 70)
        result = queue_pdf_jobs_from_jsonl(
            input_file=args.queue_pdf,
            limit=args.limit,
            filter_status=args.pdf_status,
            dry_run=args.dry_run
        )
        
        if 'error' in result:
            logger.error(result['error'])
            return 1
        
        print("\n" + "=" * 70)
        print("PDF QUEUEING COMPLETE")
        print("=" * 70)
        print(f"Total read:     {result['total']}")
        print(f"Queued:         {result['queued']} ✅")
        print(f"Skipped:        {result['skipped']}")
        print(f"Failed:         {result['failed']} ❌")
        print(f"Time:           {result['elapsed']:.1f}s")
        print(f"Dry run:        {result['dry_run']}")
        print("=" * 70)
        
        # Show queue stats
        print("\nQueue Status:")
        stats = get_queue_stats()
        for queue_name, queue_stats in stats.items():
            print(f"  {queue_name}: {queue_stats['count']} jobs")
        
        print("\nNext Steps:")
        print("  1. Start workers: rq worker pdf_queue (or add to multi-worker command)")
        print("  2. Monitor: rq-dashboard (visit http://localhost:9181)")
        
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