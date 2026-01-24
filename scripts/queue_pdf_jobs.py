#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Queue PDF Jobs Script
Reads form submission data from JSONL files and enqueues PDF generation jobs.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from task_queue.queues import pdf_queue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def transform_submission_to_document_input(submission_data):
    """
    Transform form submission data to DocumentInput format.
    
    Args:
        submission_data: dict from submission_log.jsonl
        
    Returns:
        dict: formatted for PDFdocsEngine.engine.generate()
    """
    # Extract parties from submission data
    parties = []
    
    # Build parties from form submission fields
    # Adjust field names based on your actual submission structure
    if 'party1_name' in submission_data and 'party1_address' in submission_data:
        parties.append({
            'name': submission_data['party1_name'],
            'address': submission_data.get('party1_address', ''),
            'role': submission_data.get('party1_role', 'party1')
        })
    
    if 'party2_name' in submission_data and 'party2_address' in submission_data:
        parties.append({
            'name': submission_data['party2_name'],
            'address': submission_data.get('party2_address', ''),
            'role': submission_data.get('party2_role', 'party2')
        })
    
    # Build document input
    document_input = {
        'document_type': submission_data.get('document_type', 'prenuptial'),
        'parties': parties,
        'options': {
            'property_separation': submission_data.get('property_separation', False),
            'alimony': submission_data.get('alimony', False),
            'children': submission_data.get('children', False)
        },
        'custom_values': submission_data.get('custom_values', {}),
        'include_signatures': submission_data.get('include_signatures', True),
        'include_witnesses': submission_data.get('include_witnesses', False),
    }
    
    return document_input


def queue_pdf_jobs_from_jsonl(jsonl_file, filter_status=None):
    """
    Read form submission data from JSONL file and enqueue PDF generation jobs.
    
    Args:
        jsonl_file: Path to JSONL file
        filter_status: Optional status filter (e.g., 'success' to only queue successful submissions)
        
    Returns:
        dict: Statistics about queued jobs
    """
    jsonl_path = Path(jsonl_file)
    
    if not jsonl_path.exists():
        logger.error(f"❌ File not found: {jsonl_file}")
        return {'total': 0, 'queued': 0, 'failed': 0, 'skipped': 0}
    
    stats = {'total': 0, 'queued': 0, 'failed': 0, 'skipped': 0}
    
    logger.info(f"📖 Reading submissions from: {jsonl_file}")
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                stats['total'] += 1
                
                # Apply status filter if provided
                if filter_status and data.get('status') != filter_status:
                    stats['skipped'] += 1
                    continue
                
                # Skip if already processed
                if data.get('pdf_generated'):
                    logger.debug(f"Line {line_num}: PDF already generated, skipping")
                    stats['skipped'] += 1
                    continue
                
                # Transform to document input format
                document_input = transform_submission_to_document_input(data)
                
                # Validate parties
                if not document_input['parties'] or len(document_input['parties']) < 2:
                    logger.warning(f"Line {line_num}: Skipping - insufficient party data")
                    stats['skipped'] += 1
                    continue
                
                # Enqueue job
                job = pdf_queue.enqueue(
                    'workers.pdf_worker.generate_pdf_job',
                    document_input,
                    job_timeout='10m',
                    result_ttl=3600
                )
                
                stats['queued'] += 1
                logger.info(f"Line {line_num}: ✅ Queued PDF job: {job.id}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Line {line_num}: Invalid JSON - {str(e)}")
                stats['failed'] += 1
            except Exception as e:
                logger.error(f"Line {line_num}: Failed to queue - {str(e)}")
                stats['failed'] += 1
    
    return stats


def queue_pdf_jobs_from_directory(directory, pattern="submission_log.jsonl"):
    """
    Queue PDF jobs from all matching JSONL files in a directory.
    
    Args:
        directory: Directory to search
        pattern: File pattern to match
        
    Returns:
        dict: Combined statistics
    """
    dir_path = Path(directory)
    total_stats = {'total': 0, 'queued': 0, 'failed': 0, 'skipped': 0}
    
    files = list(dir_path.glob(f"**/{pattern}"))
    
    if not files:
        logger.warning(f"No files matching '{pattern}' found in {directory}")
        return total_stats
    
    logger.info(f"📂 Found {len(files)} file(s) to process")
    
    for jsonl_file in files:
        logger.info(f"\n📄 Processing: {jsonl_file}")
        stats = queue_pdf_jobs_from_jsonl(str(jsonl_file))
        
        for key in total_stats:
            total_stats[key] += stats[key]
    
    return total_stats


def print_queue_stats():
    """Print current PDF queue statistics."""
    from task_queue.queues import get_queue_stats
    
    stats = get_queue_stats()
    pdf_stats = stats.get('pdf_queue', {})
    
    logger.info("\n📊 PDF Queue Statistics:")
    logger.info(f"  Pending: {pdf_stats.get('count', 0)}")
    logger.info(f"  Started: {pdf_stats.get('started_jobs', 0)}")
    logger.info(f"  Finished: {pdf_stats.get('finished_jobs', 0)}")
    logger.info(f"  Failed: {pdf_stats.get('failed_jobs', 0)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Queue PDF generation jobs from submission logs"
    )
    parser.add_argument(
        "source",
        nargs="?",
        default="submission_log.jsonl",
        help="JSONL file or directory to process (default: submission_log.jsonl)"
    )
    parser.add_argument(
        "--status",
        help="Filter submissions by status (e.g., 'success')"
    )
    parser.add_argument(
        "--dir",
        action="store_true",
        help="Treat source as directory and process all matching files"
    )
    parser.add_argument(
        "--pattern",
        default="submission_log.jsonl",
        help="File pattern when using --dir (default: submission_log.jsonl)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show PDF queue statistics"
    )
    
    args = parser.parse_args()
    
    if args.stats:
        print_queue_stats()
    elif args.dir:
        stats = queue_pdf_jobs_from_directory(args.source, args.pattern)
    else:
        stats = queue_pdf_jobs_from_jsonl(args.source, args.status)
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("📋 SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total read:     {stats['total']}")
    logger.info(f"Queued:         {stats['queued']} ✅")
    logger.info(f"Skipped:        {stats['skipped']}")
    logger.info(f"Failed:         {stats['failed']} ❌")
    logger.info("=" * 60)
