# -*- coding: utf-8 -*-
"""
Queue Definitions
Defines all RQ queues used in the system
"""

from rq import Queue
from task_queue.config import (
    get_redis_connection,
    CRAWL_JOB_TIMEOUT,
    EMAIL_JOB_TIMEOUT,
    FORM_JOB_TIMEOUT,
    RESULT_TTL
)

# Get shared Redis connection
redis_conn = get_redis_connection()

# ==================== MAIN QUEUES ====================

# Crawl Queue - High priority
crawl_queue = Queue(
    'crawl_queue',
    connection=redis_conn,
    default_timeout=CRAWL_JOB_TIMEOUT,
    result_ttl=RESULT_TTL
)

# Email Queue - Default priority
email_queue = Queue(
    'email_queue',
    connection=redis_conn,
    default_timeout=EMAIL_JOB_TIMEOUT,
    result_ttl=RESULT_TTL
)

# Form Queue - Low priority
form_queue = Queue(
    'form_queue',
    connection=redis_conn,
    default_timeout=FORM_JOB_TIMEOUT,
    result_ttl=RESULT_TTL
)

# ==================== SPECIAL QUEUES ====================

# Failed Queue - For manual retry
failed_queue = Queue(
    'failed_queue',
    connection=redis_conn,
    default_timeout='30m',
    result_ttl=RESULT_TTL * 2  # Keep failed jobs longer
)

# CAPTCHA Queue - For manual intervention
captcha_queue = Queue(
    'captcha_queue',
    connection=redis_conn,
    default_timeout='1h',
    result_ttl=RESULT_TTL * 7  # Keep for a month
)

# PDF Generation Queue - For document generation
pdf_queue = Queue(
    'pdf_queue',
    connection=redis_conn,
    default_timeout='10m',
    result_ttl=RESULT_TTL
)

# ==================== HELPER FUNCTIONS ====================

def get_all_queues():
    """Get list of all queues"""
    return [crawl_queue, email_queue, form_queue, failed_queue, captcha_queue, pdf_queue]


def get_queue_stats():
    """Get statistics for all queues"""
    stats = {}
    
    for queue in get_all_queues():
        stats[queue.name] = {
            'count': len(queue),
            'started_jobs': queue.started_job_registry.count,
            'finished_jobs': queue.finished_job_registry.count,
            'failed_jobs': queue.failed_job_registry.count,
            'deferred_jobs': queue.deferred_job_registry.count,
        }
    
    return stats


def clear_all_queues():
    """Clear all queues (for testing/reset)"""
    for queue in get_all_queues():
        queue.empty()
    
    print("✅ All queues cleared")


def get_queue_by_name(name):
    """Get queue by name"""
    queue_map = {
        'crawl': crawl_queue,
        'email': email_queue,
        'form': form_queue,
        'failed': failed_queue,
        'captcha': captcha_queue
    }
    
    return queue_map.get(name)