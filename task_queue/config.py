# -*- coding: utf-8 -*-
"""
Queue Configuration
Centralized settings for Redis and RQ
"""

import os
from redis import Redis
from rq import Retry

# ==================== REDIS CONNECTION ====================

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

def get_redis_connection():
    """Get Redis connection with settings"""
    return Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=False  # Keep as bytes for RQ compatibility
    )

# ==================== QUEUE PRIORITIES ====================

QUEUE_PRIORITIES = {
    'crawl_queue': 'high',      # Process crawls first
    'email_queue': 'default',   # Then emails
    'form_queue': 'low',        # Finally forms
    'failed_queue': 'low'       # Retry failed jobs
}

# ==================== RETRY POLICIES ====================

# Crawl jobs: Retry 3 times with exponential backoff
CRAWL_RETRY = Retry(max=3, interval=[10, 30, 60])

# Email jobs: Retry 2 times (SendGrid is usually reliable)
EMAIL_RETRY = Retry(max=2, interval=[30, 60])

# Form jobs: Retry 3 times (forms can be temperamental)
FORM_RETRY = Retry(max=3, interval=[15, 45, 90])

# ==================== JOB TIMEOUTS ====================

CRAWL_JOB_TIMEOUT = '10m'   # 10 minutes per crawl
EMAIL_JOB_TIMEOUT = '5m'    # 5 minutes per email
FORM_JOB_TIMEOUT = '15m'    # 15 minutes per form (includes browser)

# ==================== RESULT TTL ====================

RESULT_TTL = 86400 * 7  # Keep results for 7 days, then auto-delete

# ==================== WORKER SETTINGS ====================

WORKER_COUNT_CRAWL = int(os.getenv('CRAWL_WORKERS', 3))
WORKER_COUNT_EMAIL = int(os.getenv('EMAIL_WORKERS', 2))
WORKER_COUNT_FORM = int(os.getenv('FORM_WORKERS', 2))

# ==================== RATE LIMITING ====================

# Crawl rate limit: 1 request per domain per 2 seconds
CRAWL_RATE_LIMIT = 2.0  # seconds

# Email rate limit: Respect SendGrid hourly limit
EMAIL_RATE_LIMIT_PER_HOUR = 100

# Form rate limit: 1 submission per domain per 5 seconds  
FORM_RATE_LIMIT = 5.0  # seconds

# ==================== LOGGING ====================

LOG_LEVEL = os.getenv('QUEUE_LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'