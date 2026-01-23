#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redis & RQ Installation Verification Script
Run this after installing Redis to verify everything works
"""

import sys
import time

def test_redis_connection():
    """Test basic Redis connection"""
    try:
        import redis
        
        print("🔄 Testing Redis connection...")
        r = redis.Redis(host='localhost', port=6379, db=0)
        
        # Test ping
        r.ping()
        print("✅ Redis connected successfully")
        
        # Test basic operations
        r.set('test_key', 'test_value')
        value = r.get('test_key')
        
        if value.decode('utf-8') == 'test_value':
            print("✅ Redis read/write test passed")
        else:
            print("❌ Redis read/write test failed")
            return False
        
        # Cleanup
        r.delete('test_key')
        
        return True
        
    except redis.ConnectionError:
        print("❌ Cannot connect to Redis")
        print("   Make sure Redis is running:")
        print("   - Windows: redis-server.exe")
        print("   - Mac/Linux: redis-server")
        return False
    except ImportError:
        print("❌ Redis Python package not installed")
        print("   Run: pip install -r requirements_queue.txt")
        return False
    except Exception as e:
        print(f"❌ Redis test failed: {e}")
        return False


def test_rq_queues():
    """Test RQ queue operations"""
    try:
        from redis import Redis
        from rq import Queue
        
        print("\n🔄 Testing RQ queues...")
        
        redis_conn = Redis()
        test_queue = Queue('test_queue', connection=redis_conn)
        
        # Test basic queue operation (just check queue exists)
        queue_length = len(test_queue)
        print(f"✅ RQ queue created successfully")
        print(f"   Queue length: {queue_length}")
        
        # Clear any test data
        test_queue.empty()
        
        return True
        
    except ImportError:
        print("❌ RQ not installed")
        print("   Run: pip install -r requirements_queue.txt")
        return False
    except Exception as e:
        print(f"❌ RQ test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_rq_worker():
    """Test RQ worker availability"""
    try:
        from rq import Worker
        from redis import Redis
        
        print("\n🔄 Testing RQ worker setup...")
        
        redis_conn = Redis()
        workers = Worker.all(connection=redis_conn)
        
        print(f"✅ RQ worker module loaded")
        print(f"   Active workers: {len(workers)}")
        
        if len(workers) == 0:
            print("   ℹ️  No workers running yet (this is normal)")
        
        return True
        
    except Exception as e:
        print(f"❌ RQ worker test failed: {e}")
        return False


def test_config():
    """Test that queue configuration can be loaded"""
    try:
        print("\n🔄 Testing queue configuration...")
        
        from task_queue.config import (
            get_redis_connection,
            CRAWL_JOB_TIMEOUT,
            EMAIL_JOB_TIMEOUT,
            FORM_JOB_TIMEOUT
        )
        from task_queue.queues import crawl_queue, email_queue, form_queue
        
        print(f"✅ Queue configuration loaded")
        print(f"   Crawl timeout: {CRAWL_JOB_TIMEOUT}")
        print(f"   Email timeout: {EMAIL_JOB_TIMEOUT}")
        print(f"   Form timeout: {FORM_JOB_TIMEOUT}")
        print(f"   Crawl queue: {crawl_queue.name}")
        print(f"   Email queue: {email_queue.name}")
        print(f"   Form queue: {form_queue.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all verification tests"""
    print("=" * 70)
    print("REDIS + RQ INSTALLATION VERIFICATION")
    print("=" * 70)
    
    results = []
    
    # Test 1: Redis connection
    results.append(test_redis_connection())
    
    # Test 2: RQ queues
    if results[0]:  # Only if Redis works
        results.append(test_rq_queues())
        results.append(test_rq_worker())
        results.append(test_config())
    
    # Summary
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)
    
    if all(results):
        print("✅ All tests passed! Queue system is ready.")
        print("\nNext steps:")
        print("  1. Test the scheduler (dry run):")
        print("     python queue_scheduler.py crawl.jsonl --dry-run")
        print("\n  2. Queue some URLs (just 5 to test):")
        print("     python queue_scheduler.py crawl.jsonl --limit 5")
        print("\n  3. Start a worker (in another terminal):")
        print("     rq worker crawl_queue -v")
        print("\n  4. Monitor queues:")
        print("     python monitor_queues.py")
        return 0
    else:
        print("❌ Some tests failed. Please fix the issues above.")
        print("\nTroubleshooting:")
        print("  1. Make sure Redis is running: redis-server")
        print("  2. Install Python packages: pip install -r requirements_queue.txt")
        print("  3. Check that you're in the virtual environment: source venv/bin/activate")
        return 1


if __name__ == "__main__":
    sys.exit(main())