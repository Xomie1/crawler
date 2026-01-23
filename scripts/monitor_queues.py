#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Queue Monitor
Real-time monitoring of queue status
"""

import sys
import time
from datetime import datetime, timedelta

from task_queue.queues import get_all_queues, get_queue_stats
from rq import Worker
from task_queue.config import get_redis_connection


def get_worker_info():
    """Get information about active workers"""
    redis_conn = get_redis_connection()
    workers = Worker.all(connection=redis_conn)
    
    worker_info = []
    for worker in workers:
        worker_info.append({
            'name': worker.name,
            'state': worker.get_state(),
            'current_job': worker.get_current_job(),
            'queues': [q.name for q in worker.queues]
        })
    
    return worker_info


def calculate_eta(total, completed, rate):
    """Calculate ETA for completion"""
    if rate == 0:
        return "Unknown"
    
    remaining = total - completed
    seconds_remaining = remaining / rate
    
    eta = timedelta(seconds=int(seconds_remaining))
    return str(eta)


def format_number(num):
    """Format number with commas"""
    return f"{num:,}"


def print_queue_monitor(refresh_interval=5):
    """
    Print queue monitor with auto-refresh.
    
    Args:
        refresh_interval: Seconds between refreshes
    """
    
    print("\n" * 2)
    print("=" * 70)
    print("RQ QUEUE MONITOR")
    print("=" * 70)
    print(f"Refresh interval: {refresh_interval}s (Ctrl+C to exit)")
    print("=" * 70)
    
    last_stats = None
    start_time = time.time()
    
    try:
        while True:
            # Clear screen (Unix/Mac)
            if sys.platform != 'win32':
                print("\033[H\033[J", end="")
            
            # Get current stats
            stats = get_queue_stats()
            worker_info = get_worker_info()
            
            # Calculate totals
            total_queued = sum(s['count'] for s in stats.values())
            total_started = sum(s['started_jobs'] for s in stats.values())
            total_finished = sum(s['finished_jobs'] for s in stats.values())
            total_failed = sum(s['failed_jobs'] for s in stats.values())
            
            # Calculate rate
            if last_stats:
                time_diff = refresh_interval
                finished_diff = total_finished - sum(s['finished_jobs'] for s in last_stats.values())
                rate = finished_diff / time_diff if time_diff > 0 else 0
            else:
                rate = 0
            
            # Print header
            print(f"\n{'╔' + '═' * 68 + '╗'}")
            print(f"{'║'}  RQ QUEUE MONITOR{' ' * 50}{'║'}")
            print(f"{'║'}  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{' ' * 50}{'║'}")
            print(f"{'╚' + '═' * 68 + '╝'}\n")
            
            # Print queue stats
            print("QUEUES:")
            print("-" * 70)
            for queue_name, queue_stats in stats.items():
                count = queue_stats['count']
                started = queue_stats['started_jobs']
                finished = queue_stats['finished_jobs']
                failed = queue_stats['failed_jobs']
                
                print(f"  {queue_name:20s}  {format_number(count):>8s} queued  "
                      f"{format_number(started):>6s} active  "
                      f"{format_number(finished):>8s} done  "
                      f"{format_number(failed):>6s} failed")
            
            print()
            
            # Print workers
            print("WORKERS:")
            print("-" * 70)
            if worker_info:
                for worker in worker_info:
                    state = worker['state']
                    queues_str = ', '.join(worker['queues'])
                    current_job = worker['current_job']
                    
                    status = f"{state}"
                    if current_job:
                        status += f" (processing {current_job.id[:8]}...)"
                    
                    print(f"  {worker['name']:30s}  {status}")
                    print(f"    Queues: {queues_str}")
            else:
                print("  No workers running")
                print("  Start workers: rq worker crawl_queue email_queue form_queue")
            
            print()
            
            # Print summary
            print("SUMMARY:")
            print("-" * 70)
            print(f"  Total queued:     {format_number(total_queued)}")
            print(f"  Active jobs:      {format_number(total_started)}")
            print(f"  Completed:        {format_number(total_finished)}")
            print(f"  Failed:           {format_number(total_failed)}")
            
            if rate > 0:
                print(f"  Processing rate:  {rate:.1f} jobs/sec")
                
                if total_queued > 0:
                    eta = calculate_eta(total_queued + total_finished, total_finished, rate)
                    print(f"  ETA:              {eta}")
            
            elapsed = time.time() - start_time
            print(f"  Uptime:           {str(timedelta(seconds=int(elapsed)))}")
            
            print("\n" + "=" * 70)
            print(f"Refreshing in {refresh_interval}s... (Ctrl+C to exit)")
            
            # Save for next iteration
            last_stats = stats
            
            # Wait
            time.sleep(refresh_interval)
            
    except KeyboardInterrupt:
        print("\n\nMonitor stopped.")
        return 0


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor RQ queues')
    parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Refresh interval in seconds (default: 5)'
    )
    
    args = parser.parse_args()
    
    return print_queue_monitor(refresh_interval=args.interval)


if __name__ == "__main__":
    sys.exit(main())