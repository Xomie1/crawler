#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main script for executing email campaigns from crawl results
"""

import argparse
import logging
import sys
from pathlib import Path

from config.email_config import EmailConfig
from services.email_campaign_manager import EmailCampaignManager
from services.sendgrid_email_service import SendGridEmailService


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('email_campaign.log', encoding='utf-8')
        ]
    )


def test_sendgrid_connection(test_email: str = None):
    """
    Test SendGrid connection by sending a test email.
    
    Args:
        test_email: Email address to send test to (optional)
    """
    print("\n" + "="*70)
    print("🔧 TESTING SENDGRID CONNECTION")
    print("="*70 + "\n")
    
    # Validate configuration
    is_valid, error = EmailConfig.validate()
    if not is_valid:
        print(f"❌ Configuration error: {error}")
        print("\nPlease check your .env file:")
        print("  - SENDGRID_API_KEY must be set")
        print("  - SENDER_EMAIL must be valid")
        return False
    
    EmailConfig.print_config()
    
    if test_email:
        try:
            service = SendGridEmailService()
            result = service.send_test_email(test_email)
            
            if result['success']:
                print(f"\n✅ Test email sent successfully!")
                print(f"   Message ID: {result['message_id']}")
                print(f"   Check {test_email} inbox")
                return True
            else:
                print(f"\n❌ Test email failed!")
                print(f"   Error: {result['error']}")
                return False
        except Exception as e:
            print(f"\n❌ Error: {e}")
            return False
    else:
        print("⚠️  No test email specified - skipping test send")
        print("   Use --test-email your@email.com to send test")
        return True


def run_campaign(
    crawl_results_file: str,
    dry_run: bool = False,
    skip_duplicates: bool = True,
    max_emails: int = None
):
    """
    Run email campaign from crawl results.
    
    Args:
        crawl_results_file: Path to crawl results JSONL file
        dry_run: Test mode (don't actually send)
        skip_duplicates: Skip emails already sent
        max_emails: Maximum emails to send (optional)
    """
    print("\n" + "="*70)
    print("📧 EMAIL CAMPAIGN")
    print("="*70 + "\n")
    
    # Validate file exists
    if not Path(crawl_results_file).exists():
        print(f"❌ File not found: {crawl_results_file}")
        return False
    
    # Validate configuration
    is_valid, error = EmailConfig.validate()
    if not is_valid:
        print(f"❌ Configuration error: {error}")
        return False
    
    try:
        # Initialize manager
        manager = EmailCampaignManager()
        
        # Show statistics before campaign
        print("📊 Current Statistics:")
        manager.print_statistics()
        
        # Run campaign
        result = manager.run_campaign(
            crawl_results_file=crawl_results_file,
            skip_duplicates=skip_duplicates,
            dry_run=dry_run
        )
        
        if 'error' in result:
            print(f"\n❌ Campaign failed: {result['error']}")
            return False
        
        # Success
        print("\n✅ Campaign completed successfully!")
        
        if dry_run:
            print("\n⚠️  DRY RUN - No emails were actually sent")
            print("   Remove --dry-run flag to send for real")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_statistics():
    """Show campaign statistics."""
    try:
        manager = EmailCampaignManager()
        manager.print_statistics()
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def clear_cache(older_than_days: int = None):
    """
    Clear sent emails cache.
    
    Args:
        older_than_days: Only clear emails older than N days
    """
    try:
        manager = EmailCampaignManager()
        
        if older_than_days:
            print(f"Clearing sent emails older than {older_than_days} days...")
        else:
            print("Clearing ALL sent emails cache...")
        
        manager.clear_sent_cache(older_than_days)
        print("✅ Cache cleared")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Email Campaign Manager - Phase 2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test SendGrid connection
  python run_email_campaign.py --test --test-email your@email.com
  
  # Dry run (don't actually send)
  python run_email_campaign.py crawl_results.jsonl --dry-run
  
  # Send emails for real
  python run_email_campaign.py crawl_results.jsonl
  
  # Show statistics
  python run_email_campaign.py --stats
  
  # Clear sent emails cache
  python run_email_campaign.py --clear-cache
        """
    )
    
    # Main arguments
    parser.add_argument(
        'crawl_results',
        nargs='?',
        help='Crawl results JSONL file'
    )
    
    # Campaign options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test mode - don\'t actually send emails'
    )
    parser.add_argument(
        '--allow-duplicates',
        action='store_true',
        help='Allow sending to same email multiple times'
    )
    parser.add_argument(
        '--max-emails',
        type=int,
        help='Maximum number of emails to send'
    )
    
    # Testing
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test SendGrid connection'
    )
    parser.add_argument(
        '--test-email',
        type=str,
        help='Email address for test send'
    )
    
    # Statistics
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show campaign statistics'
    )
    
    # Cache management
    parser.add_argument(
        '--clear-cache',
        action='store_true',
        help='Clear sent emails cache'
    )
    parser.add_argument(
        '--older-than',
        type=int,
        metavar='DAYS',
        help='Clear cache entries older than N days'
    )
    
    # Logging
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Handle commands
    success = True
    
    if args.test:
        # Test SendGrid connection
        success = test_sendgrid_connection(args.test_email)
    
    elif args.stats:
        # Show statistics
        success = show_statistics()
    
    elif args.clear_cache:
        # Clear cache
        success = clear_cache(args.older_than)
    
    elif args.crawl_results:
        # Run campaign
        success = run_campaign(
            crawl_results_file=args.crawl_results,
            dry_run=args.dry_run,
            skip_duplicates=not args.allow_duplicates,
            max_emails=args.max_emails
        )
    
    else:
        # No command specified
        parser.print_help()
        sys.exit(1)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()