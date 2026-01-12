# -*- coding: utf-8 -*-
"""
Email Campaign Manager - Phase 2
Orchestrates bulk email campaigns with duplicate checking and rate limiting
"""

import json
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path

from services.sendgrid_email_service import SendGridEmailService
from config.email_config import EmailConfig

logger = logging.getLogger(__name__)


class EmailCampaignManager:
    """
    Manages email campaigns with:
    - Duplicate prevention
    - Rate limiting
    - Logging
    - Error handling
    """
    
    def __init__(
        self,
        log_file: str = "email_campaign_log.jsonl",
        sent_emails_cache: str = "sent_emails_cache.json"
    ):
        """
        Initialize campaign manager.
        
        Args:
            log_file: Path to log file for campaign results
            sent_emails_cache: Cache file for sent emails (duplicate prevention)
        """
        self.email_service = SendGridEmailService()
        self.log_file = log_file
        self.cache_file = sent_emails_cache
        
        # Load sent emails cache
        self.sent_emails = self._load_sent_cache()
        
        # Daily counter (resets each day)
        self.daily_count = 0
        self.daily_reset_date = datetime.utcnow().date()
        
        logger.info("Email Campaign Manager initialized")
        logger.info(f"  Log file: {self.log_file}")
        logger.info(f"  Cache file: {self.cache_file}")
        logger.info(f"  Cached sent emails: {len(self.sent_emails)}")
    
    def run_campaign(
        self,
        crawl_results_file: str,
        message_template: Optional[str] = None,
        subject_template: Optional[str] = None,
        skip_duplicates: bool = True,
        dry_run: bool = False
    ) -> Dict:
        """
        Run email campaign from crawl results.
        
        Args:
            crawl_results_file: Path to crawl results JSONL file
            message_template: Custom message template (optional)
            subject_template: Custom subject template (optional)
            skip_duplicates: Skip emails already sent (default: True)
            dry_run: Test mode - don't actually send (default: False)
            
        Returns:
            Campaign summary dictionary
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"EMAIL CAMPAIGN STARTING")
        logger.info(f"{'='*70}")
        logger.info(f"Source: {crawl_results_file}")
        logger.info(f"Dry Run: {dry_run}")
        logger.info(f"Skip Duplicates: {skip_duplicates}")
        logger.info(f"{'='*70}\n")
        
        # Load crawl results
        crawl_results = self._load_crawl_results(crawl_results_file)
        
        if not crawl_results:
            logger.error("No crawl results found")
            return {'error': 'No crawl results found'}
        
        logger.info(f"Loaded {len(crawl_results)} crawl results")
        
        # Filter targets (has email, not duplicate)
        targets = self._filter_targets(crawl_results, skip_duplicates)
        
        if not targets:
            logger.warning("No valid email targets after filtering")
            return {
                'total_crawl_results': len(crawl_results),
                'valid_targets': 0,
                'skipped': len(crawl_results),
                'sent': 0,
                'failed': 0
            }
        
        logger.info(f"Valid targets: {len(targets)}")
        
        # Check daily limit
        if self.daily_count >= EmailConfig.DAILY_EMAIL_LIMIT:
            logger.warning(f"Daily limit reached ({EmailConfig.DAILY_EMAIL_LIMIT})")
            return {
                'error': f'Daily limit reached ({EmailConfig.DAILY_EMAIL_LIMIT})',
                'valid_targets': len(targets),
                'sent': 0
            }
        
        # Apply daily limit
        remaining_today = EmailConfig.DAILY_EMAIL_LIMIT - self.daily_count
        if len(targets) > remaining_today:
            logger.warning(
                f"Limiting to {remaining_today} emails to stay within daily limit"
            )
            targets = targets[:remaining_today]
        
        # Prepare recipients for bulk send
        recipients = self._prepare_recipients(
            targets,
            message_template,
            subject_template
        )
        
        # Send emails
        if dry_run:
            logger.info("\n🏃 DRY RUN MODE - Not actually sending")
            results = {
                'total': len(recipients),
                'successful': len(recipients),
                'failed': 0,
                'details': [
                    {'email': r['email'], 'success': True, 'dry_run': True}
                    for r in recipients
                ]
            }
        else:
            results = self.email_service.send_bulk(
                recipients=recipients,
                rate_limit=EmailConfig.HOURLY_EMAIL_LIMIT
            )
        
        # Update sent emails cache
        if not dry_run:
            for detail in results['details']:
                if detail['success']:
                    self._mark_as_sent(detail['email'])
        
        # Update daily counter
        self.daily_count += results['successful']
        
        # Save results
        self._save_campaign_results(results, crawl_results_file)
        
        # Print summary
        self._print_summary(
            total_crawl_results=len(crawl_results),
            valid_targets=len(targets),
            results=results
        )
        
        return {
            'total_crawl_results': len(crawl_results),
            'valid_targets': len(targets),
            'sent': results['successful'],
            'failed': results['failed'],
            'daily_count': self.daily_count,
            'daily_limit': EmailConfig.DAILY_EMAIL_LIMIT
        }
    
    def _load_crawl_results(self, file_path: str) -> List[Dict]:
        """Load crawl results from JSONL file."""
        results = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        result = json.loads(line)
                        results.append(result)
        except FileNotFoundError:
            logger.error(f"Crawl results file not found: {file_path}")
        except Exception as e:
            logger.error(f"Error loading crawl results: {e}")
        
        return results
    
    def _filter_targets(
        self,
        crawl_results: List[Dict],
        skip_duplicates: bool
    ) -> List[Dict]:
        """
        Filter crawl results to valid email targets.
        
        Filters:
        - Must have email
        - Email not already sent (if skip_duplicates=True)
        - Not in blacklist
        """
        targets = []
        
        for result in crawl_results:
            email = result.get('email')
            
            # Must have email
            if not email:
                continue
            
            # Check duplicate
            if skip_duplicates and self._is_already_sent(email):
                logger.debug(f"Skipping duplicate: {email}")
                continue
            
            # Check blacklist
            if self._is_blacklisted_email(email):
                logger.debug(f"Skipping blacklisted: {email}")
                continue
            
            targets.append(result)
        
        return targets
    
    def _prepare_recipients(
        self,
        targets: List[Dict],
        message_template: Optional[str],
        subject_template: Optional[str]
    ) -> List[Dict]:
        """
        Prepare recipient list with template data.
        
        Args:
            targets: Filtered crawl results
            message_template: Custom message template
            subject_template: Custom subject template
            
        Returns:
            List of recipient dicts for SendGrid
        """
        recipients = []
        
        for target in targets:
            email = target.get('email')
            company_name = target.get('companyName', '御社')
            url = target.get('url', '')
            
            # Template data for variable substitution
            template_data = {
                'company_name': company_name,
                'sender_name': EmailConfig.SENDER_NAME,
                'sender_email': EmailConfig.SENDER_EMAIL,
                'message_body': self._get_default_message(company_name),
                'website_url': url
            }
            
            recipient = {
                'email': email,
                'name': company_name,
                'template_data': template_data
            }
            
            recipients.append(recipient)
        
        return recipients
    
    def _get_default_message(self, company_name: str) -> str:
        """Generate default message body."""
        return f"""
{company_name}様の新製品・サービスについてお問い合わせさせていただきたくご連絡いたしました。

弊社では、企業様のビジネス成長をサポートするサービスを提供しております。

詳細につきましてはお手数ですが、お気軽にお問い合わせください。
"""
    
    def _is_already_sent(self, email: str) -> bool:
        """Check if email was already sent recently."""
        if email not in self.sent_emails:
            return False
        
        sent_date_str = self.sent_emails[email]
        sent_date = datetime.fromisoformat(sent_date_str)
        
        # Check cooldown period
        cooldown = timedelta(days=EmailConfig.DUPLICATE_COOLDOWN_DAYS)
        if datetime.utcnow() - sent_date < cooldown:
            return True
        
        # Cooldown expired - remove from cache
        del self.sent_emails[email]
        return False
    
    def _mark_as_sent(self, email: str):
        """Mark email as sent in cache."""
        self.sent_emails[email] = datetime.utcnow().isoformat()
        self._save_sent_cache()
    
    def _is_blacklisted_email(self, email: str) -> bool:
        """Check if email domain is blacklisted."""
        domain = email.split('@')[1] if '@' in email else ''
        return domain.lower() in EmailConfig.BLACKLIST_DOMAINS
    
    def _load_sent_cache(self) -> Dict:
        """Load sent emails cache from file."""
        if not Path(self.cache_file).exists():
            return {}
        
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load sent cache: {e}")
            return {}
    
    def _save_sent_cache(self):
        """Save sent emails cache to file."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.sent_emails, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Could not save sent cache: {e}")
    
    def _save_campaign_results(self, results: Dict, source_file: str):
        """Save campaign results to log file."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'source_file': source_file,
            'total': results['total'],
            'successful': results['successful'],
            'failed': results['failed'],
            'daily_count': self.daily_count,
            'details': results['details']
        }
        
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            logger.debug(f"Campaign results saved to {self.log_file}")
        except Exception as e:
            logger.error(f"Could not save campaign results: {e}")
    
    def _print_summary(
        self,
        total_crawl_results: int,
        valid_targets: int,
        results: Dict
    ):
        """Print campaign summary."""
        print("\n" + "="*70)
        print("📧 EMAIL CAMPAIGN SUMMARY")
        print("="*70)
        print(f"Total crawl results:    {total_crawl_results}")
        print(f"Valid email targets:    {valid_targets}")
        print(f"Emails sent:            {results['successful']}")
        print(f"Failed:                 {results['failed']}")
        print(f"Success rate:           {results['successful']/results['total']*100:.1f}%")
        print("-"*70)
        print(f"Daily count:            {self.daily_count}/{EmailConfig.DAILY_EMAIL_LIMIT}")
        print(f"Remaining today:        {EmailConfig.DAILY_EMAIL_LIMIT - self.daily_count}")
        print("="*70 + "\n")
        
        if results['failed'] > 0:
            print("❌ Failed emails:")
            for detail in results['details']:
                if not detail['success']:
                    print(f"  - {detail['email']}: {detail['error']}")
            print()
    
    def get_statistics(self) -> Dict:
        """Get campaign statistics from log file."""
        stats = {
            'total_campaigns': 0,
            'total_emails_sent': 0,
            'total_emails_failed': 0,
            'unique_recipients': len(self.sent_emails),
            'daily_count': self.daily_count,
            'daily_limit': EmailConfig.DAILY_EMAIL_LIMIT
        }
        
        if not Path(self.log_file).exists():
            return stats
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        entry = json.loads(line)
                        stats['total_campaigns'] += 1
                        stats['total_emails_sent'] += entry.get('successful', 0)
                        stats['total_emails_failed'] += entry.get('failed', 0)
        except Exception as e:
            logger.error(f"Could not read campaign log: {e}")
        
        return stats
    
    def print_statistics(self):
        """Print campaign statistics."""
        stats = self.get_statistics()
        
        print("\n" + "="*70)
        print("📊 CAMPAIGN STATISTICS")
        print("="*70)
        print(f"Total campaigns:        {stats['total_campaigns']}")
        print(f"Total emails sent:      {stats['total_emails_sent']}")
        print(f"Total emails failed:    {stats['total_emails_failed']}")
        print(f"Unique recipients:      {stats['unique_recipients']}")
        print("-"*70)
        print(f"Today's count:          {stats['daily_count']}/{stats['daily_limit']}")
        print(f"Remaining today:        {stats['daily_limit'] - stats['daily_count']}")
        print("="*70 + "\n")
    
    def reset_daily_count(self):
        """Reset daily counter (called automatically at midnight)."""
        today = datetime.utcnow().date()
        if today > self.daily_reset_date:
            logger.info(f"Resetting daily counter (was: {self.daily_count})")
            self.daily_count = 0
            self.daily_reset_date = today
    
    def clear_sent_cache(self, older_than_days: int = None):
        """
        Clear sent emails cache.
        
        Args:
            older_than_days: Only clear emails older than N days (optional)
        """
        if older_than_days is None:
            # Clear all
            self.sent_emails = {}
            logger.info("Cleared all sent emails cache")
        else:
            # Clear old entries
            cutoff = datetime.utcnow() - timedelta(days=older_than_days)
            old_count = len(self.sent_emails)
            
            self.sent_emails = {
                email: date_str
                for email, date_str in self.sent_emails.items()
                if datetime.fromisoformat(date_str) > cutoff
            }
            
            removed = old_count - len(self.sent_emails)
            logger.info(f"Cleared {removed} old entries from sent cache")
        
        self._save_sent_cache()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize manager
    manager = EmailCampaignManager()
    
    # Print current statistics
    manager.print_statistics()
    
    # Run campaign (dry run first to test)
    result = manager.run_campaign(
        crawl_results_file="crawl_results_20250101_120000.jsonl",
        dry_run=True  # Set to False to actually send
    )
    
    print("\n✅ Campaign complete!")