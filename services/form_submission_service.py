# -*- coding: utf-8 -*-
"""
Form Submission Service - WITH CAPTCHA QUEUE INTEGRATION
Integrates form submission into the crawler workflow with CAPTCHA exception handling
"""

import logging
import json
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

# Import from correct modules
from crawler.fetcher import PageFetcher
from crawler.submit_form.form_submitter import FormSubmissionPipeline, SubmissionResult
from crawler.captcha_queue_manager import CaptchaQueueManager, handle_captcha_form
from services.db_service import FormSubmissionDB
from utils.encoding import safe_dump_json, safe_load_json, safe_read_file, ensure_utf8

logger = logging.getLogger(__name__)


class FormSubmissionService:
    """
    Service that manages form submissions for crawled contacts.
    Integrates with existing crawler results and handles CAPTCHA exceptions.
    """
    
    # Default inquiry message (UTF-8)
    DEFAULT_MESSAGE_TEMPLATE = """
こんにちは、

株式会社{company_name}の新製品・サービスについてお問い合わせさせていただきたくご連絡いたしました。

詳細につきましてはお手数ですが、お気軽にお問い合わせください。

よろしくお願いいたします。
"""
    
    def __init__(
        self,
        timeout: int = 30,
        user_agent: str = "ContactBot/1.0",
        submission_log_file: str = None,
        captcha_queue_file: str = "captcha_queue.jsonl",
        db_path: str = "form_submissions.db",
        throttle_per_site: float = 2.0
    ):
        """
        Initialize form submission service.
        
        Args:
            timeout: Request timeout in seconds
            user_agent: User agent string
            submission_log_file: Path to log file for submissions
            captcha_queue_file: Path to CAPTCHA queue file
            db_path: Path to database file
            throttle_per_site: Minimum seconds between submissions per site
        """
        self.fetcher = PageFetcher(timeout=timeout, user_agent=user_agent)
        self.pipeline = FormSubmissionPipeline(timeout=timeout, user_agent=user_agent)
        self.submission_log_file = submission_log_file or "submission_log.jsonl"
        
        # Initialize CAPTCHA queue manager
        self.captcha_queue = CaptchaQueueManager(captcha_queue_file)
        
        # Initialize database
        self.db = FormSubmissionDB(db_path)
        
        # Per-site throttling
        self.throttle_per_site = throttle_per_site
        self._last_submission_times: Dict[str, datetime] = {}  
        
        self.results: List[Dict] = []
        
        logger.info(f"FormSubmissionService initialized")
        logger.info(f"  Log file: {self.submission_log_file}")
        logger.info(f"  Database: {db_path}")
        logger.info(f"  CAPTCHA queue: {captcha_queue_file}")
        logger.info(f"  Throttle per site: {throttle_per_site}s")
        logger.info(f"  Pending CAPTCHAs: {self.captcha_queue.count_pending()}")
    
    def submit_inquiry(
        self,
        form_url: str,
        company_name: str,
        sender_email: str = "inquiry@example.com",
        sender_name: str = "お問い合わせ",
        message_body: str = None,
        phone: str = None
    ) -> Dict:
        """
        Submit inquiry to a form WITH CAPTCHA EXCEPTION HANDLING.
        
        Args:
            form_url: URL of the contact form
            company_name: Name of company (will be ensured UTF-8)
            sender_email: Email to use
            sender_name: Name of sender
            message_body: Message content (uses template if not provided)
            phone: Phone number (optional)
            
        Returns:
            Submission result dictionary
        """
        
        # Ensure all inputs are UTF-8
        company_name = ensure_utf8(company_name)
        sender_email = ensure_utf8(sender_email)
        sender_name = ensure_utf8(sender_name)
        
        if phone:
            phone = ensure_utf8(phone)
        
        # Use template if no message provided
        if not message_body:
            message_body = self.DEFAULT_MESSAGE_TEMPLATE.format(company_name=company_name)
        
        message_body = ensure_utf8(message_body)
        
        result = {
            'timestamp': datetime.utcnow().isoformat(),
            'form_url': form_url,
            'company_name': company_name,
            'sender_email': sender_email,
            'sender_name': sender_name,
            'phone': phone,
            'submission_status': None,
            'submission_result': None,
            'error': None,
            'captcha_queued': False  # NEW: Track if queued for CAPTCHA
        }
        
        try:
            logger.info(f"\n{'='*70}")
            logger.info(f"Submitting to: {form_url}")
            logger.info(f"Company: {company_name}")
            logger.info(f"Phone: {phone or 'Not provided'}")
            logger.info(f"{'='*70}")
            
            # Step 1: Fetch form page
            logger.info("Step 1: Fetching form page...")

            max_fetch_retries = 3
            fetch_attempt = 0
            html_content = None
            status_code = None
            final_url = None
            error = None

            while fetch_attempt < max_fetch_retries:
                html_content, status_code, final_url, error = self.fetcher.fetch_page(form_url)
                
                if html_content and status_code == 200:
                    break
                
                if fetch_attempt < max_fetch_retries - 1:
                    wait_time = 2 ** fetch_attempt  # Exponential: 1, 2, 4 seconds
                    logger.warning(f"Fetch failed (attempt {fetch_attempt+1}/{max_fetch_retries}), "
                                f"retrying in {wait_time}s...")
                    time.sleep(wait_time)
                
                fetch_attempt += 1

            if not html_content or status_code != 200:
                result['error'] = f"Could not fetch form after {max_fetch_retries} attempts: HTTP {status_code}"
                logger.error(result['error'])
                result['submission_status'] = 'fetch_error'
                self._log_submission(result)
                return result
            
            # Ensure HTML is UTF-8
            html_content = ensure_utf8(html_content)
            
            logger.info(f"✅ Form fetched successfully (HTTP {status_code})")
            
            # Step 2: Submit via pipeline (which includes CAPTCHA check)
            logger.info("Step 2: Submitting inquiry...")
            submission_result = self.pipeline.submit_to_form(
                form_url=final_url or form_url,
                html_content=html_content,
                company_name=company_name,
                sender_email=sender_email,
                sender_name=sender_name,
                message_body=message_body,
                phone=phone
            )
            
            # Apply per-site throttling
            parsed = urlparse(form_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            self._apply_throttle(base_url)
            
            # NEW: Check if CAPTCHA blocked submission
            if submission_result.error and 'captcha' in submission_result.error.lower():
                logger.warning(f"🔐 CAPTCHA detected - queuing for manual handling")
                
                # Extract CAPTCHA type from error message
                captcha_type = 'unknown'
                if 'recaptcha v2' in submission_result.error.lower():
                    captcha_type = 'recaptcha_v2'
                elif 'recaptcha v3' in submission_result.error.lower():
                    captcha_type = 'recaptcha_v3'
                elif 'hcaptcha' in submission_result.error.lower():
                    captcha_type = 'hcaptcha'
                elif 'image' in submission_result.error.lower():
                    captcha_type = 'image'
                
                # Add to CAPTCHA queue
                handle_captcha_form(
                    queue_manager=self.captcha_queue,
                    url=form_url,
                    form_url=final_url or form_url,
                    captcha_type=captcha_type,
                    company_name=company_name,
                    email=sender_email
                )
                
                result['submission_status'] = 'captcha_blocked'
                result['captcha_queued'] = True
                result['error'] = f"CAPTCHA detected ({captcha_type}) - queued for manual handling"
                
                logger.info(f"✅ Form queued for manual CAPTCHA handling")
                
            else:
                # No CAPTCHA - process normal result
                result['submission_result'] = submission_result.to_dict()
                result['submission_status'] = 'success' if submission_result.success else 'failed'
                
                if submission_result.success:
                    logger.info(f"✅ Submission successful!")
                else:
                    logger.warning(f"⚠️ Submission failed: {submission_result.message or submission_result.error}")
            
            # Log to database
            self._log_to_database(result, submission_result, base_url)
            
            self.results.append(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Submission error: {e}")
            import traceback
            traceback.print_exc()
            
            result['error'] = str(e)
            result['submission_status'] = 'error'
            self.results.append(result)
            
            return result
        
        finally:
            # Always log
            self._log_submission(result)
    
    def submit_bulk_inquiries(
        self,
        crawl_results: List[Dict],
        sender_email: str = "inquiry@example.com",
        sender_name: str = "お問い合わせ",
        skip_with_email: bool = True,
        rate_limit_delay: float = 2.0
    ) -> List[Dict]:
        """
        Submit inquiries to multiple forms from crawler results.
        Handles CAPTCHA exceptions gracefully by queuing them.
        
        Args:
            crawl_results: List of crawl result dictionaries
            sender_email: Email to use
            sender_name: Name of sender
            skip_with_email: Skip companies that have email addresses
            rate_limit_delay: Delay between submissions (seconds)
            
        Returns:
            List of submission results
        """
        
        submission_results = []
        total = len(crawl_results)
        
        logger.info(f"\n{'='*70}")
        logger.info(f"BULK SUBMISSION STARTING")
        logger.info(f"{'='*70}")
        logger.info(f"Total targets: {total}")
        logger.info(f"Skip with email: {skip_with_email}")
        logger.info(f"Rate limit delay: {rate_limit_delay}s")
        logger.info(f"{'='*70}\n")
        
        for i, result in enumerate(crawl_results, 1):
            url = result.get('url')
            form_url = result.get('inquiryFormUrl')
            company_name = result.get('companyName')
            email = result.get('email')
            
            # Validate inputs
            if not form_url:
                logger.warning(f"[{i}/{total}] No form URL for {url}")
                continue
            
            if not company_name:
                logger.warning(f"[{i}/{total}] No company name for {url}")
                continue
            
            # Skip if we have email and flag is set
            if skip_with_email and email:
                logger.info(f"[{i}/{total}] Skipping {company_name} - has email: {email}")
                continue
            
            logger.info(f"\n[{i}/{total}] Submitting to {company_name}")
            
            # Submit form (CAPTCHA will be queued automatically)
            submission_result = self.submit_inquiry(
                form_url=form_url,
                company_name=company_name,
                sender_email=sender_email,
                sender_name=sender_name
            )
            
            submission_results.append(submission_result)
            
            # Rate limiting (except for last item)
            if i < total:
                time.sleep(rate_limit_delay)
        
        # Print summary including CAPTCHA queue status
        self._print_bulk_summary(submission_results)
        
        return submission_results
    
    def _print_bulk_summary(self, submission_results: List[Dict]):
        """Print summary of bulk submissions including CAPTCHA stats."""
        total = len(submission_results)
        
        if total == 0:
            logger.warning("No submissions to summarize")
            return
        
        successful = sum(1 for r in submission_results if r['submission_status'] == 'success')
        failed = sum(1 for r in submission_results if r['submission_status'] == 'failed')
        errors = sum(1 for r in submission_results if r['submission_status'] == 'error')
        fetch_errors = sum(1 for r in submission_results if r['submission_status'] == 'fetch_error')
        captcha_blocked = sum(1 for r in submission_results if r['submission_status'] == 'captcha_blocked')
        
        success_rate = (successful / total * 100) if total > 0 else 0
        
        print("\n" + "=" * 70)
        print("BULK SUBMISSION SUMMARY")
        print("=" * 70)
        print(f"Total submissions:   {total}")
        print(f"  Successful:        {successful:3d} ({success_rate:5.1f}%)")
        print(f"  Failed:            {failed:3d} ({failed/total*100:5.1f}%)")
        print(f"  CAPTCHA Blocked:   {captcha_blocked:3d} ({captcha_blocked/total*100:5.1f}%)")
        print(f"  Fetch errors:      {fetch_errors:3d} ({fetch_errors/total*100:5.1f}%)")
        print(f"  Other errors:      {errors:3d} ({errors/total*100:5.1f}%)")
        print("=" * 70)
        
        if captcha_blocked > 0:
            print(f"\n🔐 {captcha_blocked} form(s) queued for manual CAPTCHA handling")
            print(f"   Use captcha_queue.export_pending_to_csv() to export")
        
        if errors > 0 or fetch_errors > 0:
            print("\n❌ Errors encountered:")
            for result in submission_results:
                if result['submission_status'] in ['error', 'fetch_error']:
                    company = result.get('company_name', 'Unknown')
                    error = result.get('error', 'Unknown error')
                    print(f"  - {company}: {error}")
        
        print("=" * 70 + "\n")
    
    def _log_submission(self, result: Dict):
        """
        Log submission result to file.
        
        Args:
            result: Submission result dictionary
        """
        try:
            # Ensure all strings are UTF-8
            result = {k: ensure_utf8(str(v)) if v else v for k, v in result.items()}
            
            with open(self.submission_log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(result, ensure_ascii=False) + '\n')
            
            logger.debug(f"Logged submission to {self.submission_log_file}")
            
        except Exception as e:
            logger.error(f"Could not log submission: {e}")
    
    def save_results(self, output_file: str = None) -> str:
        """
        Save all results to JSON file.
        
        Args:
            output_file: Output file path (auto-generated if not specified)
            
        Returns:
            Path to saved file or None on error
        """
        if output_file is None:
            output_file = f"submission_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Use safe JSON dump (ensures UTF-8, no ASCII escaping)
        success = safe_dump_json(output_file, self.results, ensure_ascii=False)
        
        if success:
            logger.info(f"✅ Results saved to: {output_file}")
            return output_file
        else:
            logger.error(f"Failed to save results to: {output_file}")
            return None
    
    def load_results(self, input_file: str) -> List[Dict]:
        """
        Load results from previous submission.
        
        Args:
            input_file: Input file path
            
        Returns:
            List of result dictionaries
        """
        results = safe_load_json(input_file, fallback=[])
        logger.info(f"Loaded {len(results)} results from {input_file}")
        return results
    
    def print_summary(self):
        """Print submission summary with statistics."""
        total = len(self.results)
        
        if total == 0:
            logger.warning("No submissions to summarize")
            return
        
        successful = sum(1 for r in self.results if r['submission_status'] == 'success')
        failed = sum(1 for r in self.results if r['submission_status'] == 'failed')
        errors = sum(1 for r in self.results if r['submission_status'] == 'error')
        fetch_errors = sum(1 for r in self.results if r['submission_status'] == 'fetch_error')
        captcha_blocked = sum(1 for r in self.results if r.get('captcha_queued', False))
        
        success_rate = (successful / total * 100) if total > 0 else 0
        
        print("\n" + "=" * 70)
        print("FORM SUBMISSION SUMMARY")
        print("=" * 70)
        print(f"Total submissions:    {total}")
        print(f"Successful:           {successful:3d} ({success_rate:5.1f}%)")
        print(f"Failed:               {failed:3d} ({failed/total*100:5.1f}%)")
        print(f"CAPTCHA Blocked:      {captcha_blocked:3d} ({captcha_blocked/total*100:5.1f}%)")
        print(f"Fetch errors:         {fetch_errors:3d} ({fetch_errors/total*100:5.1f}%)")
        print(f"Other errors:         {errors:3d} ({errors/total*100:5.1f}%)")
        print("=" * 70)
        
        if captcha_blocked > 0:
            print(f"\n🔐 CAPTCHA Queue Status:")
            self.captcha_queue.print_summary()
        
        if errors > 0 or fetch_errors > 0:
            print("\nErrors encountered:")
            for result in self.results:
                if result['submission_status'] in ['error', 'fetch_error']:
                    company = result.get('company_name', 'Unknown')
                    error = result.get('error', 'Unknown error')
                    print(f"  - {company}: {error}")
        
        print("=" * 70 + "\n")
    
    def export_captcha_queue(self, output_file: str = None) -> Optional[str]:
        """
        Export pending CAPTCHA forms to CSV.
        
        Args:
            output_file: Output CSV file path
            
        Returns:
            Path to CSV file or None
        """
        return self.captcha_queue.export_pending_to_csv(output_file)
    
    def _apply_throttle(self, base_url: str):
        """Apply per-site throttling."""
        if base_url in self._last_submission_times:
            last_time = self._last_submission_times[base_url]
            elapsed = (datetime.utcnow() - last_time).total_seconds()
            
            if elapsed < self.throttle_per_site:
                wait_time = self.throttle_per_site - elapsed
                logger.info(f"Throttling: waiting {wait_time:.1f}s for {base_url}")
                time.sleep(wait_time)
        
        self._last_submission_times[base_url] = datetime.utcnow()
    
    def _log_to_database(
        self,
        result: Dict,
        submission_result: SubmissionResult,
        base_url: str
    ):
        """Log submission to database."""
        try:
            # Determine mode
            mode = 'browser' if submission_result.submission_method == 'browser' else 'direct'
            
            # Determine send_status
            send_status = result['submission_status']
            if send_status == 'captcha_blocked':
                send_status = 'captcha_blocked'
            elif send_status == 'success':
                send_status = 'success'
            elif send_status == 'failed':
                send_status = 'failed'
            else:
                send_status = 'error'
            
            self.db.log_submission(
                form_url=result['form_url'],
                send_status=send_status,
                http_status=submission_result.response_status,
                mode=mode,
                error_reason=result.get('error') or submission_result.error,
                company_name=result.get('company_name'),
                sender_email=result.get('sender_email'),
                submission_method=submission_result.submission_method,
                verification_confidence=submission_result.verification_confidence,
                retry_count=submission_result.retry_count,
                response_url=submission_result.response_url,
                sent_at=datetime.fromisoformat(result['timestamp'].replace('Z', '+00:00')) if result.get('timestamp') else None
            )
        except Exception as e:
            logger.error(f"Failed to log to database: {e}")
    
    def close(self):
        """Clean up resources."""
        try:
            self.fetcher.close()
            self.pipeline.submitter.close()
            self.db.close()
            logger.info("FormSubmissionService closed")
        except Exception as e:
            logger.error(f"Error closing service: {e}")


# Example usage
if __name__ == "__main__":
    # Setup logging
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Load crawl results
    crawl_results = []
    crawl_file = "crawl_results_20250101_120000.jsonl"
    
    try:
        with open(crawl_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    crawl_results.append(json.loads(line))
        
        logger.info(f"Loaded {len(crawl_results)} crawl results")
        
    except FileNotFoundError:
        logger.error(f"Crawl results file not found: {crawl_file}")
        logger.error("Usage: Provide crawl_results JSONL file")
        exit(1)
    
    # Initialize service
    service = FormSubmissionService()
    
    try:
        # Submit inquiries (CAPTCHA forms will be queued automatically)
        results = service.submit_bulk_inquiries(
            crawl_results,
            sender_email="contact@yourcompany.com",
            sender_name="お問い合わせ",
            skip_with_email=True  # Don't submit forms for companies with emails
        )
        
        # Print summary (includes CAPTCHA queue status)
        service.print_summary()
        
        # Save results
        output_file = service.save_results()
        
        if output_file:
            logger.info(f"Results saved to: {output_file}")
        
        # Export CAPTCHA queue if any
        if service.captcha_queue.count_pending() > 0:
            captcha_csv = service.export_captcha_queue()
            if captcha_csv:
                logger.info(f"📄 CAPTCHA queue exported to: {captcha_csv}")
        
    except KeyboardInterrupt:
        logger.info("Submission interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always cleanup
        service.close()