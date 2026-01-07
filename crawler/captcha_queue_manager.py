# -*- coding: utf-8 -*-
"""
CAPTCHA Queue Manager - Priority 2
Handles forms with CAPTCHA by queuing them for manual intervention
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class CaptchaForm:
    """Represents a form that has CAPTCHA."""
    
    url: str
    form_url: str
    captcha_type: str  # 'recaptcha_v2', 'recaptcha_v3', 'hcaptcha', 'image'
    company_name: Optional[str] = None
    email: Optional[str] = None
    discovered_at: str = None
    status: str = 'pending'  # 'pending', 'in_progress', 'completed', 'failed'
    notes: str = None
    
    # Form details for manual submission
    form_action: Optional[str] = None
    form_method: str = 'POST'
    required_fields: List[str] = None
    
    def __post_init__(self):
        if self.discovered_at is None:
            self.discovered_at = datetime.utcnow().isoformat()
        if self.required_fields is None:
            self.required_fields = []
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


class CaptchaQueueManager:
    """Manages queue of forms with CAPTCHA for manual handling."""
    
    def __init__(self, queue_file: str = "captcha_queue.jsonl"):
        """
        Initialize CAPTCHA queue manager.
        
        Args:
            queue_file: Path to JSONL file for storing queue
        """
        self.queue_file = Path(queue_file)
        self.queue: List[CaptchaForm] = []
        
        # Load existing queue
        self._load_queue()
        
        logger.info(f"CAPTCHA Queue Manager initialized")
        logger.info(f"  Queue file: {self.queue_file}")
        logger.info(f"  Pending forms: {self.count_pending()}")
    
    def add_form(
        self,
        url: str,
        form_url: str,
        captcha_type: str,
        company_name: str = None,
        email: str = None,
        form_action: str = None,
        form_method: str = 'POST',
        required_fields: List[str] = None,
        notes: str = None
    ) -> CaptchaForm:
        """
        Add a form with CAPTCHA to the queue.
        
        Args:
            url: Main website URL
            form_url: Contact form URL
            captcha_type: Type of CAPTCHA
            company_name: Company name
            email: Email if found
            form_action: Form submission action
            form_method: Form method (POST/GET)
            required_fields: List of required field names
            notes: Additional notes
            
        Returns:
            CaptchaForm object
        """
        captcha_form = CaptchaForm(
            url=url,
            form_url=form_url,
            captcha_type=captcha_type,
            company_name=company_name,
            email=email,
            form_action=form_action,
            form_method=form_method,
            required_fields=required_fields or [],
            notes=notes
        )
        
        self.queue.append(captcha_form)
        self._save_queue()
        
        logger.info(f"✅ Added CAPTCHA form to queue: {form_url}")
        logger.info(f"   Type: {captcha_type}")
        logger.info(f"   Company: {company_name or 'Unknown'}")
        logger.info(f"   Queue size: {len(self.queue)}")
        
        return captcha_form
    
    def get_pending_forms(self) -> List[CaptchaForm]:
        """Get all pending forms (not yet processed)."""
        return [f for f in self.queue if f.status == 'pending']
    
    def get_form_by_url(self, form_url: str) -> Optional[CaptchaForm]:
        """Get form by URL."""
        for form in self.queue:
            if form.form_url == form_url:
                return form
        return None
    
    def update_status(self, form_url: str, status: str, notes: str = None) -> bool:
        """
        Update status of a form.
        
        Args:
            form_url: Form URL
            status: New status ('pending', 'in_progress', 'completed', 'failed')
            notes: Optional notes
            
        Returns:
            True if updated, False if not found
        """
        form = self.get_form_by_url(form_url)
        if form:
            form.status = status
            if notes:
                form.notes = notes
            self._save_queue()
            logger.info(f"Updated status: {form_url} -> {status}")
            return True
        return False
    
    def count_pending(self) -> int:
        """Count pending forms."""
        return len([f for f in self.queue if f.status == 'pending'])
    
    def count_completed(self) -> int:
        """Count completed forms."""
        return len([f for f in self.queue if f.status == 'completed'])
    
    def export_pending_to_csv(self, output_file: str = None) -> str:
        """
        Export pending forms to CSV for manual processing.
        
        Args:
            output_file: Output CSV file path (auto-generated if None)
            
        Returns:
            Path to output file
        """
        if output_file is None:
            output_file = f"captcha_queue_pending_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            import pandas as pd
            
            pending = self.get_pending_forms()
            
            if not pending:
                logger.warning("No pending CAPTCHA forms to export")
                return None
            
            # Convert to DataFrame
            data = [
                {
                    'URL': f.url,
                    'Form URL': f.form_url,
                    'CAPTCHA Type': f.captcha_type,
                    'Company Name': f.company_name or '',
                    'Email': f.email or '',
                    'Form Action': f.form_action or '',
                    'Form Method': f.form_method,
                    'Required Fields': ', '.join(f.required_fields) if f.required_fields else '',
                    'Discovered At': f.discovered_at,
                    'Status': f.status,
                    'Notes': f.notes or ''
                }
                for f in pending
            ]
            
            df = pd.DataFrame(data)
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"✅ Exported {len(pending)} pending forms to: {output_file}")
            return output_file
            
        except ImportError:
            logger.error("pandas not installed. Install with: pip install pandas")
            return None
        except Exception as e:
            logger.error(f"Failed to export: {e}")
            return None
    
    def print_summary(self):
        """Print summary of CAPTCHA queue."""
        total = len(self.queue)
        pending = self.count_pending()
        completed = self.count_completed()
        in_progress = len([f for f in self.queue if f.status == 'in_progress'])
        failed = len([f for f in self.queue if f.status == 'failed'])
        
        # Count by CAPTCHA type
        captcha_types = {}
        for form in self.queue:
            captcha_types[form.captcha_type] = captcha_types.get(form.captcha_type, 0) + 1
        
        print("\n" + "=" * 70)
        print("🔐 CAPTCHA QUEUE SUMMARY")
        print("=" * 70)
        print(f"Total forms:        {total}")
        print(f"  Pending:          {pending}")
        print(f"  In Progress:      {in_progress}")
        print(f"  Completed:        {completed}")
        print(f"  Failed:           {failed}")
        print("-" * 70)
        print("CAPTCHA Types:")
        for captcha_type, count in sorted(captcha_types.items()):
            print(f"  {captcha_type:20s} {count:3d} ({count/total*100:.1f}%)")
        print("=" * 70 + "\n")
    
    def _load_queue(self):
        """Load queue from JSONL file."""
        if not self.queue_file.exists():
            logger.info(f"No existing queue file found - starting fresh")
            return
        
        try:
            with open(self.queue_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        form = CaptchaForm(**data)
                        self.queue.append(form)
            
            logger.info(f"Loaded {len(self.queue)} forms from queue")
            
        except Exception as e:
            logger.error(f"Error loading queue: {e}")
    
    def _save_queue(self):
        """Save queue to JSONL file."""
        try:
            with open(self.queue_file, 'w', encoding='utf-8') as f:
                for form in self.queue:
                    f.write(json.dumps(form.to_dict(), ensure_ascii=False) + '\n')
            
            logger.debug(f"Saved queue to {self.queue_file}")
            
        except Exception as e:
            logger.error(f"Error saving queue: {e}")
    
    def clear_completed(self):
        """Remove completed forms from queue."""
        original_count = len(self.queue)
        self.queue = [f for f in self.queue if f.status != 'completed']
        removed = original_count - len(self.queue)
        
        if removed > 0:
            self._save_queue()
            logger.info(f"Removed {removed} completed forms from queue")
        
        return removed


def handle_captcha_form(
    queue_manager: CaptchaQueueManager,
    url: str,
    form_url: str,
    captcha_type: str,
    company_name: str = None,
    email: str = None,
    form_details: Dict = None
) -> None:
    """
    Convenience function to handle a form with CAPTCHA.
    
    Args:
        queue_manager: CaptchaQueueManager instance
        url: Main website URL
        form_url: Contact form URL
        captcha_type: Type of CAPTCHA detected
        company_name: Company name
        email: Email if found
        form_details: Additional form details
    """
    # Extract form details if provided
    form_action = None
    form_method = 'POST'
    required_fields = []
    
    if form_details:
        form_action = form_details.get('form_action')
        form_method = form_details.get('form_method', 'POST')
        
        # Get required fields
        if 'fields' in form_details:
            required_fields = [
                f['name'] 
                for f in form_details['fields'] 
                if f.get('required')
            ]
    
    # Generate notes
    notes = f"CAPTCHA detected during crawl. Manual intervention required."
    if email:
        notes += f" Email found: {email}"
    
    # Add to queue
    queue_manager.add_form(
        url=url,
        form_url=form_url,
        captcha_type=captcha_type,
        company_name=company_name,
        email=email,
        form_action=form_action,
        form_method=form_method,
        required_fields=required_fields,
        notes=notes
    )


# Example usage
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize queue manager
    queue = CaptchaQueueManager("captcha_queue.jsonl")
    
    # Example: Add a form with CAPTCHA
    queue.add_form(
        url="https://example.com",
        form_url="https://example.com/contact",
        captcha_type="recaptcha_v2",
        company_name="Example株式会社",
        email="info@example.com",
        form_action="https://example.com/submit",
        required_fields=['name', 'email', 'message'],
        notes="Found during batch crawl"
    )
    
    # Print summary
    queue.print_summary()
    
    # Export pending to CSV
    csv_file = queue.export_pending_to_csv()
    if csv_file:
        print(f"\n📄 Pending forms exported to: {csv_file}")