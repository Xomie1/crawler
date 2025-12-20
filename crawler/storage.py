"""
Storage utilities
Handles crawl result formatting and storage.
"""

from datetime import datetime
from typing import Optional, Dict, Any
import json
import logging

logger = logging.getLogger(__name__)


class CrawlResult:
    """Represents a crawl result."""
    
    def __init__(
        self,
        url: str,
        email: Optional[str] = None,
        inquiry_form_url: Optional[str] = None,
        company_name: Optional[str] = None,
        industry: Optional[str] = None,
        http_status: int = 0,
        robots_allowed: bool = True,
        crawl_status: str = "success",
        error_message: Optional[str] = None,
        form_detection_method: Optional[str] = None,
        # AI-related fields
        email_confidence: float = 0.0,
        email_used_ai: bool = False,
        company_name_confidence: float = 0.0,
        company_name_used_ai: bool = False,
        industry_confidence: float = 0.0,
        industry_used_ai: bool = False,
        ai_extraction_method: Optional[str] = None
    ):
        """
        Initialize crawl result.
        
        Args:
            url: Crawled URL
            email: Extracted email address
            inquiry_form_url: Detected inquiry form URL
            company_name: Extracted company name
            industry: Detected industry
            http_status: HTTP status code
            robots_allowed: Whether robots.txt allowed crawling
            crawl_status: "success", "error", or "retry"
            error_message: Error message if any
            form_detection_method: Method used to detect form
            email_confidence: Confidence score for email (0.0-1.0)
            email_used_ai: Whether AI was used for email extraction
            company_name_confidence: Confidence score for company name
            company_name_used_ai: Whether AI was used for company name
            industry_confidence: Confidence score for industry
            industry_used_ai: Whether AI was used for industry
            ai_extraction_method: Which fields used AI (e.g., "company_name,email")
        """
        self.url = url
        self.email = email
        self.inquiry_form_url = inquiry_form_url
        self.company_name = company_name
        self.industry = industry
        self.http_status = http_status
        self.robots_allowed = robots_allowed
        self.last_crawled_at = datetime.utcnow()
        self.crawl_status = crawl_status
        self.error_message = error_message
        self.form_detection_method = form_detection_method
        
        # AI-related fields
        self.email_confidence = email_confidence
        self.email_used_ai = email_used_ai
        self.company_name_confidence = company_name_confidence
        self.company_name_used_ai = company_name_used_ai
        self.company_name_source = None
        self.industry_confidence = industry_confidence
        self.industry_used_ai = industry_used_ai
        self.ai_extraction_method = ai_extraction_method
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert crawl result to dictionary.
        
        Returns:
            Dictionary representation ready for JSON serialization
        """
        return {
            'url': self.url,
            'email': self.email,
            'inquiryFormUrl': self.inquiry_form_url,
            'companyName': self.company_name,
            'industry': self.industry,
            'httpStatus': self.http_status,
            'robotsAllowed': self.robots_allowed,
            'lastCrawledAt': self.last_crawled_at.isoformat(),
            'crawlStatus': self.crawl_status,
            'errorMessage': self.error_message,
            'formDetectionMethod': self.form_detection_method,
            # AI-related fields
            'emailConfidence': round(self.email_confidence, 2) if self.email_confidence else 0.0,
            'emailUsedAI': self.email_used_ai,
            'companyNameConfidence': round(self.company_name_confidence, 2) if self.company_name_confidence else 0.0,
            'companyNameUsedAI': self.company_name_used_ai,
            'companyNameSource': self.company_name_source,
            'industryConfidence': round(self.industry_confidence, 2) if self.industry_confidence else 0.0,
            'industryUsedAI': self.industry_used_ai,
            'aiExtractionMethod': self.ai_extraction_method
        }
    
    def to_json(self) -> str:
        """
        Convert crawl result to JSON string.
        
        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


def store_crawl_result(result: CrawlResult, output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Store crawl result to file or return as dictionary.
    
    Args:
        result: CrawlResult instance
        output_file: Optional file path to append result
        
    Returns:
        Dictionary representation of the result
    """
    result_dict = result.to_dict()
    
    if output_file:
        try:
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
            logger.debug(f"Stored crawl result to {output_file}")
        except Exception as e:
            logger.error(f"Failed to store crawl result to {output_file}: {e}")
    
    return result_dict