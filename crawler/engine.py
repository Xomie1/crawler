"""
Main crawler engine
Orchestrates the crawling process with AI-enhanced extraction
"""

from typing import Dict, Optional, List
import logging

from .fetcher import PageFetcher
from .parser import HTMLParser
from .robots import RobotsChecker
from .storage import CrawlResult
from crawler.extractors.email_extractor import EmailExtractor
from crawler.extractors.enhanced_contact_form_detector import EnhancedContactFormDetector
from crawler.extractors.enhanced_company_name_extractor import EnhancedCompanyNameExtractor
from crawler.extractors.improved_ai_company_extractor import ImprovedAICompanyExtractor


logger = logging.getLogger(__name__)


class CrawlerEngine:
    """Main crawler engine with AI-enhanced extraction."""
    
    def __init__(
        self,
        root_url: str,
        crawl_settings: Dict[str, int] = None,
        user_agent_policy: str = "CrawlerBot/1.0",
        robots_policy: str = "respect",
        exclude_patterns: List[str] = None,
        use_enhanced_form_detection: bool = True,
        max_form_pages: int = 15,
        use_standard_company_extractor: bool = True,
        # AI parameters
        use_ai_extraction: bool = False,
        ai_provider: str = None,
        ai_always: bool = False,
        ai_confidence_thresholds: Optional[Dict[str, float]] = None
    ):
        self.root_url = root_url
        if crawl_settings is None:
            crawl_settings = {'timeout': 30}
        self.timeout = crawl_settings.get('timeout', 30)
        self.user_agent_policy = user_agent_policy
        self.robots_policy = robots_policy
        self.exclude_patterns = exclude_patterns or []
        self.use_playwright = True
        self.use_enhanced_form_detection = use_enhanced_form_detection
        self.max_form_pages = max_form_pages
        self.use_standard_company_extractor = use_standard_company_extractor
        
        # AI settings
        self.use_ai_extraction = use_ai_extraction
        self.ai_provider = ai_provider
        self.ai_always = ai_always
        self.ai_confidence_thresholds = ai_confidence_thresholds
        
        # Initialize components
        self.fetcher = PageFetcher(
            timeout=self.timeout,
            max_retries=3,
            user_agent=self.user_agent_policy
        )
        self.robots_checker = RobotsChecker(user_agent=self.user_agent_policy)
        self.parser = HTMLParser()
        
        # Initialize hybrid extractor if AI is enabled
        self.hybrid_extractor = None
        if self.use_ai_extraction:
            try:
                from crawler.ai.hybrid_extractor import HybridExtractor
                self.hybrid_extractor = HybridExtractor(
                    ai_provider=self.ai_provider,
                    use_ai=True,
                    always_use_ai=self.ai_always,
                    confidence_thresholds=self.ai_confidence_thresholds
                )
                logger.info("✓ AI extraction enabled")
            except Exception as e:
                logger.error(f"Failed to initialize AI extraction: {e}")
                logger.warning("Continuing with rule-based extraction only")
                self.use_ai_extraction = False
        
        logger.info(f"Initialized crawler for {root_url}")
        if self.use_ai_extraction:
            logger.info(f"AI Provider: {self.ai_provider or 'groq'}")
            logger.info(f"AI Mode: {'Always' if self.ai_always else 'Hybrid'}")
    
    def crawl(self, output_file: Optional[str] = None) -> Dict:
        """Crawl the root URL once and return result."""
        logger.info(f"Starting crawl for {self.root_url}")
        
        url = self.root_url
        
        # Check exclude patterns
        if any(pattern in url for pattern in self.exclude_patterns):
            logger.warning(f"URL matches exclude pattern: {url}")
            result = CrawlResult(
                url=url,
                http_status=0,
                crawl_status="error",
                error_message="URL matches exclude pattern"
            )
            return result.to_dict()
        
        # Check robots.txt permission
        robots_allowed = self.robots_checker.is_allowed(url, self.robots_policy)
        if not robots_allowed:
            logger.warning(f"Robots.txt disallows crawling: {url}")
            result = CrawlResult(
                url=url,
                http_status=0,
                robots_allowed=False,
                crawl_status="error",
                error_message="Robots.txt disallows crawling"
            )
            return result.to_dict()
        
        logger.info(f"Crawling: {url}")
        
        # Fetch page
        content, status_code, final_url, error_message = self.fetcher.fetch_page(url)
        
        self.last_fetched_html = content
        
        final_url_to_use = final_url or url
        result = CrawlResult(
            url=final_url_to_use,
            http_status=status_code,
            robots_allowed=robots_allowed,
            crawl_status="success" if (content and status_code == 200) else "error",
            error_message=error_message
        )
        
        if not content or status_code != 200:
            logger.warning(f"Failed to fetch {url}: HTTP {status_code}")
            if output_file:
                self._write_result(result, output_file)
            return result.to_dict()
        
        # Parse HTML and extract information
        try:
            parser = HTMLParser(final_url_to_use)
            
            # ==================== EMAIL EXTRACTION ====================
            if self.use_ai_extraction and self.hybrid_extractor:
                logger.info("Using AI-enhanced email extraction...")
                
                # Get rule-based result first (unless AI-only mode)
                email_result = None
                if not self.ai_always:
                    email_extractor = EmailExtractor(
                        base_url=final_url_to_use, 
                        use_playwright=self.use_playwright
                    )
                    email_result = email_extractor.extract(content, final_url=final_url_to_use)
                    email_extractor.close()
                
                # Use hybrid extractor
                hybrid_email = self.hybrid_extractor.extract_email(
                    final_url_to_use,
                    content,
                    rule_based_result=email_result
                )
                
                result.email = hybrid_email['value']
                result.email_confidence = hybrid_email['confidence']
                result.email_used_ai = hybrid_email['used_ai']
                
                if result.email:
                    logger.info(f"Found email: {result.email} (AI: {result.email_used_ai}, confidence: {result.email_confidence:.2f})")
            else:
                # Pure rule-based mode
                email_extractor = EmailExtractor(
                    base_url=final_url_to_use, 
                    use_playwright=self.use_playwright
                )
                email_result = email_extractor.extract(content, final_url=final_url_to_use)
                
                if email_result.get('email'):
                    result.email = email_result['email']
                    result.email_confidence = email_result.get('confidence', 0.0)
                    result.email_used_ai = False
                    logger.info(f"Found email: {result.email}")
                
                email_extractor.close()
            
            # ==================== FORM DETECTION ====================
            form_detection_method = None

            if self.use_enhanced_form_detection:
                try:
                    form_detector = EnhancedContactFormDetector(
                        fetcher=self.fetcher,
                        robots_checker=self.robots_checker,
                        max_pages=self.max_form_pages
                    )
                    
                    form_result = form_detector.detect_contact_form(final_url_to_use)
                    
                    if form_result.get('form_url'):
                        result.inquiry_form_url = form_result['form_url']
                        form_detection_method = 'enhanced_detector'
                        logger.info(f"✓ Found inquiry form: {result.inquiry_form_url}")
                    else:
                        form_detection_method = 'not_found'
                        
                except Exception as e:
                    logger.error(f"Enhanced form detection failed: {e}")
                    form_detection_method = 'detection_error'
                    
                    # Fallback to basic detection
                    forms = parser.detect_forms(content)
                    if forms:
                        result.inquiry_form_url = forms[0]
                        form_detection_method = 'basic_fallback'
            else:
                # Use basic detection
                forms = parser.detect_forms(content)
                if forms:
                    result.inquiry_form_url = forms[0]
                    form_detection_method = 'basic_detector'
                else:
                    form_detection_method = 'not_found'

            # Fallback: Use Homepage if No Form Found
            if not result.inquiry_form_url:
                result.inquiry_form_url = final_url_to_use
                form_detection_method = 'homepage_fallback'

            result.form_detection_method = form_detection_method
            
            # ==================== COMPANY NAME EXTRACTION ====================
            logger.info("=" * 60)
            logger.info("📊 COMPANY NAME EXTRACTION")
            logger.info("=" * 60)
            
            try:
                # STEP 1: Enhanced rule-based extraction (10 methods)
                logger.info("\n[Step 1] Running enhanced rule-based extraction (10 methods)...")
                rule_based_extractor = EnhancedCompanyNameExtractor(
                    base_url=final_url_to_use,
                    fetcher=self.fetcher
                )
                rule_based_result = rule_based_extractor.extract(
                    content,
                    final_url=final_url_to_use
                )
                
                logger.info(f"\nRule-based result:")
                logger.info(f"  Value: {rule_based_result.get('company_name')}")
                logger.info(f"  Confidence: {rule_based_result.get('company_name_confidence'):.2f}")
                logger.info(f"  Method: {rule_based_result.get('company_name_method')}")
                logger.info(f"  Source: {rule_based_result.get('company_name_source')}")
                
                # STEP 2: Decide if we need AI
                rb_confidence = rule_based_result.get('company_name_confidence', 0.0)
                rb_value = rule_based_result.get('company_name')
                
                ai_needed = (
                    not rb_value or  # No value found
                    rb_confidence < 0.75  # Low confidence
                )
                
                if ai_needed and self.use_ai_extraction and self.hybrid_extractor:
                    logger.info(f"\n[Step 2] AI needed (value={bool(rb_value)}, conf={rb_confidence:.2f})")
                    logger.info("Using improved AI extractor...")
                    
                    improved_ai_extractor = ImprovedAICompanyExtractor(
                        self.hybrid_extractor.ai_extractor
                    )
                    
                    ai_result = improved_ai_extractor.extract_company_name(
                        final_url_to_use,
                        content,
                        rule_based_result=rule_based_result
                    )
                    
                    # Use AI result if it's better
                    if ai_result.get('value'):
                        result.company_name = ai_result['value']
                        result.company_name_confidence = ai_result['confidence']
                        result.company_name_source = ai_result['source']
                        result.company_name_used_ai = ai_result['used_ai']
                        logger.info(f"  ✓ AI result: {ai_result['value']} (conf: {ai_result['confidence']:.2f})")
                    else:
                        # AI failed, fallback to rule-based
                        result.company_name = rb_value
                        result.company_name_confidence = rb_confidence
                        result.company_name_source = rule_based_result.get('company_name_source')
                        result.company_name_used_ai = False
                        logger.info(f"  ⚠ AI failed, using rule-based: {rb_value}")
                else:
                    logger.info(f"\n[Step 2] Using rule-based result (no AI needed)")
                    result.company_name = rb_value
                    result.company_name_confidence = rb_confidence
                    result.company_name_source = rule_based_result.get('company_name_source')
                    result.company_name_used_ai = False
                
                # STEP 3: Summary
                logger.info(f"\n[Summary]")
                logger.info(f"  Final company name: {result.company_name or 'NOT FOUND'}")
                logger.info(f"  Confidence: {result.company_name_confidence:.2f}")
                logger.info(f"  AI used: {result.company_name_used_ai}")
                logger.info(f"  Candidates found: {len(rule_based_result.get('company_name_candidates', []))}")
                
            except Exception as e:
                logger.error(f"Error in company name extraction: {e}")
                import traceback
                traceback.print_exc()
                result.error_message = f"Company extraction error: {str(e)}"
                result.crawl_status = "error" 
            # ==================== INDUSTRY EXTRACTION ====================
            if self.use_ai_extraction and self.hybrid_extractor:
                logger.info("Using AI-enhanced industry extraction...")
                
                # Get rule-based result first (unless AI-only mode)
                rule_based_industry = None
                if not self.ai_always:
                    metadata = parser.extract_metadata(content)
                    rule_based_industry = metadata.get('industry')
                
                hybrid_industry = self.hybrid_extractor.extract_industry(
                    final_url_to_use,
                    content,
                    rule_based_result=rule_based_industry
                )
                
                result.industry = hybrid_industry['value']
                result.industry_confidence = hybrid_industry['confidence']
                result.industry_used_ai = hybrid_industry['used_ai']
                
                if result.industry:
                    logger.info(
                        f"Found industry: {result.industry} "
                        f"(AI: {result.industry_used_ai}, confidence: {result.industry_confidence:.2f})"
                    )
            else:
                # Pure rule-based mode
                metadata = parser.extract_metadata(content)
                if metadata.get('industry'):
                    result.industry = metadata['industry']
                    result.industry_confidence = 0.65
                    result.industry_used_ai = False
                    logger.info(f"Found industry: {result.industry}")
            
            # Track which fields used AI
            ai_fields = []
            if result.email_used_ai:
                ai_fields.append('email')
            if result.company_name_used_ai:
                ai_fields.append('company_name')
            if result.industry_used_ai:
                ai_fields.append('industry')
            
            if ai_fields:
                result.ai_extraction_method = ','.join(ai_fields)
                logger.info(f"AI used for: {result.ai_extraction_method}")
                
        except Exception as e:
            logger.error(f"Error parsing HTML for {url}: {e}")
            result.error_message = f"Parsing error: {str(e)}"
            result.crawl_status = "error"
        
        logger.info(f"Crawl completed for {url}")
        
        # Write to file if specified
        if output_file:
            self._write_result(result, output_file)
        
        return result.to_dict()
    

    def log_extraction_metrics(self, results: List[Dict]):
        """Log extraction metrics to track improvement."""
        
        total = len(results)
        successful = sum(1 for r in results if r.get('crawlStatus') == 'success')
        
        company_names_found = sum(1 for r in results if r.get('companyName'))
        company_confidence_avg = sum(
            r.get('companyNameConfidence', 0) for r in results 
            if r.get('companyName')
        ) / max(1, company_names_found)
        
        ai_used_count = sum(1 for r in results if r.get('companyNameUsedAI'))
        ai_success = sum(
            1 for r in results 
            if r.get('companyNameUsedAI') and r.get('companyName')
        )
        
        print("\n" + "=" * 70)
        print("📈 COMPANY NAME EXTRACTION METRICS")
        print("=" * 70)
        print(f"Total URLs crawled: {total}")
        print(f"Successful crawls: {successful} ({successful/total*100:.1f}%)")
        print(f"\nCompany Names Found: {company_names_found} ({company_names_found/successful*100:.1f}% of successful)")
        print(f"Average Confidence: {company_confidence_avg:.2f}")
        print(f"\nAI Fallback Used: {ai_used_count} times")
        print(f"AI Success Rate: {ai_success}/{ai_used_count} ({ai_success/max(1,ai_used_count)*100:.1f}%)")
        print("=" * 70 + "\n")
        
    def _write_result(self, result: CrawlResult, output_file: str):
        """Write result to output file."""
        import json
        try:
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(result.to_dict(), ensure_ascii=False) + '\n')
        except Exception as e:
            logger.error(f"Failed to write result to {output_file}: {e}")
    
    def close(self):
        """Clean up resources."""
        self.fetcher.close()
