# -*- coding: utf-8 -*-
"""
Browser-based Form Submitter using Playwright
Handles forms that require JavaScript execution, Cloudflare protection, etc.
"""

import logging
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class BrowserFormSubmitter:
    """
    Submit forms using headless browser (Playwright).
    Used as fallback when Direct POST fails or for JS-heavy forms.
    """
    
    def __init__(self, timeout: int = 30, headless: bool = True):
        """
        Initialize browser form submitter.
        
        Args:
            timeout: Page load timeout in seconds
            headless: Run browser in headless mode
        """
        self.timeout = timeout
        self.headless = headless
        self._playwright = None
        self._browser = None
        self._context = None
    
    def _ensure_browser(self):
        """Ensure browser is initialized."""
        try:
            from playwright.sync_api import sync_playwright
            
            if not self._playwright:
                self._playwright = sync_playwright().start()
            
            if not self._browser:
                self._browser = self._playwright.chromium.launch(
                    headless=self.headless,
                    args=['--disable-blink-features=AutomationControlled']
                )
            
            if not self._context:
                self._context = self._browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
            
            return True
        except ImportError:
            logger.error("Playwright not installed. Install with: pip install playwright && playwright install chromium")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            return False
    
    def submit_form(
        self,
        form_url: str,
        form_data: Dict,
        html_content: str = None
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Submit form using browser.
        
        Args:
            form_url: URL of the form
            form_data: Data to submit (dict of field_name -> value)
            html_content: Optional HTML content (will fetch if not provided)
            
        Returns:
            Tuple of (result_dict, error_message)
            result_dict contains: success, response_url, response_content, http_status
        """
        if not self._ensure_browser():
            return None, "Browser initialization failed"
        
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeout
            
            page = self._context.new_page()
            
            try:
                # Navigate to form page
                logger.info(f"Loading form page: {form_url}")
                page.goto(form_url, wait_until='networkidle', timeout=self.timeout * 1000)
                
                # Wait a bit for JS to execute
                time.sleep(2)
                
                # Fill form fields
                logger.info(f"Filling {len(form_data)} fields...")
                filled_count = 0
                
                for field_name, value in form_data.items():
                    try:
                        # Try multiple selectors
                        selectors = [
                            f'input[name="{field_name}"]',
                            f'textarea[name="{field_name}"]',
                            f'select[name="{field_name}"]',
                            f'input[id="{field_name}"]',
                            f'textarea[id="{field_name}"]',
                            f'#{field_name}',
                        ]
                        
                        filled = False
                        for selector in selectors:
                            try:
                                element = page.query_selector(selector)
                                if element:
                                    # Check if it's visible
                                    if element.is_visible():
                                        element.fill(str(value))
                                        filled = True
                                        filled_count += 1
                                        logger.debug(f"  ✓ Filled {field_name}")
                                        break
                            except Exception:
                                continue
                        
                        if not filled:
                            logger.debug(f"  ✗ Could not fill {field_name}")
                    except Exception as e:
                        logger.debug(f"  ✗ Error filling {field_name}: {e}")
                        continue
                
                logger.info(f"Filled {filled_count}/{len(form_data)} fields")
                
                # Submit form
                logger.info("Submitting form...")
                
                # Try to find and click submit button
                submit_selectors = [
                    'input[type="submit"]',
                    'button[type="submit"]',
                    'button:has-text("送信")',
                    'button:has-text("Submit")',
                    'button:has-text("送信する")',
                    'form button',
                ]
                
                submitted = False
                for selector in submit_selectors:
                    try:
                        button = page.query_selector(selector)
                        if button and button.is_visible():
                            # Wait for navigation
                            with page.expect_navigation(timeout=30000, wait_until='networkidle'):
                                button.click()
                            submitted = True
                            logger.info("✓ Form submitted via button click")
                            break
                    except Exception:
                        continue
                
                # If no button found, try form.submit()
                if not submitted:
                    try:
                        form = page.query_selector('form')
                        if form:
                            with page.expect_navigation(timeout=30000, wait_until='networkidle'):
                                page.evaluate('document.querySelector("form").submit()')
                            submitted = True
                            logger.info("✓ Form submitted via form.submit()")
                    except Exception as e:
                        logger.warning(f"Form submit failed: {e}")
                
                # Wait for response
                time.sleep(2)
                
                # Get response
                response_url = page.url
                response_content = page.content()
                response_status = 200  # Browser always returns 200 if page loads
                
                # Parse response to check success
                soup = BeautifulSoup(response_content, 'html.parser')
                content_lower = response_content.lower()
                
                # Check for success indicators
                success_keywords = [
                    'thank you', 'thanks', 'success', 'successful', 'submitted',
                    'ありがとう', '送信完了', '受信しました', '確認'
                ]
                error_keywords = [
                    'error', 'fail', 'failed', 'invalid', 'エラー', '失敗'
                ]
                
                has_success = any(kw in content_lower for kw in success_keywords)
                has_error = any(kw in content_lower for kw in error_keywords)
                
                # Check if form still present (indicates failure)
                form_still_present = bool(soup.find('form'))
                
                success = has_success and not has_error and not form_still_present
                
                result = {
                    'success': success,
                    'response_url': response_url,
                    'response_content': response_content[:1000],  # Limit size
                    'http_status': response_status,
                    'filled_fields': filled_count,
                    'total_fields': len(form_data),
                    'method': 'browser'
                }
                
                if success:
                    logger.info("✅ Browser submission successful")
                else:
                    logger.warning("⚠️ Browser submission unclear or failed")
                
                return result, None
                
            except PlaywrightTimeout:
                error = f"Timeout loading page: {form_url}"
                logger.error(error)
                return None, error
            except Exception as e:
                error = f"Browser submission error: {str(e)}"
                logger.error(error)
                return None, error
            finally:
                page.close()
                
        except Exception as e:
            error = f"Browser error: {str(e)}"
            logger.error(error)
            return None, error
    
    def capture_ajax_endpoints(
        self,
        form_url: str,
        timeout: int = 10
    ) -> Dict[str, str]:
        """
        Capture AJAX endpoints by monitoring network requests.
        
        Args:
            form_url: URL of the form
            timeout: How long to monitor (seconds)
            
        Returns:
            Dict mapping endpoint URLs to request methods
        """
        if not self._ensure_browser():
            return {}
        
        endpoints = {}
        
        try:
            page = self._context.new_page()
            
            # Monitor network requests
            def handle_request(request):
                url = request.url
                method = request.method
                
                # Filter for likely form submission endpoints
                if method in ['POST', 'PUT', 'PATCH']:
                    # Check if it's not a static resource
                    if not any(ext in url.lower() for ext in ['.js', '.css', '.png', '.jpg', '.gif', '.ico']):
                        endpoints[url] = method
                        logger.debug(f"Captured AJAX endpoint: {method} {url}")
            
            page.on('request', handle_request)
            
            # Load page
            page.goto(form_url, wait_until='networkidle', timeout=self.timeout * 1000)
            
            # Wait for potential AJAX calls
            time.sleep(timeout)
            
            page.close()
            
            logger.info(f"Captured {len(endpoints)} AJAX endpoints")
            return endpoints
            
        except Exception as e:
            logger.error(f"Failed to capture AJAX endpoints: {e}")
            return {}
    
    def close(self):
        """Close browser and cleanup."""
        try:
            if self._context:
                self._context.close()
                self._context = None
            if self._browser:
                self._browser.close()
                self._browser = None
            if self._playwright:
                self._playwright.stop()
                self._playwright = None
            logger.debug("Browser closed")
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

