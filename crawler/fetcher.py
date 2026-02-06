"""
Page fetcher utility
Handles HTTP requests with retry logic, redirect following, and timeout handling.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Tuple
import logging
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


class PageFetcher:
    """Handles fetching web pages with retry logic and redirect following."""
    
    def __init__(self, timeout: int = 30, max_retries: int = 3, user_agent: str = "CrawlerBot/1.0"):
        """
        Initialize page fetcher.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            user_agent: User agent string for requests
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.user_agent = user_agent
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
    
    def fetch_page(self, url: str) -> Tuple[Optional[str], int, Optional[str], Optional[str]]:
        """
        Fetch a web page with retry logic and redirect following.
        
        Args:
            url: URL to fetch
            
        Returns:
            Tuple of (content, status_code, final_url, error_message)
            - content: HTML content or None if failed
            - status_code: HTTP status code
            - final_url: Final URL after redirects
            - error_message: Error message if failed, None otherwise
        """
        try:
            response = self.session.get(
                url,
                timeout=self.timeout,
                allow_redirects=True,
                stream=False
            )
            
            final_url = response.url
            status_code = response.status_code
            
            if status_code == 200:
                # Try to decode content
                try:
                    response.encoding = response.apparent_encoding or 'utf-8'
                    content = response.text
                    logger.debug(f"Successfully fetched {url} -> {final_url}")
                    return content, status_code, final_url, None
                except Exception as e:
                    error_msg = f"Failed to decode content: {str(e)}"
                    logger.warning(f"{url}: {error_msg}")
                    return None, status_code, final_url, error_msg
            else:
                error_msg = f"HTTP {status_code}"
                logger.warning(f"{url}: {error_msg}")
                return None, status_code, final_url, error_msg
                
        except requests.exceptions.Timeout as e:
            error_msg = f"Request timeout: {str(e)}"
            logger.error(f"{url}: {error_msg}")
            return None, 0, None, error_msg
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(f"{url}: {error_msg}")
            return None, 0, None, error_msg
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {str(e)}"
            logger.error(f"{url}: {error_msg}")
            return None, 0, None, error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"{url}: {error_msg}")
            return None, 0, None, error_msg
    
    def close(self):
        """Close the session."""
        self.session.close()

