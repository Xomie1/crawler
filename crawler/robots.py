"""
Robots.txt checker utility
Handles robots.txt parsing and URL permission checking.
"""

from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class RobotsChecker:
    """Handles robots.txt checking for URLs."""
    
    def __init__(self, user_agent: str = "CrawlerBot/1.0"):
        """
        Initialize robots checker.
        
        Args:
            user_agent: User agent string to use for robots.txt checks
        """
        self.user_agent = user_agent
        self._cache: dict[str, RobotFileParser] = {}
    
    def _get_robots_url(self, url: str) -> str:
        """Get the robots.txt URL for a given URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    
    def _get_parser(self, url: str) -> Optional[RobotFileParser]:
        """
        Get or create RobotFileParser for a domain.
        
        Args:
            url: URL to check
            
        Returns:
            RobotFileParser instance or None if robots.txt is inaccessible
        """
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        
        if domain not in self._cache:
            robots_url = self._get_robots_url(url)
            parser = RobotFileParser()
            parser.set_url(robots_url)
            
            try:
                parser.read()
                self._cache[domain] = parser
                logger.debug(f"Loaded robots.txt from {robots_url}")
            except Exception as e:
                logger.warning(f"Failed to load robots.txt from {robots_url}: {e}")
                return None
        
        return self._cache.get(domain)
    
    def is_allowed(self, url: str, policy: str = "respect") -> bool:
        """
        Check if a URL is allowed by robots.txt.
        
        Args:
            url: URL to check
            policy: "respect" or "ignore"
            
        Returns:
            True if allowed, False otherwise
        """
        if policy == "ignore":
            return True
        
        parser = self._get_parser(url)
        if parser is None:
            # If robots.txt is inaccessible, allow by default
            return True
        
        try:
            return parser.can_fetch(self.user_agent, url)
        except Exception as e:
            logger.warning(f"Error checking robots.txt for {url}: {e}")
            return True  # Allow by default on error

