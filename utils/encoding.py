# -*- coding: utf-8 -*-
"""
Encoding Utilities Module
Handles UTF-8 encoding/decoding with mojibake recovery

Usage:
    from utils.encoding import ensure_utf8, safe_read, safe_write
"""

import logging
import json
from typing import Optional, Union, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class EncodingError(Exception):
    """Raised when encoding cannot be fixed."""
    pass


def ensure_utf8(text: Union[str, bytes]) -> str:
    """
    Ensure text is properly UTF-8 encoded.
    Handles mojibake recovery from Latin-1 encoding.
    
    Args:
        text: String or bytes to normalize
        
    Returns:
        Properly UTF-8 encoded string
        
    Example:
        >>> text = "å¯ã„ã‚ã‚ŸãŸ"  # Mojibake
        >>> fixed = ensure_utf8(text)
        >>> print(fixed)  # ありがとうございました (correct!)
    """
    if isinstance(text, bytes):
        # Bytes: try UTF-8 first, then Latin-1
        try:
            return text.decode('utf-8')
        except UnicodeDecodeError:
            try:
                logger.debug("UTF-8 decode failed, trying Latin-1...")
                return text.decode('latin-1')
            except UnicodeDecodeError:
                logger.error(f"Could not decode bytes: {text[:50]}")
                return text.decode('utf-8', errors='ignore')
    
    if not isinstance(text, str):
        return str(text)
    
    # String: check if it's valid UTF-8
    try:
        # If this succeeds, it's already valid UTF-8
        text.encode('utf-8').decode('utf-8')
        return text
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    
    # Try to fix mojibake (UTF-8 decoded as Latin-1)
    try:
        fixed = text.encode('latin-1').decode('utf-8')
        logger.warning(f"Fixed mojibake: '{text[:30]}' -> '{fixed[:30]}'")
        return fixed
    except (UnicodeDecodeError, UnicodeEncodeError, AttributeError):
        logger.error(f"Could not fix encoding for: {text[:50]}")
        # Last resort: ignore bad characters
        return text.encode('utf-8', errors='ignore').decode('utf-8')


def safe_read_file(
    filepath: Union[str, Path],
    encoding: str = 'utf-8',
    fallback_encoding: str = 'latin-1'
) -> str:
    """
    Safely read file with automatic encoding detection and recovery.
    
    Args:
        filepath: Path to file
        encoding: Primary encoding to try (default: utf-8)
        fallback_encoding: Fallback encoding (default: latin-1)
        
    Returns:
        File contents as string
        
    Raises:
        FileNotFoundError: If file doesn't exist
        EncodingError: If file cannot be decoded
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    # Try primary encoding
    try:
        with open(filepath, 'r', encoding=encoding) as f:
            content = f.read()
        logger.debug(f"Read {filepath} with {encoding}")
        return content
    except (UnicodeDecodeError, LookupError) as e:
        logger.debug(f"Failed to read {filepath} with {encoding}: {e}")
    
    # Try fallback encoding
    try:
        with open(filepath, 'r', encoding=fallback_encoding) as f:
            content = f.read()
        # Try to fix mojibake
        content = ensure_utf8(content)
        logger.warning(f"Read {filepath} with {fallback_encoding} and fixed mojibake")
        return content
    except (UnicodeDecodeError, LookupError) as e:
        logger.error(f"Failed to read {filepath} with {fallback_encoding}: {e}")
    
    # Last resort: read with error ignoring
    try:
        with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
            content = f.read()
        logger.warning(f"Read {filepath} with error='ignore'")
        return content
    except Exception as e:
        raise EncodingError(f"Could not read {filepath}: {e}")


def safe_write_file(
    filepath: Union[str, Path],
    content: str,
    encoding: str = 'utf-8',
    ensure_ascii: bool = False
) -> bool:
    """
    Safely write file with UTF-8 encoding.
    
    Args:
        filepath: Path to file
        content: Content to write
        encoding: Encoding to use (default: utf-8)
        ensure_ascii: If True, escape non-ASCII (default: False for Japanese)
        
    Returns:
        True if successful
    """
    try:
        # Ensure content is UTF-8
        content = ensure_utf8(content)
        
        # Write with UTF-8
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding=encoding) as f:
            f.write(content)
        
        logger.debug(f"Wrote {filepath} with {encoding}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to write {filepath}: {e}")
        return False


def safe_load_json(
    filepath: Union[str, Path],
    fallback: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Safely load JSON file with encoding recovery.
    
    Args:
        filepath: Path to JSON file
        fallback: Fallback value if file cannot be loaded
        
    Returns:
        Dictionary loaded from JSON
    """
    try:
        content = safe_read_file(filepath)
        return json.loads(content)
    except FileNotFoundError:
        logger.warning(f"JSON file not found: {filepath}")
        return fallback or {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filepath}: {e}")
        return fallback or {}
    except Exception as e:
        logger.error(f"Error loading JSON {filepath}: {e}")
        return fallback or {}


def safe_dump_json(
    filepath: Union[str, Path],
    data: Dict[str, Any],
    ensure_ascii: bool = False,
    indent: int = 2
) -> bool:
    """
    Safely dump dictionary to JSON with UTF-8 encoding.
    
    Args:
        filepath: Path to JSON file
        data: Dictionary to save
        ensure_ascii: If True, escape non-ASCII (default: False for Japanese)
        indent: JSON indentation level
        
    Returns:
        True if successful
    """
    try:
        json_str = json.dumps(
            data,
            ensure_ascii=ensure_ascii,
            indent=indent,
            default=str  # Fallback for non-serializable objects
        )
        return safe_write_file(filepath, json_str, encoding='utf-8')
    except Exception as e:
        logger.error(f"Error dumping JSON to {filepath}: {e}")
        return False


def fix_html_encoding(html_content: str) -> str:
    """
    Fix encoding in HTML content.
    
    Args:
        html_content: HTML string (potentially mojibake)
        
    Returns:
        Fixed HTML string
    """
    return ensure_utf8(html_content)


def get_response_text(response) -> str:
    """
    Safely extract text from requests.Response object.
    
    Args:
        response: requests.Response object
        
    Returns:
        Response text as properly UTF-8 string
    """
    try:
        # Try to set encoding explicitly
        response.encoding = response.apparent_encoding or 'utf-8'
        text = response.text
        return ensure_utf8(text)
    except Exception as e:
        logger.debug(f"Error getting response text: {e}")
        # Fallback to binary and decode
        try:
            return response.content.decode('utf-8', errors='ignore')
        except Exception:
            return str(response.content)


def safe_format_string(format_str: str, **kwargs) -> str:
    """
    Safely format string with UTF-8 values.
    
    Args:
        format_str: Format string
        **kwargs: Values to insert (will be ensured UTF-8)
        
    Returns:
        Formatted string
    """
    # Ensure all values are UTF-8
    safe_kwargs = {k: ensure_utf8(str(v)) for k, v in kwargs.items()}
    return format_str.format(**safe_kwargs)


# Common Japanese patterns for validation
JAPANESE_PATTERNS = {
    'hiragana': r'[\u3040-\u309f]',      # ぁ-ん
    'katakana': r'[\u30a0-\u30ff]',      # ァ-ン
    'kanji': r'[\u4e00-\u9fff]',         # 一-龥
    'japanese': r'[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9fff]',
}


def has_japanese(text: str) -> bool:
    """Check if text contains Japanese characters."""
    import re
    return bool(re.search(JAPANESE_PATTERNS['japanese'], text))


def is_mojibake(text: str) -> bool:
    """
    Detect if text appears to be mojibake.
    
    Args:
        text: Text to check
        
    Returns:
        True if text appears to be mojibake
    """
    try:
        # Valid UTF-8 should pass this
        text.encode('utf-8').decode('utf-8')
        return False
    except (UnicodeDecodeError, UnicodeEncodeError):
        return True


# Example usage
if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Test cases
    print("=" * 70)
    print("ENCODING UTILITY TESTS")
    print("=" * 70)
    
    # Test 1: Fix mojibake
    mojibake_text = "å¯ã„ã‚ã‚ŸãŸ"
    fixed = ensure_utf8(mojibake_text)
    print(f"\nTest 1: Fix mojibake")
    print(f"  Input:  {mojibake_text}")
    print(f"  Output: {fixed}")
    print(f"  Correct: {fixed == 'ありがとうございました'}")
    
    # Test 2: Check if text is mojibake
    print(f"\nTest 2: Detect mojibake")
    print(f"  Is mojibake: {is_mojibake(mojibake_text)}")
    print(f"  Is valid UTF-8: {not is_mojibake(fixed)}")
    
    # Test 3: Check for Japanese
    print(f"\nTest 3: Detect Japanese")
    jp_text = "会社名"
    print(f"  '{jp_text}' has Japanese: {has_japanese(jp_text)}")
    print(f"  'Hello' has Japanese: {has_japanese('Hello')}")
    
    print("\n" + "=" * 70)