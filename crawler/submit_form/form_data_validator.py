# -*- coding: utf-8 -*-
"""
Form Data Validator
Mimics JavaScript validation to format data correctly before submission
"""

import re
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class FormDataValidator:
    """Validates and formats form data to match expected formats."""
    
    # Phone number patterns (various formats)
    PHONE_PATTERNS = [
        r'^\d{10}$',  # 10 digits
        r'^\d{11}$',  # 11 digits
        r'^\d{3}-\d{4}-\d{4}$',  # 03-1234-5678
        r'^\d{2}-\d{4}-\d{4}$',  # 03-1234-5678
        r'^\d{4}-\d{2}-\d{4}$',  # 0900-12-3456
        r'^\+?\d{1,3}[\s-]?\d{1,4}[\s-]?\d{1,4}[\s-]?\d{1,9}$',  # International
    ]
    
    def __init__(self):
        """Initialize validator."""
        pass
    
    def validate_and_format(self, field_name: str, value: str, field_type: str = None) -> str:
        """
        Validate and format field value based on field name and type.
        
        Args:
            field_name: Name of the field
            field_name_lower: Lowercase field name for pattern matching
            value: Value to validate/format
            field_type: HTML field type (email, tel, etc.)
            
        Returns:
            Formatted value
        """
        if not value:
            return value
        
        field_name_lower = field_name.lower()
        value_str = str(value).strip()
        
        # Email validation/formatting
        if field_type == 'email' or 'email' in field_name_lower or 'mail' in field_name_lower:
            return self._format_email(value_str)
        
        # Phone validation/formatting
        if field_type == 'tel' or 'phone' in field_name_lower or 'tel' in field_name_lower or '電話' in field_name:
            return self._format_phone(value_str)
        
        # Name formatting (remove extra spaces, capitalize)
        if 'name' in field_name_lower or '名前' in field_name:
            return self._format_name(value_str)
        
        # Company name formatting
        if 'company' in field_name_lower or '会社' in field_name:
            return self._format_company(value_str)
        
        # Message/textarea - ensure proper line breaks
        if field_type == 'textarea' or 'message' in field_name_lower or '内容' in field_name:
            return self._format_message(value_str)
        
        return value_str
    
    def _format_email(self, email: str) -> str:
        """Format email address."""
        email = email.strip().lower()
        
        # Basic email validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            logger.warning(f"Invalid email format: {email}")
            # Try to fix common issues
            email = email.replace(' ', '')
            if '@' not in email:
                logger.error(f"Cannot fix email: {email}")
        
        return email
    
    def _format_phone(self, phone: str) -> str:
        """Format phone number."""
        # Remove all non-digit characters
        digits_only = re.sub(r'\D', '', phone)
        
        if not digits_only:
            return phone  # Return original if no digits
        
        # Japanese phone number formatting
        if len(digits_only) == 10:
            # Format: 03-1234-5678
            return f"{digits_only[:2]}-{digits_only[2:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11:
            # Format: 090-1234-5678 (mobile)
            if digits_only.startswith('0'):
                return f"{digits_only[:3]}-{digits_only[3:7]}-{digits_only[7:]}"
            else:
                return f"{digits_only[:3]}-{digits_only[3:7]}-{digits_only[7:]}"
        elif len(digits_only) >= 7 and len(digits_only) <= 15:
            # International format
            if digits_only.startswith('81'):  # Japan country code
                # Remove country code and format
                local = digits_only[2:]
                if len(local) == 10:
                    return f"0{local[:1]}-{local[1:5]}-{local[5:]}"
                elif len(local) == 9:
                    return f"0{local[:2]}-{local[2:6]}-{local[6:]}"
            
            # Generic formatting for other lengths
            if len(digits_only) == 12:
                return f"{digits_only[:4]}-{digits_only[4:8]}-{digits_only[8:]}"
        
        # If no specific format matches, return with basic formatting
        if len(digits_only) >= 10:
            # Add hyphens every 4 digits from the end
            formatted = digits_only
            if len(formatted) > 4:
                formatted = f"{formatted[:-4]}-{formatted[-4:]}"
            if len(formatted) > 9:
                parts = formatted.split('-')
                if len(parts) == 2:
                    formatted = f"{parts[0][:-4]}-{parts[0][-4:]}-{parts[1]}"
            return formatted
        
        return phone  # Return original if can't format
    
    def _format_name(self, name: str) -> str:
        """Format name (remove extra spaces, proper capitalization)."""
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name.strip())
        
        # For Japanese names, don't capitalize
        if re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]', name):
            return name  # Japanese characters - return as is
        
        # For English names, capitalize first letter of each word
        parts = name.split()
        formatted_parts = [p.capitalize() if p else '' for p in parts]
        return ' '.join(formatted_parts)
    
    def _format_company(self, company: str) -> str:
        """Format company name."""
        # Remove extra whitespace
        company = re.sub(r'\s+', ' ', company.strip())
        
        # For Japanese, return as is
        if re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]', company):
            return company
        
        # For English, capitalize appropriately
        # Keep common abbreviations uppercase
        abbreviations = ['LLC', 'Inc', 'Ltd', 'Corp', 'Co']
        parts = company.split()
        formatted_parts = []
        for part in parts:
            if part.upper() in [abbr.upper() for abbr in abbreviations]:
                formatted_parts.append(part.upper())
            else:
                formatted_parts.append(part.capitalize())
        return ' '.join(formatted_parts)
    
    def _format_message(self, message: str) -> str:
        """Format message/textarea content."""
        # Normalize line breaks
        message = message.replace('\r\n', '\n').replace('\r', '\n')
        
        # Remove excessive blank lines (more than 2 consecutive)
        message = re.sub(r'\n{3,}', '\n\n', message)
        
        # Trim whitespace from start/end
        message = message.strip()
        
        return message
    
    def validate_form_data(self, form_data: Dict, field_info: Dict[str, Dict] = None) -> Dict:
        """
        Validate and format entire form data dictionary.
        
        Args:
            form_data: Dictionary of field_name -> value
            field_info: Optional dict mapping field_name -> {'type': 'email', ...}
            
        Returns:
            Formatted form_data dictionary
        """
        formatted_data = {}
        
        for field_name, value in form_data.items():
            if value is None:
                continue
            
            # Get field type from field_info if available
            field_type = None
            if field_info and field_name in field_info:
                field_type = field_info[field_name].get('type')
            
            formatted_value = self.validate_and_format(field_name, str(value), field_type)
            formatted_data[field_name] = formatted_value
        
        return formatted_data

