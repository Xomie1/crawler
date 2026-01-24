"""
Signature block generator for legal documents.
Supports Japanese format with automatic date generation.
"""
from typing import List, Optional
from PDFdocsEngine.models import Party
from datetime import datetime
import pytz


class SignatureBlockGenerator:
    """Generates signature blocks for parties."""
    
    @staticmethod
    def _convert_to_japanese_era(date_obj: Optional[datetime] = None) -> str:
        """Convert date to Japanese era format (令和年 月 日).
        
        Uses Asia/Tokyo timezone for current date.
        Always gets fresh current date when date_obj is None.
        
        Args:
            date_obj: Date to convert (defaults to current date in Tokyo timezone)
            
        Returns:
            Japanese era date string (e.g., "令和6年1月15日")
        """
        # Get current date in Tokyo timezone if not provided
        # IMPORTANT: Call datetime.now() fresh each time, don't cache
        if date_obj is None:
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            # Get fresh current datetime in Tokyo timezone
            date_obj = datetime.now(tokyo_tz)
        else:
            # Handle timezone conversion for provided date
            tokyo_tz = pytz.timezone('Asia/Tokyo')
            if date_obj.tzinfo is None:
                # Timezone-naive: assume it's in Tokyo timezone
                date_obj = tokyo_tz.localize(date_obj)
            else:
                # Timezone-aware: convert to Tokyo timezone
                date_obj = date_obj.astimezone(tokyo_tz)
        
        # Extract date components (use .date() to get date-only, avoiding time issues)
        if isinstance(date_obj, datetime):
            date_only = date_obj.date()
        else:
            date_only = date_obj
        
        year = date_only.year
        month = date_only.month
        day = date_only.day
        
        # Reiwa era started on May 1, 2019
        # Reiwa year = Western year - 2018
        if year >= 2019:
            if year == 2019 and month < 5:
                # Before Reiwa started, use Heisei
                # Heisei year = Western year - 1988
                era_name = "平成"
                era_year = year - 1988
            else:
                era_name = "令和"
                if year == 2019 and month == 5:
                    era_year = 1
                else:
                    era_year = year - 2018
        else:
            # Before Reiwa, use Heisei
            era_name = "平成"
            era_year = year - 1988
        
        return f"{era_name}{era_year}年{month}月{day}日"
    
    @staticmethod
    def generate(parties: List[Party], include_witnesses: bool = False, 
                 date: Optional[datetime] = None) -> str:
        """Generate signature block for parties in Japanese format.
        
        Args:
            parties: List of parties to include in signature block
            include_witnesses: Whether to include witness signature lines
            date: Optional date to use (defaults to current date)
            
        Returns:
            Formatted signature block string in Japanese format
        """
        signature_lines = []
        
        # Generate Japanese date
        japanese_date = SignatureBlockGenerator._convert_to_japanese_era(date)
        
        # Date line
        signature_lines.append(japanese_date)
        signature_lines.append("")
        signature_lines.append("")
        
        # Party labels in Japanese: 甲 (Party A), 乙 (Party B), etc.
        party_labels = ["甲", "乙", "丙", "丁", "戊", "己", "庚", "辛"]
        
        # Generate signature blocks for each party
        for i, party in enumerate(parties):
            if i < len(party_labels):
                label = party_labels[i]
            else:
                label = f"第{i+1}当事者"
            
            # Format: 甲　住所：{address}
            #         氏名：{name}　　　　　　　　　　　印
            signature_lines.append(f"{label}　住所：{party.address}")
            signature_lines.append(f"　　氏名：{party.name}　　　　　　　　　　　印")
            signature_lines.append("")
        
        # Witness signatures if requested
        if include_witnesses:
            signature_lines.append("")
            signature_lines.append("証人：")
            signature_lines.append("")
            signature_lines.append("証人1　住所：")
            signature_lines.append("　　氏名：　　　　　　　　　　　印")
            signature_lines.append("")
            signature_lines.append("証人2　住所：")
            signature_lines.append("　　氏名：　　　　　　　　　　　印")
        
        return "\n".join(signature_lines)

