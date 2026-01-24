"""
Google Docs API client using Service Account authentication.
Uses JSON content from environment variables.
"""
from typing import Optional, Dict, Any, List
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class GoogleDocsClient:
    """Client for interacting with Google Docs API using Service Account.
    
    Service accounts are ideal for server-to-server authentication without user interaction.
    """
    
    # Scopes required for Google Docs API
    SCOPES = ['https://www.googleapis.com/auth/documents.readonly']
    
    def __init__(self, service_account_json: Optional[str] = None):
        """
        Initialize Google Docs client with Service Account.
        
        Args:
            service_account_json: Service account JSON content as string.
                                 If None, reads from GOOGLE_SERVICE_ACCOUNT_JSON env var.
        
        Example:
            # From environment variable (recommended)
            client = GoogleDocsClient()
            
            # Or directly pass JSON string
            client = GoogleDocsClient(service_account_json='{"type":"service_account",...}')
        
        Note:
            The JSON should be converted to a single-line string and added to .env file.
            See GOOGLE_DOCS_SETUP.md for instructions on converting the JSON file.
        """
        # Get service account JSON from parameter or env var
        if service_account_json is None:
            service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        
        if not service_account_json:
            raise ValueError(
                "Service account JSON not provided. "
                "Set GOOGLE_SERVICE_ACCOUNT_JSON environment variable in your .env file.\n"
                "Use convert_json_to_env.py to convert your JSON file to env format."
            )
        
        # Parse JSON string (assumes it's already properly formatted from .env)
        service_account_info = json.loads(service_account_json)
        
        # Create credentials from service account info
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=self.SCOPES
        )
        
        # Build the service
        self.service = build('docs', 'v1', credentials=credentials)
        self.service_account_email = service_account_info.get('client_email')
    
    def get_document_content(self, document_id: str) -> str:
        """
        Fetch document content from Google Docs (plain text for backward compatibility).
        
        Args:
            document_id: Google Docs document ID (from URL)
            
        Returns:
            Plain text content of the document
        """
        structured_content = self.get_document_structured_content(document_id)
        # Convert to plain text for backward compatibility
        text_parts = []
        for para in structured_content:
            text_parts.append(para['text'])
        return '\n'.join(text_parts)
    
    def get_document_structured_content(self, document_id: str) -> List[Dict[str, Any]]:
        """
        Fetch document content with formatting information from Google Docs.
        
        Args:
            document_id: Google Docs document ID (from URL)
            
        Returns:
            List of paragraph dictionaries with text and formatting:
            [
                {
                    'text': 'paragraph text',
                    'font_size': 12.0,
                    'font_family': 'Arial',
                    'bold': False,
                    'italic': False,
                    'underline': False,
                    'alignment': 'LEFT',
                    'line_spacing': 1.15,
                    'space_before': 0.0,
                    'space_after': 6.0,
                    'heading': None or 'HEADING_1', 'HEADING_2', etc.
                },
                ...
            ]
        """
        try:
            # Get document
            doc = self.service.documents().get(documentId=document_id).execute()
            
            # Get named styles for reference
            named_styles = doc.get('namedStyles', {}).get('styles', [])
            style_map = {}
            for style in named_styles:
                style_map[style.get('namedStyleType', '')] = style.get('paragraphStyle', {})
            
            # Extract structured content
            content = doc.get('body', {}).get('content', [])
            paragraphs = []
            
            def extract_paragraph_formatting(para_elem: Dict) -> Dict[str, Any]:
                """Extract formatting from paragraph element."""
                para = para_elem.get('paragraph', {})
                para_style = para.get('paragraphStyle', {})
                named_style_type = para.get('namedStyleType', '')
                
                # Get default formatting from named style if available
                default_style = style_map.get(named_style_type, {})
                
                # Extract text runs with formatting
                text_runs = []
                if 'elements' in para:
                    for elem in para['elements']:
                        if 'textRun' in elem:
                            text_run = elem['textRun']
                            content = text_run.get('content', '')
                            # Remove NULL bytes and problematic control characters
                            content = content.replace('\x00', '').replace('\x0B', '').replace('\x0C', '')
                            
                            # Get text run formatting
                            text_style = text_run.get('textStyle', {})
                            font_size = text_style.get('fontSize', {}).get('magnitude', None)
                            font_family = text_style.get('weightedFontFamily', {}).get('fontFamily', None)
                            bold = text_style.get('bold', False)
                            italic = text_style.get('italic', False)
                            underline = text_style.get('underline', False)
                            
                            text_runs.append({
                                'text': content,
                                'font_size': font_size,
                                'font_family': font_family,
                                'bold': bold,
                                'italic': italic,
                                'underline': underline
                            })
                
                # Combine text runs
                full_text = ''.join([run['text'] for run in text_runs])
                if not full_text.strip():
                    return None
                
                # Get paragraph-level formatting
                alignment = para_style.get('alignment', 'LEFT')
                line_spacing = para_style.get('lineSpacing', {})
                line_spacing_value = None
                if line_spacing:
                    if 'magnitude' in line_spacing:
                        line_spacing_value = line_spacing['magnitude']
                    elif 'spacingMode' in line_spacing:
                        # Use default spacing
                        line_spacing_value = 1.15
                
                space_before = para_style.get('spaceAbove', {}).get('magnitude', 0.0)
                space_after = para_style.get('spaceBelow', {}).get('magnitude', 0.0)
                
                # Get font size from first text run or paragraph style
                first_font_size = text_runs[0]['font_size'] if text_runs and text_runs[0]['font_size'] else None
                if not first_font_size:
                    first_font_size = default_style.get('fontSize', {}).get('magnitude', 11.0)
                
                # Get font family from first text run
                first_font_family = text_runs[0]['font_family'] if text_runs and text_runs[0]['font_family'] else None
                if not first_font_family:
                    first_font_family = 'Arial'  # Default
                
                # Check if any text run is bold
                is_bold = any(run['bold'] for run in text_runs) if text_runs else False
                
                return {
                    'text': full_text,
                    'text_runs': text_runs,  # Keep individual runs for detailed formatting
                    'font_size': first_font_size,
                    'font_family': first_font_family,
                    'bold': is_bold,
                    'italic': any(run['italic'] for run in text_runs) if text_runs else False,
                    'underline': any(run['underline'] for run in text_runs) if text_runs else False,
                    'alignment': alignment,
                    'line_spacing': line_spacing_value or 1.15,
                    'space_before': space_before,
                    'space_after': space_after,
                    'heading': named_style_type if named_style_type.startswith('HEADING_') else None
                }
            
            def extract_text(element):
                """Recursively extract text from document elements."""
                if 'paragraph' in element:
                    para_info = extract_paragraph_formatting(element)
                    if para_info:
                        paragraphs.append(para_info)
                
                elif 'table' in element:
                    # Handle tables - extract cell text
                    table = element['table']
                    if 'tableRows' in table:
                        for row in table['tableRows']:
                            if 'tableCells' in row:
                                row_texts = []
                                for cell in row['tableCells']:
                                    if 'content' in cell:
                                        for cell_elem in cell['content']:
                                            if 'paragraph' in cell_elem:
                                                para_info = extract_paragraph_formatting(cell_elem)
                                                if para_info:
                                                    row_texts.append(para_info['text'])
                                if row_texts:
                                    # Create a simple paragraph for table row
                                    paragraphs.append({
                                        'text': ' | '.join(row_texts),
                                        'text_runs': [],
                                        'font_size': 11.0,
                                        'font_family': 'Arial',
                                        'bold': False,
                                        'italic': False,
                                        'underline': False,
                                        'alignment': 'LEFT',
                                        'line_spacing': 1.15,
                                        'space_before': 0.0,
                                        'space_after': 6.0,
                                        'heading': None
                                    })
            
            # Process all content elements
            for element in content:
                extract_text(element)
            
            return paragraphs
            
        except HttpError as error:
            if error.resp.status == 403:
                raise PermissionError(
                    f"Access denied to document {document_id}. "
                    f"Please share the document with the service account: {self.service_account_email}"
                )
            raise Exception(f"Error fetching document: {error}")
    
    def get_document_metadata(self, document_id: str) -> Dict[str, Any]:
        """
        Get document metadata.
        
        Args:
            document_id: Google Docs document ID
            
        Returns:
            Dictionary with document metadata
        """
        try:
            doc = self.service.documents().get(documentId=document_id).execute()
            return {
                'title': doc.get('title', ''),
                'document_id': document_id,
                'revision_id': doc.get('revisionId', '')
            }
        except HttpError as error:
            if error.resp.status == 403:
                raise PermissionError(
                    f"Access denied to document {document_id}. "
                    f"Please share the document with the service account: {self.service_account_email}"
                )
            raise Exception(f"Error fetching document metadata: {error}")
    
    def get_service_account_email(self) -> str:
        """
        Get the service account email address.
        
        Use this email to share Google Docs documents with the service account.
        
        Returns:
            Service account email address
        """
        return self.service_account_email
