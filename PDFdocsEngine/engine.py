"""
Main document generation engine with support for multiple document types.
"""
from typing import Dict, Optional, Any, List
from PDFdocsEngine.models import DocumentInput
from PDFdocsEngine.word_generator import WordGenerator
from PDFdocsEngine.docx_to_pdf import DocxToPdfConverter
from PDFdocsEngine.template_processor import TemplateProcessor
from PDFdocsEngine.signature_block import SignatureBlockGenerator
from PDFdocsEngine.google_docs_client import GoogleDocsClient
import os
from datetime import datetime


class DocumentEngine:
    """Main engine for generating legal documents from Google Docs templates."""
    
    def __init__(self, output_dir: str = "output", google_docs_client: Optional[GoogleDocsClient] = None):
        """Initialize the document generation engine.
        
        Args:
            output_dir: Directory to save generated documents
            google_docs_client: Optional Google Docs client (will create one if not provided)
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.word_generator = WordGenerator(output_dir)
        self.docx_to_pdf = DocxToPdfConverter(output_dir)
        self.google_docs_client = google_docs_client
    
    def _get_google_docs_client(self) -> GoogleDocsClient:
        """Get or create Google Docs client."""
        if self.google_docs_client:
            return self.google_docs_client
        
        try:
            return GoogleDocsClient()
        except Exception as e:
            raise Exception(
                f"Google Docs client not available. Please set up service account.\n"
                f"Error: {str(e)}\n"
                f"See GOOGLE_DOCS_SETUP.md for instructions."
            )
    
    def _get_document_id(self, document_type: str) -> str:
        """Get document ID from environment variable based on document type.
        
        Priority:
        1. GOOGLE_DOCS_ID_{DOCUMENT_TYPE} env var (e.g., GOOGLE_DOCS_ID_PRENUPTIAL)
        2. GOOGLE_DOCS_ID env var (fallback)
        
        Args:
            document_type: Document type (e.g., "prenuptial", "divorce")
            
        Returns:
            Document ID string
        """
        # Try document-type-specific environment variable
        env_var_name = f'GOOGLE_DOCS_ID_{document_type.upper()}'
        document_id = os.getenv(env_var_name)
        
        if not document_id:
            # Fallback to generic GOOGLE_DOCS_ID
            document_id = os.getenv('GOOGLE_DOCS_ID')
        
        if not document_id:
            raise ValueError(
                f"Document ID not provided for document type '{document_type}'. "
                f"Set GOOGLE_DOCS_ID_{document_type.upper()} environment variable, "
                f"or set GOOGLE_DOCS_ID environment variable."
            )
        
        return document_id
    
    def _process_template(self, template_content: str, document_input: DocumentInput) -> str:
        """Process template with placeholder replacement (for plain text)."""
        return TemplateProcessor.process(template_content, document_input)
    
    def _process_structured_template(self, structured_content: list, document_input: DocumentInput) -> list:
        """Process structured template with placeholder replacement."""
        from PDFdocsEngine.template_processor import TemplateProcessor
        
        processed = []
        for para_info in structured_content:
            # Create a copy to avoid modifying original
            processed_para = para_info.copy()
            
            # Process text content
            processed_text = TemplateProcessor.process(para_info['text'], document_input)
            processed_para['text'] = processed_text
            
            # Process text runs if they exist
            if 'text_runs' in para_info and para_info['text_runs']:
                processed_runs = []
                for run in para_info['text_runs']:
                    processed_run = run.copy()
                    processed_run['text'] = TemplateProcessor.process(run['text'], document_input)
                    processed_runs.append(processed_run)
                processed_para['text_runs'] = processed_runs
            
            processed.append(processed_para)
        
        return processed
    
    def _add_signatures(self, content: Any, document_input: DocumentInput, 
                       include_signatures: bool, include_witnesses: bool) -> Any:
        """Add signature block to content if requested."""
        if include_signatures:
            # Let SignatureBlockGenerator handle current date generation
            # It will automatically get current date in Tokyo timezone
            signature_block = SignatureBlockGenerator.generate(
                document_input.parties,
                include_witnesses=include_witnesses
            )
            
            # Check if content is structured (list) or plain text (string)
            if isinstance(content, list):
                # Add signature as structured paragraph
                signature_para = {
                    'text': signature_block,
                    'text_runs': [{'text': signature_block, 'font_size': 11.0, 'font_family': 'Arial', 'bold': False, 'italic': False, 'underline': False}],
                    'font_size': 11.0,
                    'font_family': 'Arial',
                    'bold': False,
                    'italic': False,
                    'underline': False,
                    'alignment': 'LEFT',
                    'line_spacing': 1.15,
                    'space_before': 12.0,
                    'space_after': 6.0,
                    'heading': None
                }
                content.append(signature_para)
            else:
                # Plain text
                content += "\n\n" + signature_block
        return content
    
    def _generate_files(self, content: Any, filename_base: str, document_type: str) -> Dict[str, str]:
        """Generate Word file first, then convert to PDF to ensure identical outputs.
        
        This approach ensures that the PDF and Word documents are exactly the same,
        as the PDF is generated directly from the Word document.
        """
        # Generate Word document first
        docx_path = self.word_generator.generate(content, filename_base)
        
        # Convert Word to PDF (ensures identical output)
        pdf_path = self.docx_to_pdf.convert(docx_path)
        
        return {
            "pdf_path": pdf_path,
            "docx_path": docx_path,
            "document_type": document_type
        }
    
    def prenuptial_engine(self, input_data: Dict) -> Dict[str, str]:
        """Generate prenuptial agreement document.
        
        Args:
            input_data: Dictionary containing document input data
            
        Returns:
            Dictionary with paths to generated PDF and Word files
        """
        # Validate input
        document_input = DocumentInput.from_json(input_data)
        if document_input.document_type != "prenuptial":
            raise ValueError("Document type must be 'prenuptial' for prenuptial_engine")
        
        # Get options
        include_signatures = input_data.get('include_signatures', True)
        include_witnesses = input_data.get('include_witnesses', False)
        filename = input_data.get('filename')
        
        # Get document ID from environment variable (document-type-specific)
        document_id = self._get_document_id("prenuptial")
        
        # Get Google Docs client and fetch template with formatting
        client = self._get_google_docs_client()
        structured_content = client.get_document_structured_content(document_id)
        
        # Process template (replace placeholders in structured content)
        processed_content = self._process_structured_template(structured_content, document_input)
        
        # Add signatures
        processed_content = self._add_signatures(
            processed_content, document_input, include_signatures, include_witnesses
        )
        
        # Generate filename
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"prenuptial_{timestamp}"
        
        # Generate files
        return self._generate_files(processed_content, filename, "prenuptial")
    
    def divorce_engine(self, input_data: Dict) -> Dict[str, str]:
        """Generate divorce settlement agreement document.
        
        Args:
            input_data: Dictionary containing document input data
            
        Returns:
            Dictionary with paths to generated PDF and Word files
        """
        # Validate input
        document_input = DocumentInput.from_json(input_data)
        if document_input.document_type != "divorce":
            raise ValueError("Document type must be 'divorce' for divorce_engine")
        
        # Get options
        include_signatures = input_data.get('include_signatures', True)
        include_witnesses = input_data.get('include_witnesses', False)
        filename = input_data.get('filename')
        
        # Get document ID from environment variable (document-type-specific)
        document_id = self._get_document_id("divorce")
        
        # Get Google Docs client and fetch template with formatting
        client = self._get_google_docs_client()
        structured_content = client.get_document_structured_content(document_id)
        
        # Process template (replace placeholders in structured content)
        processed_content = self._process_structured_template(structured_content, document_input)
        
        # Add signatures
        processed_content = self._add_signatures(
            processed_content, document_input, include_signatures, include_witnesses
        )
        
        # Generate filename
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"divorce_{timestamp}"
        
        # Generate files
        return self._generate_files(processed_content, filename, "divorce")
    
    def generate(self, input_data: Dict) -> Dict[str, str]:
        """Generate documents from Google Docs template.
        
        Routes to the appropriate engine based on document_type in input_data.
        
        Supported document types:
        - "prenuptial" -> prenuptial_engine()
        - "divorce" -> divorce_engine()
        - Add more engines as needed
        
        Args:
            input_data: Dictionary containing all document generation data:
                - document_type: "prenuptial", "divorce", or other supported types
                - parties: List of parties
                - options: Document options
                - custom_values: Custom values for placeholders
                - include_signatures: bool (optional, default True)
                - include_witnesses: bool (optional, default False)
                - filename: str (optional)
                - document_id is read from GOOGLE_DOCS_ID_{DOCUMENT_TYPE} or GOOGLE_DOCS_ID env var
            
        Returns:
            Dictionary with paths to generated PDF and Word files:
            {
                "pdf_path": "...",
                "docx_path": "...",
                "document_type": "..."
            }
        """
        # Validate input to get document_type
        try:
            document_input = DocumentInput.from_json(input_data)
        except Exception as e:
            raise ValueError(f"Invalid input data: {str(e)}")
        
        document_type = document_input.document_type
        
        # Route to appropriate engine based on document type
        if document_type == "prenuptial":
            return self.prenuptial_engine(input_data)
        elif document_type == "divorce":
            return self.divorce_engine(input_data)
        else:
            raise ValueError(
                f"Unknown document type: {document_type}. "
                f"Supported types: 'prenuptial', 'divorce'. "
                f"To add a new document type, create a new engine method (e.g., '{document_type}_engine')."
            )
