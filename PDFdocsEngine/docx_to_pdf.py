"""
Convert Word (.docx) documents to PDF.
Uses platform-appropriate tools: docx2pdf on Windows, LibreOffice on Linux/Mac.
"""
from typing import Optional
import os
import platform
import subprocess


class DocxToPdfConverter:
    """Converts Word documents to PDF."""
    
    def __init__(self, output_dir: str = "output"):
        """Initialize converter.
        
        Args:
            output_dir: Directory to save converted PDFs
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.system = platform.system()
        self._check_dependencies()
    
    def _check_dependencies(self):
        """Check if required dependencies are available."""
        if self.system == 'Windows':
            try:
                import docx2pdf
                self.converter_type = 'docx2pdf'
            except ImportError:
                raise ImportError(
                    "docx2pdf is required for PDF conversion on Windows. "
                    "Install it with: pip install docx2pdf\n"
                    "Note: Microsoft Word must be installed on Windows."
                )
        else:
            # Check for LibreOffice on Linux/Mac
            if self._has_libreoffice():
                self.converter_type = 'libreoffice'
            else:
                raise ImportError(
                    f"LibreOffice is required for PDF conversion on {self.system}. "
                    "Install it with: sudo apt-get install libreoffice (Ubuntu/Debian) "
                    "or brew install --cask libreoffice (macOS)"
                )
    
    def _has_libreoffice(self) -> bool:
        """Check if LibreOffice is installed."""
        try:
            subprocess.run(
                ['libreoffice', '--version'],
                capture_output=True,
                timeout=5
            )
            return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
    
    def convert(self, docx_path: str, pdf_path: Optional[str] = None) -> str:
        """Convert Word document to PDF.
        
        Args:
            docx_path: Path to input .docx file
            pdf_path: Optional path for output PDF (defaults to same name with .pdf extension)
            
        Returns:
            Path to generated PDF file
            
        Raises:
            FileNotFoundError: If docx_path doesn't exist
            Exception: If conversion fails
        """
        if not os.path.exists(docx_path):
            raise FileNotFoundError(f"Word document not found: {docx_path}")
        
        # Generate PDF path if not provided
        if pdf_path is None:
            pdf_path = os.path.splitext(docx_path)[0] + '.pdf'
        
        # Ensure output directory exists
        pdf_dir = os.path.dirname(pdf_path)
        if pdf_dir:
            os.makedirs(pdf_dir, exist_ok=True)
        
        try:
            if self.converter_type == 'docx2pdf':
                self._convert_with_docx2pdf(docx_path, pdf_path)
            else:  # libreoffice
                self._convert_with_libreoffice(docx_path, pdf_path)
            
            if not os.path.exists(pdf_path):
                raise Exception(f"PDF file was not created: {pdf_path}")
            
            return pdf_path
            
        except Exception as e:
            if isinstance(e, (FileNotFoundError, Exception)) and "was not created" in str(e):
                raise
            raise Exception(f"PDF conversion failed on {self.system}: {str(e)}") from e
    
    def _convert_with_docx2pdf(self, docx_path: str, pdf_path: str) -> None:
        """Convert using docx2pdf library (Windows)."""
        from docx2pdf import convert
        convert(docx_path, pdf_path)
    
    def _convert_with_libreoffice(self, docx_path: str, pdf_path: str) -> None:
        """Convert using LibreOffice command-line (Linux/Mac)."""
        output_dir = os.path.dirname(pdf_path) or "."
        
        # LibreOffice command to convert DOCX to PDF
        cmd = [
            'libreoffice',
            '--headless',
            '--convert-to', 'pdf',
            '--outdir', output_dir,
            docx_path
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                raise Exception(
                    f"LibreOffice conversion failed (return code {result.returncode}): "
                    f"{result.stderr}"
                )
        except subprocess.TimeoutExpired:
            raise Exception("LibreOffice conversion timed out after 30 seconds")
        except FileNotFoundError:
            raise Exception(
                "LibreOffice is not installed or not in PATH. "
                f"Install with: sudo apt-get install libreoffice (Ubuntu/Debian) "
                f"or brew install --cask libreoffice (macOS)"
            )

