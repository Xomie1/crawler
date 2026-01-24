"""
Template processor for placeholder replacement.
Replaces {{variable}} style placeholders with actual values from DocumentInput.
"""
from typing import Dict, Any
import re


class TemplateProcessor:
    """Processes templates by replacing placeholders with document input values."""
    
    @staticmethod
    def process(template_text: str, document_input: 'DocumentInput') -> str:
        """Replace placeholders in template with values from document_input.
        
        Supports placeholders like:
        - {{party1_name}} - replaced with first party's name
        - {{party2_name}} - replaced with second party's name
        - {{company_name}} - from custom_values
        - {{marriage_date}} - from custom_values
        
        Args:
            template_text: Template string with {{variable}} placeholders
            document_input: DocumentInput object with data to fill
            
        Returns:
            Processed template string with placeholders replaced
        """
        if not isinstance(template_text, str):
            return template_text
        
        result = template_text
        
        # Build replacement dictionary
        replacements = TemplateProcessor._build_replacements(document_input)
        
        # Replace all placeholders
        for placeholder, value in replacements.items():
            pattern = r'{{\s*' + re.escape(placeholder) + r'\s*}}'
            result = re.sub(pattern, str(value) if value is not None else '', result, flags=re.IGNORECASE)
        
        return result
    
    @staticmethod
    def _build_replacements(document_input: 'DocumentInput') -> Dict[str, Any]:
        """Build dictionary of placeholder -> value mappings.
        
        Args:
            document_input: DocumentInput object
            
        Returns:
            Dictionary of replacements
        """
        replacements = {}
        
        # Party information (support multiple party numbers)
        for i, party in enumerate(document_input.parties, 1):
            replacements[f'party{i}_name'] = party.name
            replacements[f'party{i}_address'] = party.address
            replacements[f'party{i}_role'] = party.role
        
        # Aliases for first two parties
        if len(document_input.parties) >= 1:
            replacements['party_a_name'] = document_input.parties[0].name
            replacements['party_a_address'] = document_input.parties[0].address
            replacements['party_a_role'] = document_input.parties[0].role
            replacements['party1_name'] = document_input.parties[0].name
            replacements['party1_address'] = document_input.parties[0].address
            replacements['party1_role'] = document_input.parties[0].role
        
        if len(document_input.parties) >= 2:
            replacements['party_b_name'] = document_input.parties[1].name
            replacements['party_b_address'] = document_input.parties[1].address
            replacements['party_b_role'] = document_input.parties[1].role
            replacements['party2_name'] = document_input.parties[1].name
            replacements['party2_address'] = document_input.parties[1].address
            replacements['party2_role'] = document_input.parties[1].role
        
        # Document options
        replacements['property_separation'] = document_input.options.property_separation
        replacements['alimony'] = document_input.options.alimony
        replacements['children'] = document_input.options.children
        
        # Custom values
        replacements.update(document_input.custom_values)
        
        # Document type
        replacements['document_type'] = document_input.document_type
        
        return replacements
