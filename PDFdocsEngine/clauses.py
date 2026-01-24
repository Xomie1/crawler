"""
Clause branching logic system for conditional document generation.
"""
from typing import Dict, Callable, List, Any
from models import DocumentInput, Party
from datetime import datetime


class Clause:
    """Represents a document clause with condition and content."""
    
    def __init__(self, 
                 name: str,
                 condition: Callable[[DocumentInput], bool],
                 content: Callable[[DocumentInput], str]):
        """
        Initialize a clause.
        
        Args:
            name: Unique identifier for the clause
            condition: Function that returns True if clause should be included
            content: Function that generates clause content given input data
        """
        self.name = name
        self.condition = condition
        self.content = content
    
    def should_include(self, input_data: DocumentInput) -> bool:
        """Check if clause should be included based on input."""
        try:
            return self.condition(input_data)
        except Exception:
            return False
    
    def get_content(self, input_data: DocumentInput) -> str:
        """Get clause content for given input."""
        try:
            return self.content(input_data)
        except Exception as e:
            return f"[Error generating clause {self.name}: {str(e)}]"


class ClauseRegistry:
    """Registry for managing document clauses."""
    
    def __init__(self):
        """Initialize clause registry."""
        self.clauses: Dict[str, Clause] = {}
    
    def register(self, clause: Clause):
        """Register a clause."""
        self.clauses[clause.name] = clause
    
    def get_clause(self, name: str) -> Clause:
        """Get a clause by name."""
        if name not in self.clauses:
            raise ValueError(f"Clause '{name}' not found in registry")
        return self.clauses[name]
    
    def get_applicable_clauses(self, input_data: DocumentInput, clause_names: List[str]) -> List[Clause]:
        """Get clauses that should be included based on conditions."""
        applicable = []
        for name in clause_names:
            if name in self.clauses:
                clause = self.clauses[name]
                if clause.should_include(input_data):
                    applicable.append(clause)
        return applicable
    
    def generate_content(self, input_data: DocumentInput, clause_names: List[str]) -> str:
        """Generate content for applicable clauses."""
        applicable = self.get_applicable_clauses(input_data, clause_names)
        content_parts = []
        for clause in applicable:
            content_parts.append(clause.get_content(input_data))
        return "\n\n".join(content_parts)


# Helper functions for common conditions
def always_include(input_data: DocumentInput) -> bool:
    """Always include this clause."""
    return True

def has_property_separation(input_data: DocumentInput) -> bool:
    """Check if property separation option is enabled."""
    return input_data.options.property_separation

def has_alimony(input_data: DocumentInput) -> bool:
    """Check if alimony option is enabled."""
    return input_data.options.alimony

def has_children(input_data: DocumentInput) -> bool:
    """Check if children option is enabled."""
    return input_data.options.children

def is_prenuptial(input_data: DocumentInput) -> bool:
    """Check if document type is prenuptial."""
    return input_data.document_type == "prenuptial"

def is_divorce(input_data: DocumentInput) -> bool:
    """Check if document type is divorce."""
    return input_data.document_type == "divorce"

