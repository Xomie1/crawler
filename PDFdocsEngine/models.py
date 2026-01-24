"""
Data models for document generation engine.
"""
from typing import List, Dict, Optional, Literal
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class Party(BaseModel):
    """Represents a party in the legal document."""
    name: str = Field(..., min_length=1, description="Full name of the party")
    address: str = Field(..., min_length=1, description="Address of the party")
    role: str = Field(..., description="Role of the party (e.g., 'husband', 'wife', 'spouse')")
    
    @field_validator('role')
    @classmethod
    def validate_role(cls, v: str) -> str:
        """Normalize role to lowercase."""
        return v.lower()


class DocumentOptions(BaseModel):
    """Options for document generation."""
    property_separation: bool = Field(default=False, description="Include property separation clause")
    alimony: bool = Field(default=False, description="Include alimony/spousal support clause")
    children: bool = Field(default=False, description="Include children-related clauses")


class DocumentInput(BaseModel):
    """Input model for document generation."""
    document_type: str = Field(..., description="Type of document to generate (e.g., 'prenuptial', 'divorce', etc.)")
    parties: List[Party] = Field(..., min_length=2, description="List of parties involved")
    options: DocumentOptions = Field(default_factory=DocumentOptions, description="Document options")
    custom_values: Dict[str, str] = Field(default_factory=dict, description="Custom key-value pairs")
    
    @field_validator('parties')
    @classmethod
    def validate_parties(cls, v: List[Party]) -> List[Party]:
        """Ensure at least two parties are provided."""
        if len(v) < 2:
            raise ValueError("At least two parties are required")
        return v
    
    @classmethod
    def from_json(cls, json_data: dict) -> 'DocumentInput':
        """Create DocumentInput from JSON dictionary."""
        return cls(**json_data)

