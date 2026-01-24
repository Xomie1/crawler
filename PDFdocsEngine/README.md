# Legal Document Generation Engine

A Python-based document generation engine for legal automation systems. Generates Prenuptial Agreements and Divorce Settlement Agreements as PDF and Word (.docx) files with clause branching logic and template processing.

## Features

- **Clause Branching Logic**: Conditional clause inclusion based on input values
- **Template Processing**: Support for Google Docs templates with `{{placeholder}}` syntax
- **Automatic Signature Blocks**: Generates signature blocks based on number of parties
- **Dual Output**: Generates both PDF and Word (.docx) files
- **Input Validation**: Pydantic-based validation for structured input
- **Modular & Extensible**: Easy to add new document types and clauses

## Installation

```bash
pip install -r requirements.txt
```

## Architecture

### Core Components

1. **Clause System** (`clauses.py`): Conditional clause inclusion with `condition()` and `content()` functions
2. **Template Processor** (`template_processor.py`): Handles `{{placeholder}}` replacement
3. **Signature Generator** (`signature_block.py`): Automatic signature block generation
4. **Document Engine** (`engine.py`): Main orchestration class

## Usage

### Method 1: Generate from Google Docs Template

```python
from engine import DocumentEngine

engine = DocumentEngine()

# Fetch template from Google Docs (you implement this)
template_content = """
PRENUPTIAL AGREEMENT

This Agreement is entered into on {{marriage_date}}, by and between:

{{party1_name}}
{{party1_address}}
(hereinafter referred to as "{{party1_role}}")

and

{{party2_name}}
{{party2_address}}
(hereinafter referred to as "{{party2_role}}")
...
"""

# Input data
input_data = {
    "document_type": "prenuptial",
    "parties": [
        {"name": "John Doe", "address": "123 Main St, Tokyo", "role": "husband"},
        {"name": "Jane Doe", "address": "456 Oak Ave, Tokyo", "role": "wife"}
    ],
    "options": {
        "property_separation": True,
        "alimony": False,
        "children": True
    },
    "custom_values": {
        "marriage_date": "2025-01-01",
        "jurisdiction": "Tokyo"
    }
}

# Generate documents
result = engine.generate_from_template(
    template_content=template_content,
    input_data=input_data,
    include_signatures=True,
    include_witnesses=False
)
# Returns: {"pdf_path": "...", "docx_path": "...", "document_type": "..."}
```

### Method 2: Generate from Clause Definitions

```python
from engine import DocumentEngine

engine = DocumentEngine()

# Input data
input_data = {
    "document_type": "prenuptial",
    "parties": [
        {"name": "John Doe", "address": "123 Main St, Tokyo", "role": "husband"},
        {"name": "Jane Doe", "address": "456 Oak Ave, Tokyo", "role": "wife"}
    ],
    "options": {
        "property_separation": True,
        "alimony": False,
        "children": True
    },
    "custom_values": {
        "marriage_date": "2025-01-01",
        "jurisdiction": "Tokyo"
    }
}

# Generate using clause branching logic
result = engine.generate_from_clauses(
    input_data=input_data,
    include_signatures=True,
    include_witnesses=False
)
```

## Template Placeholders

Available placeholders in templates:

### Party Information
- `{{party1_name}}`, `{{party1_address}}`, `{{party1_role}}`
- `{{party2_name}}`, `{{party2_address}}`, `{{party2_role}}`
- `{{husband_name}}`, `{{wife_name}}` (by role)
- `{{party_1_name}}`, `{{party_2_name}}` (indexed)

### Options
- `{{property_separation}}` (boolean)
- `{{alimony}}` (boolean)
- `{{children}}` (boolean)

### Custom Values
- `{{marriage_date}}` (auto-formatted)
- `{{divorce_date}}` (auto-formatted)
- `{{jurisdiction}}`
- Any custom key in `custom_values`

## Clause Branching

Clauses are conditionally included based on input:

```python
# Example: Property separation clause only included if option is True
if options["property_separation"]:
    include_clause("property_separation_clause")
```

Clauses define:
- `condition(input_data) -> bool`: Whether to include the clause
- `content(input_data) -> str`: The clause content

## Input Schema

```json
{
  "document_type": "prenuptial | divorce",
  "parties": [
    {
      "name": "string",
      "address": "string",
      "role": "string"
    }
  ],
  "options": {
    "property_separation": boolean,
    "alimony": boolean,
    "children": boolean
  },
  "custom_values": {
    "marriage_date": "YYYY-MM-DD",
    "jurisdiction": "string",
    ...
  }
}
```

## Extending the System

### Adding a New Clause

```python
from clauses import Clause, ClauseRegistry

# Register a new clause
registry.register(Clause(
    name="custom_clause",
    condition=lambda d: d.options.custom_option == True,
    content=lambda d: f"Custom clause content for {d.parties[0].name}"
))
```

### Adding a New Document Type

1. Create clause definitions in `clause_definitions.py`
2. Add clause order in `get_clause_order()`
3. Register clauses in engine initialization

## Output

Generated files are saved to the `output/` directory (configurable):
- Word files: `.docx` format (generated first)
- PDF files: `.pdf` format (converted from Word document)

**Important**: The PDF is generated by converting the Word document, ensuring both formats are identical. This approach guarantees:
- Identical formatting between Word and PDF
- Perfect preservation of fonts, styles, and layout
- Consistent appearance across both formats

Both formats preserve:
- Paragraph structure
- Headings and heading levels
- Font sizes, families, and styles (bold, italic, underline)
- Alignment (left, center, right, justify)
- Line spacing and paragraph spacing
- Line breaks
- Signature blocks
- Japanese text rendering

### PDF Conversion Requirements

The system uses `docx2pdf` to convert Word documents to PDF:
- **Windows**: Requires Microsoft Word to be installed
- **Linux/Mac**: Requires LibreOffice to be installed
  - Ubuntu/Debian: `sudo apt-get install libreoffice`
  - macOS: `brew install --cask libreoffice`

## Quality & Constraints

- **Deterministic**: Same input always produces same output
- **Reproducible**: No random elements
- **Stable**: Suitable for legal review
- **Modular**: Easy to test and extend
- **Testable**: All components are unit-testable
