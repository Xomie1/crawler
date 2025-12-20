# Web Crawler (Batch & Excel Driven)

A **production-ready, Excel-driven web crawler** for extracting company contact information at scale.  
Designed for **high-volume batch crawling**, with optional **AI-assisted extraction** for improved accuracy on difficult or non-standard websites.

**Status**: ✅ **Stable & Actively Used** – Supports large batch runs and AI-powered enrichment

---

## 📋 Features

- ✅ **Batch Crawling from Excel**
  - Process hundreds or thousands of company websites from a single Excel file
- ✅ **Email Extraction**
  - Detects, normalizes, and validates email addresses
- ✅ **Contact / Inquiry Form Detection**
  - Supports English & Japanese websites
  - Detects form URLs and embedded forms
- ✅ **Company Name Extraction**
  - Metadata, page structure, and AI-enhanced detection
- ✅ **Industry Detection**
  - Rule-based + AI-assisted extraction
- ✅ **AI-Assisted Extraction (Optional)**
  - Improves results for hard-to-parse or poorly structured sites
- ✅ **Robots.txt Compliance**
  - Respects crawling permissions by default
- ✅ **Retry & Error Handling**
  - Graceful degradation with detailed crawl status
- ✅ **Multiple Export Options**
  - Excel output
  - Google Sheets export
  - Google Apps Script integration
- ✅ **Scalable Batch Mode**
  - Supports large datasets with optional row limits for testing

---

## 📁 Project Structure

```text
crawler-main/
├── batch/ # Batch & CLI execution
│ ├── batch_crawler.py # Main CLI entry point
│ ├── excel_export.py # Excel output handling
│
├── config/
│ └── ai_config.py # AI configuration & settings
│
├── crawler/
│ ├── engine.py # Core crawling engine
│ ├── fetcher.py # HTTP fetching & retries
│ ├── parser.py # HTML parsing utilities
│ ├── robots.py # Robots.txt handling
│ ├── storage.py # Result storage & formatting
│ │
│ ├── extractors/ # Rule-based extractors
│ │ ├── email_extractor.py
│ │ ├── enhanced_company_name_extractor.py
│ │ ├── enhanced_contact_form_detector.py
│ │ ├── improved_ai_company_extractor.py
│ │ └── industry_extractor.py
│ │
│ └── ai/ # AI & hybrid extraction logic
│ ├── ai_extractor.py
│ └── hybrid_extractor.py
│
├── utils/
│ ├── logger.py # Logging utilities
│ ├── groq_normalizer.py # AI response normalization
│ └── prompt_templates.py # AI prompt templates
│
├── load_env.py # Environment variable loader
├── requirements.txt
├── setup.py
├── test data.xlsx # Sample input
└── README.md
```
---

## 🚀 Quick Start

### 1. Installation

```bash
pip install -r requirements.txt
Requires Python 3.10+ (tested on Python 3.11)
```

2. Basic Batch Crawl (No AI)
```bash
python batch/batch_crawler.py "excelfile.xlsx"
```
Crawls each website listed in the Excel file
Uses rule-based extractors only
Fast and cost-effective

3. Batch Crawl with AI Assistance
```bash
python batch/batch_crawler.py "excelfile.xlsx" --use-ai --ai-always
```
Enables AI extraction for:
Company name
Industry
Contact form detection (fallback)

Recommended for:
- Japanese sites
- Low-quality HTML
- JS-heavy pages

4. Small Batch / Test Run
```bash
python batch/batch_crawler.py "excelfile.xlsx" --use-ai --ai-always --limit n
```
Example:
```
python batch/batch_crawler.py "excelfile.xlsx" --use-ai --ai-always --limit 20
```
### 📊 Input Format (Excel)
Your Excel file should contain at least:
- Website URL column (root domain per company)
- All other columns are preserved and enriched with crawl results.

### 📤 Output Data
Each row is augmented with structured crawl results:
```text
json
{
  "url": "https://example.com",
  "email": "info@example.com",
  "inquiryFormUrl": "https://example.com/contact",
  "companyName": "Example Co., Ltd.",
  "industry": "Manufacturing",
  "httpStatus": 200,
  "robotsAllowed": true,
  "crawlStatus": "success",
  "errorMessage": null
}
```
### 🧠 AI vs Non-AI Mode
| Mode            | Description         | When to Use               |
| --------------- | ------------------- | ------------------------- |
| Rule-based only | Deterministic, fast | Clean HTML, Western sites |
| Hybrid AI       | Rules + AI fallback | Mixed-quality sites       |
| AI Always       | AI-first extraction | Japanese / complex sites  |


AI behavior is configured in:
```
config/ai_config.py
```
### ⚙️ Core Components
- Crawler Engine: crawler/engine.py
- Coordinates fetching, parsing, and extraction
- Controls rule-based and AI-assisted workflows

### Extractors
***crawler/extractors/***
- Modular, reusable rule-based detectors
- Easy to extend

## AI Layer
***crawler/ai/***
- Prompt-based extraction
- Hybrid logic merges deterministic + AI results

### Batch Runner
**batch/batch_crawler.py**
- CLI entry point
- Excel input/output
- Supports row limits and AI flags

### 📝 Logging
Centralized logging via:
```
from utils.logger import setup_logger
```
- INFO-level by default
- Crawl failures never halt the batch

### 🔒 Crawling Behavior & Safety
- Respects robots.txt by default
- One root URL crawl per company
- No deep crawling (intentional for scale)
- Safe retry and timeout handling

### ⚡ Performance Notes
- Optimized for large Excel batches
- Robots.txt cached per domain
- Suitable for 10,000+ rows depending on network and AI usage

### Form Detection
The crawler detects inquiry/contact forms using:

- Form tags with inquiry-related keywords
- Button labels (English and Japanese supported)
- Link text containing form-related keywords

Supported keywords include:
- English: "contact", "inquiry", "consultation", "form", etc.
- Japanese: "問い合わせ", "お問い合わせ", "相談", etc.

## Error Handling
The crawler implements comprehensive error handling:

- Network errors: Automatic retry with exponential backoff
- Timeout errors: Configurable timeout with retry
- Parsing errors: Graceful degradation with error logging
- Robots.txt errors: Defaults to allowing crawl if robots.txt is inaccessible

📄 License
Provided as-is for internal automation, research, and data enrichment workflows.
