"""
Financial Documents API - On-Demand Report Scraping and Retrieval
Provides endpoints to fetch specific annual/quarterly reports with intelligent scraping
"""

import os
import re
import time
import requests
import google.generativeai as genai
from datetime import datetime
from typing import Optional, Dict, List
from dotenv import load_dotenv
from supabase import create_client
from firecrawl import Firecrawl
from fastapi import FastAPI, HTTPException

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY")  # Gemini API key is stored as GOOGLE_API_KEY

# Validate required environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

if not FIRECRAWL_API_KEY:
    raise ValueError("FIRECRAWL_API_KEY must be set in environment variables")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY must be set in environment variables")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)

# Configure Gemini AI
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-2.5-flash')

app = FastAPI(
    title="Financial Documents API",
    description="On-demand scraping and retrieval of bank financial reports",
    version="1.0.0"
)

# Fiscal year conversion dictionary
FISCAL_YEAR_CONVERSION = {
    "2000/01": "2057/58", "2001/02": "2058/59", "2002/03": "2059/60",
    "2003/04": "2060/61", "2004/05": "2061/62", "2005/06": "2062/63",
    "2006/07": "2063/64", "2007/08": "2064/65", "2008/09": "2065/66",
    "2009/10": "2066/67", "2010/11": "2067/68", "2011/12": "2068/69",
    "2012/13": "2069/70", "2013/14": "2070/71", "2014/15": "2071/72",
    "2015/16": "2072/73", "2016/17": "2073/74", "2017/18": "2074/75",
    "2018/19": "2075/76", "2019/20": "2076/77", "2020/21": "2077/78",
    "2021/22": "2078/79", "2022/23": "2079/80", "2023/24": "2080/81",
    "2024/25": "2081/82", "2025/26": "2082/83"
}

# Reverse conversion (Nepali to English)
FISCAL_YEAR_REVERSE = {v: k for k, v in FISCAL_YEAR_CONVERSION.items()}

# SANIMA-specific fiscal year corrections
SANIMA_FISCAL_YEAR_CORRECTIONS = {
    "2065/66": ["2065/66", "2066/67", "2065/2066", "2066/2067"],
    "2065/2066": ["2065/66", "2066/67", "2065/2066", "2066/2067"],
}

# Dynamic API Configuration for banks with public APIs
DYNAMIC_API_BANKS = {
    "NABIL": {
        "name": "Nabil Bank Limited",
        "api_base": "https://siteapi.nabilbank.com/financialdocument",
        "file_base": "https://siteadmin.nabilbank.com/assets/backend",
        "category_id": 21,
        "quarterly_subcategory_id": "94",
        "annual_subcategory_id": "95",
        "method": "nabil_api"
    },
    "PCBL": {
        "name": "Prime Commercial Bank",
        "api_base": "https://primebank.com.np/pr1me4dm1n/api",
        "annual_endpoint": "/annual/all/{page}",
        "quarterly_endpoint": "/quarterly/all/{page}",
        "method": "prime_api",
        "paginated": True,
        "per_page": 5
    },
    "SANIMA": {
        "name": "Sanima Bank Limited",
        "api_base": "https://cms.sanimabank.com/framework/api/frontend/document/list",
        "file_base": "https://cms.sanimabank.com/framework/",
        "method": "sanima_api",
        "annual_category": "Annual Report",
        "quarterly_category": "Financial Report"
    },
    "GBIME": {
        "name": "Global IME Bank",
        "file_base": "https://gibl-assets.gibl.com.np/",
        "annual_api": "https://gibl-public-api.gibl.com.np/document/list?categoryId=bcf19805-0554-4490-b82f-6ad16e144efe&fiscalYear=",
        "quarterly_api": "https://gibl-public-api.gibl.com.np/document/list?categoryId=5bdd2656-1393-4d3b-b1d2-9de1ef5d8690&fiscalYear=",
        "method": "gbime_api"
    },
    "NIMB": {
        "name": "Nepal Investment Mega Bank",
        "api_base": "https://www.nimb.com.np/framework/api/frontend/document/list",
        "file_base": "https://www.nimb.com.np/framework/",
        "method": "nimb_api",
        "annual_keywords": ["Annual Reports - NIMB"],
        "quarterly_keywords": ["Financial Highlights - NIMB"]
    }
}


# ============================================================================
# DEVELOPMENT BANK DYNAMIC API CONFIGURATION
# ============================================================================

DEV_BANK_DYNAMIC_API = {
    "JBBL": {
        "name": "Jyoti Bikas Bank",
        "api_base": "https://web-cms.jbbl.com.np/framework/api/frontend/document/list",
        "file_base": "https://web-cms.jbbl.com.np/framework/",
        "method": "jbbl_api",
        "annual_category": "Annual Reports",
        "quarterly_category": "Quarterly Report"
    },
    "GRDBL": {
        "name": "Green Development Bank",
        "api_base": "https://greenbank.com.np/api/report",
        "file_base": "",  # Full URLs in response
        "method": "grdbl_api"
    },
    "SAPDBL": {
        "name": "Saptakoshi Development Bank",
        "annual_api": "https://admin.skdbl.com.np/api/reports/all/annual-report",
        "quarterly_api": "https://admin.skdbl.com.np/api/reports/all/quarterly-report",
        "file_base": "https://admin.skdbl.com.np/",
        "method": "sapdbl_api"
    }
}


# ============================================================================
# FINANCE COMPANY DYNAMIC API CONFIGURATION
# ============================================================================

FINANCE_COMPANY_DYNAMIC_API = {
    "PFL": {
        "name": "Pokhara Finance Limited",
        "annual_api": "https://adm1n.pokharafinance.com.np/api/Reports/all_reports/annualreport",
        "quarterly_api": "https://adm1n.pokharafinance.com.np/api/Reports/all_reports/quartely-report",
        "file_base": "", # Full URLs are in response
        "method": "pfl_api"
    },
    "GMFIL": {
        "name": "Guheswori Merchant Banking & Finance",
        "annual_api": "https://admin.gmbf.com.np/api/reports/all/annual-report",
        "quarterly_api": "https://admin.gmbf.com.np/api/reports/all/unaudited-financial-highlights",
        "file_base": "", # Full URLs are in response
        "method": "gmfil_api"
    },
    "ICFC": {
        "name": "ICFC Finance Limited",
        "annual_api": "https://admin.icfcbank.com/api/investor_relation/all/annual-report/",
        "quarterly_api": "https://admin.icfcbank.com/api/investor_relation/all/financial-report/",
        "file_base": "", # Full URLs are in response
        "method": "icfc_api"
    },
    "MFIL": {
        "name": "Manjushree Finance Limited",
        "api_base": "https://smc.manjushreefinance.com.np/framework/api/frontend/document/list",
        "file_base": "https://smc.manjushreefinance.com.np/framework/",
        "method": "mfil_api",
        "annual_category": "Annual Reports",
        "quarterly_category": "Quarterly Report"
    },
    "PROFL": {
        "name": "Progressive Finance Limited",
        "api_url": "https://api.pfltd.com.np/api/investor-documents",
        "api_token": "frontend_b638742e437e15b46899bfd970d4a0333c73e8cde9240f95203ecc09e6b9e994",
        "file_base": "http://api.pfltd.com.np/storage/",
        "method": "profl_api"
    }
}

# Finance companies with pagination (Firecrawl handled)
FINANCE_COMPANY_PAGINATED = {
    "GFCL": {
        "name": "Goodwill Finance",
        "annual_url": "https://goodwillfinance.com.np/downloads/2?page={page}",
        "quarterly_url": "https://goodwillfinance.com.np/downloads/3?page={page}",
        "method": "paginated",
        "max_pages": 3
    },
    "SIFC": {
        "name": "Shree Investment Finance",
        "annual_url": "https://www.shreefinance.com.np/annual-reports/page/{page}/",
        "quarterly_url": "https://www.shreefinance.com.np/category/quarterly-report/page/{page}/",
        "method": "paginated",
        "max_pages": 3,
        "ordinal_check": True # For "30th Annual Report" mapping
    },
    "SFCL": {
        "name": "Samriddhi Finance",
        "annual_url": "https://sfcl.com.np/en/files?page={page}", # Needs filtering by category in prompt
        "quarterly_url": "https://sfcl.com.np/en/files?page={page}",
        "method": "paginated",
        "max_pages": 3
    }
}

# Finance companies with static URLs (Direct Firecrawl)
FINANCE_COMPANY_STATIC = {
    "NFS": {
        "name": "Nepal Finance Limited",
        "annual_url": "https://www.nepalfinance.com.np/download/annual-reports",
        "quarterly_url": "https://www.nepalfinance.com.np/download/quarterly-unaudited-financial-highlights"
    },
    "BFC": {
        "name": "Best Finance Company",
        "annual_url": "https://bestfinance.com.np/annual-report/",
        "quarterly_url": "https://bestfinance.com.np/quarterly-report/"
    },
    "CFCL": {
        "name": "Central Finance Limited",
        "annual_url": "https://centralfinance.com.np/agm/",
        "quarterly_url": "https://centralfinance.com.np/financial-highlights/"
    },
    "JFL": {
        "name": "Janaki Finance Company",
        "annual_url": "https://jfcjanakpur.com.np/annualdetails",
        "quarterly_url": "https://jfcjanakpur.com.np/financialdetails"
    },
    "CMB": {
        "name": "Capital Merchant Banking",
        "report_page": "https://cmbfl.com.np/reports/" # Only annual available
    },
    "MPFL": {
        "name": "Multipurpose Finance",
        "annual_url": "https://www.multipurposefinance.com/financial-information/annual-report",
        "quarterly_url": "https://www.multipurposefinance.com/financial-information/financial-report"
    },
    "RLFL": {
        "name": "Reliance Finance",
        "annual_url": "https://reliancenepal.com.np/annual-report",
        "quarterly_url": "https://reliancenepal.com.np/quarterly-reports"
    },
    "GUFL": {
        "name": "Gurkhas Finance",
        "annual_url": "https://gurkhasfinance.com.np/type-of-report/annual-report",
        "quarterly_url": "https://gurkhasfinance.com.np/type-of-report/quarter-report"
    }
}
def normalize_fiscal_year_format(fiscal_year: str) -> str:
    """Normalize fiscal year to YYYY/YY format"""
    if not fiscal_year:
        return fiscal_year
    fiscal_year = fiscal_year.strip()
    if '/' not in fiscal_year:
        return fiscal_year
    parts = fiscal_year.split('/')
    if len(parts) != 2:
        return fiscal_year
    year1, year2 = parts[0].strip(), parts[1].strip()
    if len(year1) == 4 and len(year2) == 4:
        fiscal_year = f"{year1}/{year2[-2:]}"
    return fiscal_year


def extract_metadata_from_pdf_url(pdf_url: str, bank_symbol: str) -> Optional[Dict]:
    """
    Extract metadata (fiscal year, report type, quarter) from PDF using Google Gemini AI
    This ensures accurate metadata even if the URL or filename is misleading
    """
    import tempfile
    import json

    try:
        print(f"🤖 Using Gemini AI to extract metadata from PDF...")

        # Download PDF content
        response = requests.get(pdf_url, timeout=30, verify=False, stream=True)
        if response.status_code != 200:
            print(f"   ❌ Failed to download PDF: {response.status_code}")
            return None

        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            # Get PDF bytes (limit to first 5MB to avoid timeout)
            chunk_size = 1024 * 1024  # 1MB chunks
            max_size = 5 * 1024 * 1024  # 5MB limit
            total_size = 0

            for chunk in response.iter_content(chunk_size=chunk_size):
                tmp_file.write(chunk)
                total_size += len(chunk)
                if total_size >= max_size:
                    break

            tmp_path = tmp_file.name

        print(f"   📄 PDF downloaded: {total_size} bytes")

        # Prepare prompt for Gemini
        prompt = f"""
Analyze this financial report PDF and extract ONLY the following metadata:

1. **Fiscal Year**: In format YYYY/YY (e.g., 2078/79 for Nepali, or 2021/22 for English)
2. **Report Type**: Either "annual" or "quarterly"
3. **Quarter**: If quarterly, specify Q1, Q2, Q3, or Q4. If annual, leave as null.

IMPORTANT RULES:
- Look for fiscal year mentions like "FY 2078/79", "Fiscal Year 2078/79", "वित्तीय वर्ष २०७८/७९"
- For Nepali years (above 2030), use format like 2078/79
- For English years (below 2030), use format like 2021/22
- Annual reports: Look for "Annual Report", "Yearly Report", "वार्षिक प्रतिवेदन"
- Quarterly reports: Look for "Quarterly", "Q1", "Q2", "Q3", "Q4", "First Quarter", "त्रैमासिक"
- If you find "Ashad End" with a year, that's typically an annual report (not Q4)

Return ONLY a JSON object in this exact format:
{{
  "fiscal_year": "YYYY/YY",
  "report_type": "annual" or "quarterly",
  "quarter": "Q1" or "Q2" or "Q3" or "Q4" or null,
  "confidence": "high" or "medium" or "low"
}}

Bank: {bank_symbol}
"""

        try:
            # Upload PDF to Gemini
            uploaded_file = genai.upload_file(tmp_path, mime_type="application/pdf")

            # Generate response
            response = gemini_model.generate_content([prompt, uploaded_file])

            # Clean response and parse JSON
            response_text = response.text.strip()

            # Remove markdown code blocks if present
            if response_text.startswith('```'):
                response_text = response_text.split('```')[1]
                if response_text.startswith('json'):
                    response_text = response_text[4:]
                response_text = response_text.strip()

            metadata = json.loads(response_text)

            # Normalize fiscal year
            if metadata.get('fiscal_year'):
                metadata['fiscal_year'] = normalize_fiscal_year_format(metadata['fiscal_year'])

            print(f"   ✅ Metadata extracted: {metadata}")
            return metadata

        finally:
            # Clean up temporary file
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except Exception as e:
        print(f"   ⚠️ Failed to extract metadata with Gemini: {e}")
        return None


def normalize_fiscal_year(fiscal_year: str) -> tuple:
    """Normalize fiscal year to Nepali format and return both formats"""
    fiscal_year = fiscal_year.strip()
    try:
        year_start = int(fiscal_year.split('/')[0])
        if year_start < 2030:
            nepali_fy = FISCAL_YEAR_CONVERSION.get(fiscal_year, fiscal_year)
            return nepali_fy, fiscal_year
    except:
        pass
    english_fy = FISCAL_YEAR_REVERSE.get(fiscal_year, fiscal_year)
    return fiscal_year, english_fy


def get_bank_info(bank_symbol: str) -> Optional[Dict]:
    """Fetch bank information from database"""
    try:
        result = supabase.table("banks").select("*").eq("symbol", bank_symbol.upper()).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
    except Exception as e:
        print(f"Error fetching bank info: {e}")
        return None


def get_development_bank_info(bank_symbol: str) -> Optional[Dict]:
    """Fetch development bank information from database"""
    try:
        result = supabase.table("development_banks").select("*").eq("symbol", bank_symbol.upper()).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
    except Exception as e:
        print(f"Error fetching development bank info: {e}")
        return None


def check_document_exists(bank_id: int, fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[
    Dict]:
    """Check if document already exists in financial_documents table"""
    try:
        query = supabase.table("financial_documents").select("*").eq("bank_id", bank_id).eq("fiscal_year",
                                                                                            fiscal_year).eq(
            "report_type", report_type)
        if quarter:
            query = query.eq("quarter", quarter)
        else:
            query = query.is_("quarter", "null")
        result = query.execute()

        if result.data and len(result.data) > 0:
            return result.data[0]

        nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
        alt_fy = english_fy if fiscal_year == nepali_fy else nepali_fy

        if alt_fy != fiscal_year:
            query_alt = supabase.table("financial_documents").select("*").eq("bank_id", bank_id).eq("fiscal_year",
                                                                                                    alt_fy).eq(
                "report_type", report_type)
            if quarter:
                query_alt = query_alt.eq("quarter", quarter)
            else:
                query_alt = query_alt.is_("quarter", "null")
            result_alt = query_alt.execute()
            if result_alt.data and len(result_alt.data) > 0:
                return result_alt.data[0]
        return None
    except Exception as e:
        print(f"Error checking document: {e}")
        return None


def check_dev_bank_document_exists(bank_id: int, fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[
    Dict]:
    """Check if document already exists in development_banks_documents table"""
    try:
        query = supabase.table("development_banks_documents").select("*").eq("bank_id", bank_id).eq("fiscal_year",
                                                                                            fiscal_year).eq(
            "report_type", report_type)
        if quarter:
            query = query.eq("quarter", quarter)
        else:
            query = query.is_("quarter", "null")
        result = query.execute()

        if result.data and len(result.data) > 0:
            return result.data[0]

        nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
        alt_fy = english_fy if fiscal_year == nepali_fy else nepali_fy

        if alt_fy != fiscal_year:
            query_alt = supabase.table("development_banks_documents").select("*").eq("bank_id", bank_id).eq("fiscal_year",
                                                                                                    alt_fy).eq(
                "report_type", report_type)
            if quarter:
                query_alt = query_alt.eq("quarter", quarter)
            else:
                query_alt = query_alt.is_("quarter", "null")
            result_alt = query_alt.execute()
            if result_alt.data and len(result_alt.data) > 0:
                return result_alt.data[0]
        return None
    except Exception as e:
        print(f"Error checking development bank document: {e}")
        return None


# ============================================================================
# MICROFINANCE COMPANY DYNAMIC API CONFIGURATION
# ============================================================================

MICROFINANCE_DYNAMIC_API = {
    "PROFL": {
        "name": "Progressive Finance (Microfinance)",
        "api_url": "https://api.pfltd.com.np/api/investor-documents",
        "api_token": "frontend_b638742e437e15b46899bfd970d4a0333c73e8cde9240f95203ecc09e6b9e994",
        "file_base": "http://api.pfltd.com.np/storage/",
        "method": "profl_api"
    },
    "VLBS": {
        "name": "Vijaya Laghubitta",
        "token_url": "https://vlbsapi.graycode.com.np/connect/token",
        "api_url": "https://vlbsapi.graycode.com.np/api/v1/downloads/list/SettingKey",
        "client_id": "client",
        "client_secret": "secret",
        "company_id": "1",
        "annual_key": "reportannual",
        "quarterly_key": "reportqtr",
        "method": "vijaya_jwt_api",
        "token_expires": 3600
    },
    "NICLBSL": {
        "name": "NIC Asia Laghubitta",
        "api_base": "https://cms.nicasialaghubitta.com/framework/api/frontend/document/list",
        "file_base": "https://cms.nicasialaghubitta.com/framework/",
        "method": "nicbl_api",
        "annual_category": "Annual Report",
        "quarterly_category": "Quarterly Report"
    },
    "GILB": {
        "name": "Global IME Laghubitta",
        "annual_page": "https://gilb.com.np/annual-report/",
        "quarterly_page": "https://gilb.com.np/quaterly-report/",
        "method": "ninja_tables",
        "special_mapping": {
            "8th": "2076/77",
            "9th": "2077/78",
            "10th": "2078/79"
        }
    }
}

# Microfinance with CSRF Form
MICROFINANCE_CSRF_FORM = {
    "DDBL": {
        "name": "Deprosc Laghubitta",
        "annual_url": "https://www.deproscbank.com.np/en/main/ReportsPublic/2/",
        "quarterly_url": "https://www.deproscbank.com.np/en/main/ReportsPublic/3/",
        "csrf_field": "csrf_ddbank",
        "year_field": "year",
        "method": "csrf_form"
    }
}

# Microfinance with Pagination
MICROFINANCE_PAGINATED = {
    "SWBBL": {
        "name": "Swabalamban Laghubitta",
        "quarterly_url": "https://swabalambanlaghubitta.com/quarterly-report/?page={page}",
        "method": "paginated",
        "max_pages": 5
    },
    "SLBBL": {
        "name": "Swarojgar Laghubitta",
        "quarterly_url": "https://swarojgarbikas.com/quarterly-reports/?page={page}",
        "method": "paginated",
        "max_pages": 5
    },
    "LLBL": {
        "name": "Laxmi Laghubitta",
        "annual_url": "https://laxmilaghubitta.com/reports/annual/?page={page}",
        "quarterly_url": "https://laxmilaghubitta.com/reports/quarterly/?page={page}",
        "method": "paginated",
        "max_pages": 5
    },
    "RMDC": {
        "name": "RSDC Microfinance",
        "annual_url": "https://rsdcmf.com/annual-reports/?page={page}",
        "quarterly_url": "https://rsdcmf.com/quarterly-reports/?page={page}",
        "method": "paginated",
        "max_pages": 5
    },
    "NMBMF": {
        "name": "NMB Microfinance",
        "quarterly_url": "https://nmbmicrofinance.com/quarterly-report/?page={page}",
        "method": "paginated",
        "max_pages": 10,
        "reverse_order": True  # Latest in last page
    },
    "MSLB": {
        "name": "Mahuli Laghubitta",
        "annual_url": "https://mslbsl.com.np/reports/annual/?page={page}",
        "quarterly_url": "https://mslbsl.com.np/reports/quarterly/?page={page}",
        "method": "paginated",
        "max_pages": 5
    },
    "SMFDB": {
        "name": "Support Microfinance",
        "quarterly_url": "https://supportmicrofinance.com.np/quarterly-reports/?page={page}",
        "method": "paginated",
        "max_pages": 5
    },
    "FMDBL": {
        "name": "First Microfinance",
        "annual_url": "https://firstmicrofinance.com/reports/annual/?page={page}",
        "quarterly_url": "https://firstmicrofinance.com/reports/quarterly/?page={page}",
        "method": "paginated",
        "max_pages": 10,
        "reverse_order": True  # Latest in last page
    }
}

# Microfinance - Static Good (Clean implementations)
MICROFINANCE_STATIC_GOOD = {
    "NUBL": {"name": "Nirdhan Utthan", "annual_url": "https://nirdhan.com.np/reports/annual", "quarterly_url": "https://nirdhan.com.np/reports/quarterly"},
    "CBBL": {"name": "Chhimek Laghubitta", "annual_url": "https://chhimekbikas.com.np/reports/annual", "quarterly_url": "https://chhimekbikas.com.np/reports/quarterly"},
    "FOWAD": {"name": "Forward Microfinance", "annual_url": "https://forwardmicrofinance.com/annual-reports", "quarterly_url": "https://forwardmicrofinance.com/quarterly-reports"},
    "MERO": {"name": "Mero Microfinance", "annual_url": "https://meromicrofinance.com.np/reports/annual", "quarterly_url": "https://meromicrofinance.com.np/reports/quarterly"},
    "GBLBS": {"name": "Grameen Bikas", "annual_url": "https://gblbs.com.np/reports/annual", "quarterly_url": "https://gblbs.com.np/reports/quarterly"},
    "SMATA": {"name": "Samata Microfinance", "annual_url": "https://samatamicrofinance.com/annual-reports", "quarterly_url": "https://samatamicrofinance.com/quarterly-reports"},
    "SAMUBL": {"name": "Samudayik Laghubitta", "annual_url": "https://slbsl.com.np/reports/annual", "quarterly_url": "https://slbsl.com.np/reports/quarterly"},
    "NLBBL": {"name": "Nadep Laghubitta", "annual_url": "https://nadeplaghubitta.com.np/reports", "quarterly_url": "https://nadeplaghubitta.com.np/quarterly"},
    "GMFBS": {"name": "Ganapati Laghubitta", "report_page": "https://ganapatilaghubitta.com/reports"},  # Reports on Google Drive
    "ALBSL": {"name": "Asha Laghubitta", "annual_url": "https://aashalaghubitta.com/reports/annual", "quarterly_url": "https://aashalaghubitta.com/reports/quarterly"},
    "ILBS": {"name": "Infinity Laghubitta", "annual_url": "https://infinitylaghubitta.com.np/reports/annual", "quarterly_url": "https://infinitylaghubitta.com.np/reports/quarterly"},
    "SMFBS": {"name": "Swabhimaan Laghubitta", "annual_url": "https://swabhimaanlaghubitta.com/reports", "quarterly_url": "https://swabhimaanlaghubitta.com/quarterly"},
    "MLBSL": {"name": "Mahila Laghubitta", "annual_url": "https://mahilalaghubitta.com.np/reports/annual", "quarterly_url": "https://mahilalaghubitta.com.np/reports/quarterly"},
    "SWBSL": {"name": "Swastik Laghubitta", "annual_url": "https://swastiklaghubitta.com/annual-reports", "quarterly_url": "https://swastiklaghubitta.com/quarterly-reports"},
    "JBLB": {"name": "Jeevan Bikas Laghubitta", "annual_url": "https://jeevanbikaslaghubitta.com.np/reports", "quarterly_url": "https://jeevanbikaslaghubitta.com.np/quarterly"},
    "SHLB": {"name": "Shrijanshil Laghubitta", "annual_url": "https://shrijanshil.org/reports/annual", "quarterly_url": "https://shrijanshil.org/reports/quarterly"},
    "NESDO": {"name": "Nesdo Sambridhha", "annual_url": "https://nesdo.org.np/reports", "quarterly_url": "https://nesdo.org.np/quarterly"},
    "UNLB": {"name": "Unique Nepal Laghubitta", "annual_url": "https://uniquenepal.com/reports/annual", "quarterly_url": "https://uniquenepal.com/reports/quarterly"},
    "DLBS": {"name": "Dhaulagiri Laghubitta", "annual_url": "https://dhaulagirilaghubitta.com/reports", "quarterly_url": "https://dhaulagirilaghubitta.com/quarterly"},
    "ANLB": {"name": "Aatmanirbhar Laghubitta", "annual_url": "https://aatamanirbharlaghubitta.com.np/reports", "quarterly_url": "https://aatamanirbharlaghubitta.com.np/quarterly"},
    "CYCL": {"name": "CYC Nepal Laghubitta", "annual_url": "https://cycnepal.org.np/reports", "quarterly_url": "https://cycnepal.org.np/quarterly"},
    "AVYAN": {"name": "Aviyan Laghubitta", "annual_url": "https://www.aviyanlaghubitta.com/reports/annual", "quarterly_url": "https://www.aviyanlaghubitta.com/reports/quarterly"},
    "ACLBSL": {"name": "Aarambha Chautari Laghubitta", "annual_url": "https://aarambhachautari.com.np/reports", "quarterly_url": "https://aarambhachautari.com.np/quarterly"},
    "WNLB": {"name": "Wean Nepal Laghubitta", "annual_url": "https://weannepal.org/reports", "quarterly_url": "https://weannepal.org/quarterly"},
    "SWMF": {"name": "Suryodaya Womi Laghubitta", "annual_url": "https://suryodayawomi.com.np/reports", "quarterly_url": "https://suryodayawomi.com.np/quarterly"},
    "NMFBS": {"name": "National Laghubitta", "annual_url": "https://nationalmicrofinance.com.np/reports/annual", "quarterly_url": "https://nationalmicrofinance.com.np/reports/quarterly"},
    "MBLBL": {"name": "Matribhumi Laghubitta", "annual_url": "https://matribhumimf.com.np/reports/annual", "quarterly_url": "https://matribhumimf.com.np/reports/quarterly"},
    "NMLBBL": {"name": "Nerude Mirmire Laghubitta", "annual_url": "https://nerudemirmire.com.np/reports", "note": "Only 1 annual report"}
}

# Microfinance - Special Handling
MICROFINANCE_SPECIAL = {
    "KLBSL": {
        "name": "Kalika Laghubitta",
        "annual_url": "https://kalikalaghubitta.com.np/reports/annual",
        "quarterly_url": "https://kalikalaghubitta.com.np/reports/quarterly",
        "note": "No fiscal year in annual title - extract from context"
    },
    "JSLBB": {
        "name": "Janautthan Samudayic",
        "annual_url": "https://janauttanlaghubitta.com.np/reports",
        "quarterly_url": "https://janauttanlaghubitta.com.np/quarterly"
    },
    "MLBBL": {
        "name": "Mithila Laghubitta",
        "annual_url": "https://mithilalaghubitta.com/reports",
        "quarterly_url": "https://mithilalaghubitta.com/quarterly"
    },
    "HLBSL": {
        "name": "Himalayan Laghubitta",
        "base_url": "https://himalayanlaghubitta.com",
        "note": "Django full-stack"
    },
    "GLBSL": {
        "name": "Gurans Laghubitta",
        "annual_url": "https://guranslaghubitta.com/reports",
        "quarterly_url": "https://guranslaghubitta.com/quarterly",
        "note": "1st report = 2072/73, use numbering",
        "first_report_year": "2072/73"
    },
    "MLBS": {
        "name": "Manushi Laghubitta",
        "annual_url": "https://manushilaghubitta.com/reports",
        "quarterly_url": "https://manushilaghubitta.com/quarterly",
        "note": "Unclear titles - needs cleaning"
    },
    "ULBSL": {
        "name": "Upakar Laghubitta",
        "annual_url": "https://upakarlaghubitta.com/reports",
        "quarterly_url": "https://upakarlaghubitta.com/quarterly",
        "note": "Single year mentioned - parse from content"
    }
}

# Microfinance - Skip
MICROFINANCE_SKIP = {
    "SKBSL": {"name": "Sana Kisan", "reason": "Merged with RMDC"},
    "USLB": {"name": "Unnati Sahakarya", "reason": "Report page broken"},
    "SMPDA": {"name": "Sampada Laghubitta", "reason": "Only 1 annual report"}
}


# ============================================================================
# FINANCE COMPANY HELPER FUNCTIONS
# ============================================================================

def get_finance_company_info(company_symbol: str) -> Optional[Dict]:
    """Fetch finance company information from database"""
    try:
        result = supabase.table("finance_companies").select("*").eq("symbol", company_symbol.upper()).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
    except Exception as e:
        print(f"Error fetching finance company info: {e}")
        return None


def check_finance_company_document_exists(company_id: int, fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Check if document already exists in finance_companies_documents table"""
    try:
        query = supabase.table("finance_companies_documents").select("*").eq("finance_company_id", company_id).eq("fiscal_year",
                                                                                            fiscal_year).eq(
            "report_type", report_type)
        if quarter:
            query = query.eq("quarter", quarter)
        else:
            query = query.is_("quarter", "null")
        result = query.execute()

        if result.data and len(result.data) > 0:
            return result.data[0]

        # Try alternate fiscal year format
        nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
        alt_fy = english_fy if fiscal_year == nepali_fy else nepali_fy

        if alt_fy != fiscal_year:
            query_alt = supabase.table("finance_companies_documents").select("*").eq("finance_company_id", company_id).eq("fiscal_year",
                                                                                                    alt_fy).eq(
                "report_type", report_type)
            if quarter:
                query_alt = query_alt.eq("quarter", quarter)
            else:
                query_alt = query_alt.is_("quarter", "null")
            result_alt = query_alt.execute()
            if result_alt.data and len(result_alt.data) > 0:
                return result_alt.data[0]
        return None
    except Exception as e:
        print(f"Error checking finance company document: {e}")
        return None


def has_finance_company_dynamic_api(company_symbol: str) -> bool:
    """Check if finance company has dynamic API support"""
    return company_symbol.upper() in FINANCE_COMPANY_DYNAMIC_API


def has_finance_company_pagination(company_symbol: str) -> bool:
    """Check if finance company uses pagination"""
    return company_symbol.upper() in FINANCE_COMPANY_PAGINATED


def insert_finance_company_document_to_db(company_id: int, company_symbol: str, report: Dict) -> Dict:
    """
    Insert finance company document to database with duplicate checking
    If PDF URL already exists, update metadata if gemini extraction provides better data
    """
    try:
        pdf_url = report.get('pdf_url') or report.get('file_url')
        if not pdf_url:
            raise ValueError("No PDF URL found in report data")

        # Check if PDF URL already exists
        existing = supabase.table("finance_companies_documents").select("*").eq("pdf_url", pdf_url).execute()

        if existing.data and len(existing.data) > 0:
            print(f"   📄 PDF URL already exists in database")
            existing_doc = existing.data[0]

            # Check if we have better metadata from Gemini
            should_update = False
            update_data = {}

            # If existing record missing fiscal year but we have it
            if not existing_doc.get('fiscal_year') and report.get('fiscal_year'):
                should_update = True
                update_data['fiscal_year'] = report['fiscal_year']

            # If existing record missing quarter but we have it
            if not existing_doc.get('quarter') and report.get('quarter'):
                should_update = True
                update_data['quarter'] = report['quarter']

            # If existing record missing report_type but we have it
            if not existing_doc.get('report_type') and report.get('report_type'):
                should_update = True
                update_data['report_type'] = report['report_type']

            if should_update:
                print(f"   ✏️  Updating existing record with better metadata from Gemini")
                update_data['updated_at'] = datetime.now().isoformat()
                updated = supabase.table("finance_companies_documents")\
                    .update(update_data)\
                    .eq("id", existing_doc['id'])\
                    .execute()
                return updated.data[0] if updated.data else existing_doc
            else:
                print(f"   ℹ️  Existing record already has complete metadata")
                return existing_doc

        # Insert new document
        doc_data = {
            "finance_company_id": company_id,
            "finance_company_symbol": company_symbol,
            "pdf_url": pdf_url,
            "fiscal_year": report.get('fiscal_year', ''),
            "report_type": report.get('report_type', ''),
            "quarter": report.get('quarter'),
            "scraped_at": datetime.now().isoformat(),
            "method": report.get('source', 'static'),
            "added_by": report.get('added_by')
        }

        result = supabase.table("finance_companies_documents").insert(doc_data).execute()
        print(f"   ✅ Document inserted to database")
        return result.data[0] if result.data else doc_data

    except Exception as e:
        print(f"   ❌ Error inserting finance company document: {e}")
        raise


# ============================================================================
# MICROFINANCE COMPANY HELPER FUNCTIONS
# ============================================================================

def get_microfinance_company_info(company_symbol: str) -> Optional[Dict]:
    """Fetch microfinance company information from database"""
    try:
        result = supabase.table("microfinance_companies").select("*").eq("symbol", company_symbol.upper()).execute()
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
    except Exception as e:
        print(f"Error fetching microfinance company info: {e}")
        return None


def check_microfinance_company_document_exists(company_id: int, fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Check if document already exists in microfinance_companies_documents table"""
    try:
        query = supabase.table("microfinance_companies_documents").select("*").eq("microfinance_id", company_id).eq("fiscal_year",
                                                                                            fiscal_year).eq(
            "report_type", report_type)
        if quarter:
            query = query.eq("quarter", quarter)
        else:
            query = query.is_("quarter", "null")
        result = query.execute()

        if result.data and len(result.data) > 0:
            return result.data[0]

        # Try alternate fiscal year format
        nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
        alt_fy = english_fy if fiscal_year == nepali_fy else nepali_fy

        if alt_fy != fiscal_year:
            query_alt = supabase.table("microfinance_companies_documents").select("*").eq("microfinance_id", company_id).eq("fiscal_year",
                                                                                                    alt_fy).eq(
                "report_type", report_type)
            if quarter:
                query_alt = query_alt.eq("quarter", quarter)
            else:
                query_alt = query_alt.is_("quarter", "null")
            result_alt = query_alt.execute()
            if result_alt.data and len(result_alt.data) > 0:
                return result_alt.data[0]
        return None
    except Exception as e:
        print(f"Error checking microfinance company document: {e}")
        return None


def insert_microfinance_company_document_to_db(company_id: int, company_symbol: str, report: Dict) -> Dict:
    """
    Insert microfinance company document to database with duplicate checking
    If PDF URL already exists, update metadata if gemini extraction provides better data
    """
    try:
        pdf_url = report.get('pdf_url') or report.get('file_url')
        if not pdf_url:
            raise ValueError("No PDF URL found in report data")

        # Check if PDF URL already exists
        existing = supabase.table("microfinance_companies_documents").select("*").eq("pdf_url", pdf_url).execute()

        if existing.data and len(existing.data) > 0:
            print(f"   📄 PDF URL already exists in database")
            existing_doc = existing.data[0]

            # Check if we have better metadata from Gemini
            should_update = False
            update_data = {}

            # If existing record missing fiscal year but we have it
            if not existing_doc.get('fiscal_year') and report.get('fiscal_year'):
                should_update = True
                update_data['fiscal_year'] = report['fiscal_year']

            # If existing record missing quarter but we have it
            if not existing_doc.get('quarter') and report.get('quarter'):
                should_update = True
                update_data['quarter'] = report['quarter']

            # If existing record missing report_type but we have it
            if not existing_doc.get('report_type') and report.get('report_type'):
                should_update = True
                update_data['report_type'] = report['report_type']

            if should_update:
                print(f"   ✏️  Updating existing record with better metadata from Gemini")
                update_data['updated_at'] = datetime.now().isoformat()
                updated = supabase.table("microfinance_companies_documents")\
                    .update(update_data)\
                    .eq("id", existing_doc['id'])\
                    .execute()
                return updated.data[0] if updated.data else existing_doc
            else:
                print(f"   ℹ️  Existing record already has complete metadata")
                return existing_doc

        # Insert new document
        doc_data = {
            "microfinance_id": company_id,
            "microfinance_symbol": company_symbol,
            "pdf_url": pdf_url,
            "fiscal_year": report.get('fiscal_year', ''),
            "report_type": report.get('report_type', ''),
            "quarter": report.get('quarter'),
            "scraped_at": datetime.now().isoformat(),
            "method": report.get('source', 'static'),
            "added_by": report.get('added_by')
        }

        result = supabase.table("microfinance_companies_documents").insert(doc_data).execute()
        print(f"   ✅ Document inserted to database")
        return result.data[0] if result.data else doc_data

    except Exception as e:
        print(f"   ❌ Error inserting microfinance company document: {e}")
        raise


# ============================================================================
# MICROFINANCE DYNAMIC API HANDLERS
# ============================================================================

# JWT Token Manager for Vijaya Laghubitta
class VijayaTokenManager:
    """Manage JWT tokens for Vijaya Laghubitta API with auto-refresh"""
    def __init__(self):
        self.token = None
        self.expires_at = 0

    def get_token(self):
        """Get valid token, refresh if expired"""
        import time

        if self.token and time.time() < self.expires_at:
            return self.token

        # Fetch new token
        config = MICROFINANCE_DYNAMIC_API["VLBS"]
        TOKEN_URL = config["token_url"]

        response = requests.post(TOKEN_URL, data={
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "grant_type": "client_credentials"
        }, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=10)

        data = response.json()
        self.token = data["access_token"]
        self.expires_at = time.time() + 3500  # 100s buffer before 3600s expiry

        return self.token

# Global token manager instance
vijaya_token_manager = VijayaTokenManager()


def fetch_from_vijaya_jwt_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch reports from Vijaya Laghubitta JWT API"""
    try:
        config = MICROFINANCE_DYNAMIC_API["VLBS"]
        token = vijaya_token_manager.get_token()

        # Determine report key
        report_key = config["annual_key"] if report_type == "annual" else config["quarterly_key"]

        headers = {
            "Authorization": f"Bearer {token}",
            "companyid": config["company_id"],
            "Accept": "application/json"
        }

        params = {
            "pageNo": 1,
            "pageSize": 99,
            "key": report_key,
            "language": "en"
        }

        response = requests.get(config["api_url"], headers=headers, params=params, timeout=15)

        if response.status_code != 200:
            return None

        data = response.json()

        # Extract reports from response
        if not data or 'data' not in data:
            return None

        reports = data.get('data', [])

        # Normalize fiscal year
        nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

        # Find matching report
        for report in reports:
            file_year = report.get('fiscal_year', '')

            # Check fiscal year match
            if fiscal_year in file_year or nepali_fy in file_year or english_fy in file_year:
                # For quarterly, check quarter
                if report_type == "quarterly" and quarter:
                    file_title = report.get('file_title', '').lower()
                    quarter_match = False

                    if quarter == 'Q1' and any(k in file_title for k in ['first', '1st', 'q1']):
                        quarter_match = True
                    elif quarter == 'Q2' and any(k in file_title for k in ['second', '2nd', 'q2']):
                        quarter_match = True
                    elif quarter == 'Q3' and any(k in file_title for k in ['third', '3rd', 'q3']):
                        quarter_match = True
                    elif quarter == 'Q4' and any(k in file_title for k in ['fourth', '4th', 'q4']):
                        quarter_match = True

                    if not quarter_match:
                        continue

                # Return report with full PDF URL
                pdf_url = report.get('file_path_url', '')
                if not pdf_url:
                    file_path = report.get('file_path', '')
                    if file_path:
                        pdf_url = config.get('file_base', 'http://api.pfltd.com.np/storage/') + file_path

                return {
                    'pdf_url': pdf_url,
                    'fiscal_year': nepali_fy,
                    'report_type': report_type,
                    'quarter': quarter if report_type == 'quarterly' else None,
                    'source': 'vijaya_jwt_api'
                }

        return None

    except Exception as e:
        print(f"  ❌ Error fetching from Vijaya API: {e}")
        return None


def fetch_from_nicbl_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch reports from NicAsia Laghubitta API"""
    try:
        config = MICROFINANCE_DYNAMIC_API["NICLBSL"]
        api_url = config["api_base"]

        # Add fiscal year filter
        nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
        api_url_with_year = f"{api_url}?fiscalYear={nepali_fy.replace('/', '%2F')}"

        response = requests.get(api_url_with_year, timeout=15)

        if response.status_code != 200:
            # Try without fiscal year filter
            response = requests.get(api_url, timeout=15)
            if response.status_code != 200:
                return None

        data = response.json()

        # Determine target category
        target_category = config["annual_category"] if report_type == "annual" else config["quarterly_category"]

        # Parse response (similar to Sanima structure)
        for category in data:
            category_name = category.get('categoryTitle', '') or category.get('name', '')

            if target_category.lower() not in category_name.lower():
                continue

            # Look for sub-categories or documents
            documents = category.get('documents', [])
            sub_categories = category.get('subCategories', [])

            # Check documents in main category
            for doc in documents:
                doc_fiscal_year = doc.get('fiscal_year', '')
                doc_title = doc.get('name', '') or doc.get('title', '')

                # Check fiscal year match
                if not (fiscal_year in doc_fiscal_year or nepali_fy in doc_fiscal_year):
                    continue

                # For quarterly, check quarter
                if report_type == "quarterly" and quarter:
                    title_lower = doc_title.lower()
                    quarter_match = False

                    if quarter == 'Q1' and any(k in title_lower for k in ['first', '1st', 'q1']):
                        quarter_match = True
                    elif quarter == 'Q2' and any(k in title_lower for k in ['second', '2nd', 'q2']):
                        quarter_match = True
                    elif quarter == 'Q3' and any(k in title_lower for k in ['third', '3rd', 'q3']):
                        quarter_match = True
                    elif quarter == 'Q4' and any(k in title_lower for k in ['fourth', '4th', 'q4']):
                        quarter_match = True

                    if not quarter_match:
                        continue

                # Extract PDF URL
                file_path = doc.get('file', '')
                pdf_url = config["file_base"] + file_path if file_path else ''

                if pdf_url:
                    return {
                        'pdf_url': pdf_url,
                        'fiscal_year': nepali_fy,
                        'report_type': report_type,
                        'quarter': quarter if report_type == 'quarterly' else None,
                        'source': 'nicbl_api'
                    }

            # Check sub-categories
            for sub_cat in sub_categories:
                sub_docs = sub_cat.get('documents', [])
                for doc in sub_docs:
                    doc_fiscal_year = doc.get('fiscal_year', '')
                    doc_title = doc.get('name', '') or doc.get('title', '')

                    if fiscal_year in doc_fiscal_year or nepali_fy in doc_fiscal_year:
                        # Quarter check for quarterly reports
                        if report_type == "quarterly" and quarter:
                            title_lower = doc_title.lower()
                            quarter_match = False

                            if quarter == 'Q1' and any(k in title_lower for k in ['first', '1st', 'q1']):
                                quarter_match = True
                            elif quarter == 'Q2' and any(k in title_lower for k in ['second', '2nd', 'q2']):
                                quarter_match = True
                            elif quarter == 'Q3' and any(k in title_lower for k in ['third', '3rd', 'q3']):
                                quarter_match = True
                            elif quarter == 'Q4' and any(k in title_lower for k in ['fourth', '4th', 'q4']):
                                quarter_match = True

                            if not quarter_match:
                                continue

                        file_path = doc.get('file', '')
                        pdf_url = config["file_base"] + file_path if file_path else ''

                        if pdf_url:
                            return {
                                'pdf_url': pdf_url,
                                'fiscal_year': nepali_fy,
                                'report_type': report_type,
                                'quarter': quarter if report_type == 'quarterly' else None,
                                'source': 'nicbl_api'
                            }

        return None

    except Exception as e:
        print(f"  ❌ Error fetching from NicAsia Laghubitta API: {e}")
        return None


def fetch_from_gilb_ninja_tables(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch reports from Global IME Laghubitta using Ninja Tables API"""
    try:
        config = MICROFINANCE_DYNAMIC_API["GILB"]
        page_url = config["annual_page"] if report_type == "annual" else config["quarterly_page"]

        # Get API URLs from page
        response = requests.get(page_url, timeout=15)
        if response.status_code != 200:
            return None

        html = response.text

        # Extract API URLs
        pattern = r'"data_request_url":"(https:\\/\\/gilb\.com\.np\\/wp-admin\\/admin-ajax\.php[^"]+)"'
        matches = re.findall(pattern, html)

        if not matches:
            return None

        # Normalize fiscal year
        nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

        # Check special mapping for GILB
        special_mapping = config.get("special_mapping", {})
        for ordinal, mapped_year in special_mapping.items():
            if mapped_year == nepali_fy or mapped_year == fiscal_year:
                # Search for ordinal in title (e.g., "8th annual")
                for match in matches:
                    clean_url = match.replace('\\/', '/')
                    api_response = requests.get(clean_url, timeout=10)
                    if api_response.status_code == 200:
                        data = api_response.json()
                        for row in data:
                            title = row.get('report_details', '').lower()
                            if ordinal in title:
                                # Extract PDF link
                                downloads = row.get('downloads', '')
                                link_match = re.search(r'href=["\']([^"\']+)["\']', downloads)
                                if link_match:
                                    return {
                                        'pdf_url': link_match.group(1),
                                        'fiscal_year': nepali_fy,
                                        'report_type': report_type,
                                        'quarter': quarter if report_type == 'quarterly' else None,
                                        'source': 'gilb_ninja_tables'
                                    }

        # Standard search by fiscal year
        for raw_url in matches:
            clean_url = raw_url.replace('\\/', '/')

            api_response = requests.get(clean_url, timeout=10)
            if api_response.status_code != 200:
                continue

            data = api_response.json()

            for row in data:
                if 'report_details' not in row:
                    continue

                title = row.get('report_details', '')

                # Check fiscal year match
                if not (fiscal_year in title or nepali_fy in title or english_fy in title):
                    continue

                # For quarterly, check quarter
                if report_type == "quarterly" and quarter:
                    title_lower = title.lower()
                    quarter_match = False

                    if quarter == 'Q1' and any(k in title_lower for k in ['first', '1st', 'q1']):
                        quarter_match = True
                    elif quarter == 'Q2' and any(k in title_lower for k in ['second', '2nd', 'q2', 'mid']):
                        quarter_match = True
                    elif quarter == 'Q3' and any(k in title_lower for k in ['third', '3rd', 'q3']):
                        quarter_match = True
                    elif quarter == 'Q4' and any(k in title_lower for k in ['fourth', '4th', 'q4']):
                        quarter_match = True

                    if not quarter_match:
                        continue

                # Extract PDF link
                downloads = row.get('downloads', '')
                link_match = re.search(r'href=["\']([^"\']+)["\']', downloads)

                if link_match:
                    return {
                        'pdf_url': link_match.group(1),
                        'fiscal_year': nepali_fy,
                        'report_type': report_type,
                        'quarter': quarter if report_type == 'quarterly' else None,
                        'source': 'gilb_ninja_tables'
                    }

        return None

    except Exception as e:
        print(f"  ❌ Error fetching from GILB Ninja Tables: {e}")
        return None


def fetch_from_ddbl_csrf_form(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch reports from Deprosc Laghubitta using CSRF form"""
    try:
        from bs4 import BeautifulSoup

        config = MICROFINANCE_CSRF_FORM["DDBL"]
        url = config["annual_url"] if report_type == "annual" else config["quarterly_url"]

        session = requests.Session()

        # Get page with CSRF token
        response = session.get(url, timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract CSRF token
        csrf_token = soup.find('input', {'name': config["csrf_field"]})['value']

        # Get years from dropdown
        select_box = soup.find('select', {'id': config["year_field"]})
        options = select_box.find_all('option')

        # Try each year
        for opt in options:
            year_id = opt.get('value')
            year_label = opt.get_text(strip=True)

            if not year_id or year_id == 'selectyear':
                continue

            # Submit form
            payload = {
                config["csrf_field"]: csrf_token,
                'YearURL': '',
                config["year_field"]: year_id
            }

            post_response = session.post(url, data=payload, timeout=15)
            post_soup = BeautifulSoup(post_response.content, 'html.parser')

            # Extract reports
            reports_container = post_soup.find_all('div', class_='portfolio-item')

            for item in reports_container:
                link_tag = item.find('a', href=True)
                title_tag = item.find('h4')

                if not link_tag:
                    continue

                pdf_url = link_tag['href']
                if not pdf_url.startswith('http'):
                    pdf_url = 'https://www.deproscbank.com.np' + pdf_url

                title = title_tag.get_text(strip=True) if title_tag else ""

                # Extract fiscal year from title or year label
                fy_match = re.search(r'(\d{4})[/\-](\d{2,4})', title or year_label)
                if fy_match:
                    doc_fy = f"{fy_match.group(1)}/{fy_match.group(2)[-2:]}"
                else:
                    doc_fy = year_label

                # Normalize and check fiscal year
                nepali_fy, _ = normalize_fiscal_year(fiscal_year)

                if doc_fy == fiscal_year or doc_fy == nepali_fy:
                    # For quarterly, check quarter in title
                    if report_type == "quarterly" and quarter:
                        title_lower = title.lower()
                        quarter_match = False

                        if quarter == 'Q1' and any(k in title_lower for k in ['first', '1st', 'q1']):
                            quarter_match = True
                        elif quarter == 'Q2' and any(k in title_lower for k in ['second', '2nd', 'q2']):
                            quarter_match = True
                        elif quarter == 'Q3' and any(k in title_lower for k in ['third', '3rd', 'q3']):
                            quarter_match = True
                        elif quarter == 'Q4' and any(k in title_lower for k in ['fourth', '4th', 'q4']):
                            quarter_match = True

                        if not quarter_match:
                            continue

                    return {
                        'pdf_url': pdf_url,
                        'fiscal_year': nepali_fy,
                        'report_type': report_type,
                        'quarter': quarter if report_type == 'quarterly' else None,
                        'source': 'csrf_form'
                    }

        return None

    except Exception as e:
        print(f"  ❌ Error fetching from Deprosc CSRF form: {e}")
        return None


def has_microfinance_dynamic_api(company_symbol: str) -> bool:
    """Check if microfinance company has dynamic API support"""
    return company_symbol.upper() in MICROFINANCE_DYNAMIC_API


def has_microfinance_csrf_form(company_symbol: str) -> bool:
    """Check if microfinance company uses CSRF form"""
    return company_symbol.upper() in MICROFINANCE_CSRF_FORM


def has_microfinance_pagination(company_symbol: str) -> bool:
    """Check if microfinance company uses pagination"""
    return company_symbol.upper() in MICROFINANCE_PAGINATED


# ============================================================================
# COMMON HELPER FUNCTIONS
# ============================================================================

def get_scraping_urls(bank: Dict, report_type: str) -> List[tuple]:
    """Get appropriate URLs for scraping based on report type"""
    urls = []
    if report_type == "annual":
        if bank.get('annual_report_url'):
            urls.append((bank['annual_report_url'], 'annual_report_url'))
        if bank.get('report_page'):
            urls.append((bank['report_page'], 'report_page'))
    elif report_type == "quarterly":
        if bank.get('quarter_report_url'):
            urls.append((bank['quarter_report_url'], 'quarter_report_url'))
        if bank.get('report_page'):
            urls.append((bank['report_page'], 'report_page'))
    if not urls and bank.get('website'):
        urls.append((bank['website'], 'website'))
    return urls


def has_dynamic_api(bank_symbol: str) -> bool:
    """Check if bank has a dynamic API configured"""
    return bank_symbol.upper() in DYNAMIC_API_BANKS


# --- GBIME SPECIFIC HELPERS ---
def extract_gbime_quarter(quarter_obj, title):
    if quarter_obj:
        sys_name = quarter_obj.get('systemName', '').lower()
        if 'first' in sys_name: return 'Q1'
        if 'second' in sys_name: return 'Q2'
        if 'third' in sys_name: return 'Q3'
        if 'fourth' in sys_name: return 'Q4'
    t_lower = title.lower()
    if 'q1' in t_lower: return 'Q1'
    if 'q2' in t_lower: return 'Q2'
    if 'q3' in t_lower: return 'Q3'
    if 'q4' in t_lower: return 'Q4'
    if '1st' in t_lower or 'ashwin' in t_lower: return 'Q1'
    if '2nd' in t_lower or 'poush' in t_lower: return 'Q2'
    if '3rd' in t_lower or 'chaitra' in t_lower: return 'Q3'
    if '4th' in t_lower or 'ashad' in t_lower: return 'Q4'
    return None


def flatten_gbime_documents(api_response: Dict) -> List[Dict]:
    raw_docs = []
    categories_root = api_response.get('data', {}).get('documentCategory', [])
    for root_cat in categories_root:
        for sub in root_cat.get('subCategories', []) or []:
            for doc in sub.get('documents', []) or []:
                raw_docs.append(doc)
        for doc in root_cat.get('categories', []) or []:
            raw_docs.append(doc)
    return raw_docs


def fetch_from_gbime_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    config = DYNAMIC_API_BANKS["GBIME"]
    api_url = config['annual_api'] if report_type == 'annual' else config['quarterly_api']
    try:
        norm_fy = normalize_fiscal_year_format(fiscal_year)
        print(f"  Fetching from GBIME API: {api_url}")
        response = requests.get(api_url, timeout=20)
        if response.status_code != 200: return None
        all_docs = flatten_gbime_documents(response.json())
        candidates = []
        for doc in all_docs:
            if normalize_fiscal_year_format(doc.get('fiscal_year')) != norm_fy: continue
            if report_type == 'quarterly' and quarter:
                if extract_gbime_quarter(doc.get('quater'), doc.get('name', '')) != quarter: continue
            candidates.append(doc)
        if not candidates: return None
        sel = candidates[0]
        if len(candidates) > 1:
            eng = next((d for d in candidates if "english" in d.get('name', '').lower()), None)
            if eng: sel = eng
        full_url = f"{config['file_base']}{sel.get('file', '').lstrip('/')}"
        return {'fiscal_year': norm_fy, 'report_type': report_type, 'quarter': quarter, 'pdf_url': full_url,
                'document_name': sel.get('name', ''), 'source': 'gbime_api', 'raw_data': sel}
    except Exception as e:
        print(f"  GBIME Error: {e}")
        return None


def fetch_from_nabil_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    config = DYNAMIC_API_BANKS["NABIL"]
    try:
        api_url = f"{config['api_base']}/get_financial_document_subcategories_by_category/{config['category_id']}"
        print(f"  Fetching from Nabil API: {api_url}")
        response = requests.get(api_url, timeout=30)
        if response.status_code != 200: return None
        data = response.json()
        subcategories = data.get('data', [])
        target_subcategory_id = config['annual_subcategory_id'] if report_type == 'annual' else config[
            'quarterly_subcategory_id']
        documents = []
        for subcategory in subcategories:
            if subcategory.get('subcategory_id') == target_subcategory_id:
                documents = subcategory.get('documents', [])
                break
        if not documents: return None
        for doc in documents:
            doc_fiscal_year = doc.get('fiscal_year', '')
            doc_name = doc.get('name', '').lower()
            doc_fiscal_year_normalized = normalize_fiscal_year_format(doc_fiscal_year)
            fiscal_year_normalized = normalize_fiscal_year_format(fiscal_year)
            if doc_fiscal_year_normalized != fiscal_year_normalized: continue
            if report_type == 'quarterly' and quarter:
                quarter_map = {'Q1': ['first', 'q1', '1st'], 'Q2': ['second', 'q2', '2nd'],
                               'Q3': ['third', 'q3', '3rd'], 'Q4': ['fourth', 'q4', '4th']}
                quarter_keywords = quarter_map.get(quarter, [])
                if not any(kw in doc_name for kw in quarter_keywords): continue
            if doc.get('name_np') or 'nepali' in doc_name: continue
            file_path = doc.get('file', '')
            full_url = f"{config['file_base']}/{file_path}" if file_path else None
            return {'fiscal_year': fiscal_year_normalized, 'report_type': report_type, 'quarter': quarter,
                    'pdf_url': full_url, 'document_name': doc.get('name', ''), 'source': 'nabil_api', 'raw_data': doc}
        return None
    except Exception as e:
        print(f"  Error fetching from Nabil API: {e}")
        return None


def fetch_from_prime_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    config = DYNAMIC_API_BANKS["PCBL"]
    try:
        endpoint_template = config['annual_endpoint'] if report_type == 'annual' else config['quarterly_endpoint']
        fiscal_year_normalized = normalize_fiscal_year_format(fiscal_year)
        page = 1
        while page <= 20:
            api_url = f"{config['api_base']}{endpoint_template.format(page=page)}"
            response = requests.get(api_url, timeout=10)
            if response.status_code != 200: break
            api_response = response.json()
            if api_response.get('status') != 'Success': break
            items = api_response.get('items', [])
            if not items: break
            for record in items:
                title = record.get('Title', '')
                doc_path = record.get('DocPath', '')
                if not title or not doc_path: continue
                if 'kankai' in title.lower(): continue
                doc_fiscal_year = extract_fiscal_year_from_title(title)
                if not doc_fiscal_year: continue
                doc_fiscal_year_normalized = normalize_fiscal_year_format(doc_fiscal_year)
                if doc_fiscal_year_normalized != fiscal_year_normalized: continue
                if report_type == 'quarterly' and quarter:
                    doc_quarter = extract_quarter_from_title(title)
                    if doc_quarter != quarter: continue
                return {'fiscal_year': fiscal_year_normalized, 'report_type': report_type,
                        'quarter': quarter if report_type == 'quarterly' else None, 'pdf_url': doc_path,
                        'document_name': title, 'source': 'prime_api', 'raw_data': record}
            page += 1
        return None
    except Exception as e:
        print(f"  Error fetching from Prime API: {e}")
        return None


def fetch_from_sanima_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    config = DYNAMIC_API_BANKS["SANIMA"]
    try:
        fiscal_year_normalized = normalize_fiscal_year_format(fiscal_year)
        fiscal_years_to_check = [fiscal_year_normalized]
        if fiscal_year_normalized in SANIMA_FISCAL_YEAR_CORRECTIONS:
            fiscal_years_to_check.extend(SANIMA_FISCAL_YEAR_CORRECTIONS[fiscal_year_normalized])
        fiscal_years_to_check = list(set(fiscal_years_to_check))
        print(f"  Fetching from Sanima API: {config['api_base']}")
        response = requests.get(config['api_base'], timeout=15)
        if response.status_code != 200: return None
        api_response = response.json()
        if api_response.get('resCod') != '200': return None
        categories = api_response.get('data', {}).get('documentCategory', [])
        target_category = config['annual_category'] if report_type == 'annual' else config['quarterly_category']
        for category in categories:
            if category.get('name') != target_category: continue
            matching_docs = []
            for subcategory in category.get('subCategories', []):
                for doc in subcategory.get('documents', []):
                    doc_fiscal_year = doc.get('fiscal_year', '')
                    doc_fiscal_year_normalized = normalize_fiscal_year_format(doc_fiscal_year)
                    if doc_fiscal_year_normalized not in fiscal_years_to_check: continue
                    if report_type == 'quarterly' and quarter:
                        quater_obj = doc.get('quater')
                        if not quater_obj: continue
                        system_name = quater_obj.get('systemName', '')
                        quarter_map = {'first_quater': 'Q1', 'second_quater': 'Q2', 'third_quater': 'Q3',
                                       'fourth_quater': 'Q4'}
                        if quarter_map.get(system_name) != quarter: continue
                    matching_docs.append(doc)
            if matching_docs:
                selected_doc = None
                if len(matching_docs) == 1:
                    selected_doc = matching_docs[0]
                else:
                    english_docs = []
                    nepali_docs = []
                    for doc in matching_docs:
                        name = doc.get('name', '').lower()
                        if 'english' in name or '(eng)' in name:
                            english_docs.append(doc)
                        elif 'nepali' in name or '(nep)' in name:
                            nepali_docs.append(doc)
                        else:
                            if not doc.get('name_np'):
                                english_docs.append(doc)
                            else:
                                nepali_docs.append(doc)
                    if english_docs:
                        selected_doc = english_docs[0]
                    elif nepali_docs:
                        selected_doc = nepali_docs[0]
                if selected_doc:
                    file_path = selected_doc.get('file', '')
                    full_url = f"{config['file_base']}{file_path}" if file_path else None
                    return {'fiscal_year': fiscal_year_normalized, 'report_type': report_type,
                            'quarter': quarter if report_type == 'quarterly' else None, 'pdf_url': full_url,
                            'document_name': selected_doc.get('name', ''), 'source': 'sanima_api',
                            'raw_data': selected_doc}
        return None
    except Exception as e:
        print(f"  Error fetching from Sanima API: {e}")
        return None


def fetch_from_nimb_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    config = DYNAMIC_API_BANKS["NIMB"]
    try:
        fiscal_year_normalized = normalize_fiscal_year_format(fiscal_year)
        print(f"  Fetching from NIMB API: {config['api_base']}")
        response = requests.get(config['api_base'], timeout=15)
        if response.status_code != 200: return None
        api_response = response.json()
        if api_response.get('resCod') != '200': return None

        categories = api_response.get('data', {}).get('documentCategory', [])
        target_keywords = config['annual_keywords'] if report_type == 'annual' else config['quarterly_keywords']

        for category in categories:
            # Check if category matches NIMB specific keywords
            if not any(keyword in category.get('name', '') for keyword in target_keywords): continue

            matching_docs = []
            # Check nested subcategories
            for subcategory in category.get('subCategories', []) or []:
                for doc in subcategory.get('documents', []) or []:
                    doc_fy = normalize_fiscal_year_format(doc.get('fiscal_year', ''))
                    if doc_fy != fiscal_year_normalized: continue

                    if report_type == 'quarterly' and quarter:
                        quater_obj = doc.get('quater')
                        if not quater_obj: continue
                        sys_name = quater_obj.get('systemName', '').lower()
                        q_map = {'first_quater': 'Q1', 'second_quater': 'Q2', 'third_quater': 'Q3',
                                 'fourth_quater': 'Q4'}
                        if q_map.get(sys_name) != quarter: continue

                    matching_docs.append(doc)

            # Check direct documents in category (unlikely based on JSON but good practice)
            for doc in category.get('documents', []) or []:
                doc_fy = normalize_fiscal_year_format(doc.get('fiscal_year', ''))
                if doc_fy != fiscal_year_normalized: continue
                if report_type == 'quarterly' and quarter:
                    quater_obj = doc.get('quater')
                    if not quater_obj: continue
                    sys_name = quater_obj.get('systemName', '').lower()
                    q_map = {'first_quater': 'Q1', 'second_quater': 'Q2', 'third_quater': 'Q3', 'fourth_quater': 'Q4'}
                    if q_map.get(sys_name) != quarter: continue
                matching_docs.append(doc)

            if matching_docs:
                # Prefer latest upload or English if duplicates exist
                selected_doc = matching_docs[0]
                file_path = selected_doc.get('file', '')
                # Ensure no space in URL
                if file_path:
                    file_path = file_path.replace(' ', '%20')
                    full_url = f"{config['file_base']}{file_path}"
                    return {
                        'fiscal_year': fiscal_year_normalized,
                        'report_type': report_type,
                        'quarter': quarter if report_type == 'quarterly' else None,
                        'pdf_url': full_url,
                        'document_name': selected_doc.get('name', ''),
                        'source': 'nimb_api',
                        'raw_data': selected_doc
                    }
        return None
    except Exception as e:
        print(f"  Error fetching from NIMB API: {e}")
        return None


def insert_document_from_api(bank_id: int, bank_symbol: str, doc_info: Dict) -> Dict:
    try:
        document_data = {
            "bank_id": bank_id, "bank_symbol": bank_symbol, "pdf_url": doc_info['pdf_url'],
            "fiscal_year": doc_info['fiscal_year'], "report_type": doc_info['report_type'],
            "quarter": doc_info.get('quarter'), "scraped_at": datetime.now().isoformat(), "method": "dynamic"
        }
        existing = supabase.table("financial_documents").select("*").eq("pdf_url", doc_info['pdf_url']).execute()
        if existing.data and len(existing.data) > 0: return existing.data[0]
        result = supabase.table("financial_documents").insert(document_data).execute()
        if result.data and len(result.data) > 0: return result.data[0]
        raise Exception("Failed to insert document")
    except Exception as e:
        print(f"  Error inserting document: {e}")
        raise


def create_scraping_prompt(report_type: str, fiscal_year: str, quarter: Optional[str] = None) -> str:
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    # Calculate the "Closing Year" for Nepali dates (e.g., 2079/80 -> 2080)
    closing_year_str = ""
    prev_closing_year_str = ""
    try:
        if '/' in nepali_fy:
            parts = nepali_fy.split('/')
            year_end_part = parts[1].strip()
            if len(year_end_part) == 2:
                closing_year_str = "20" + year_end_part
            elif len(year_end_part) == 4:
                closing_year_str = year_end_part

            if closing_year_str and closing_year_str.isdigit():
                prev_closing_year_str = str(int(closing_year_str) - 1)
    except Exception:
        pass

    if report_type == "annual":
        # Enhanced Prompt for Annual Reports with STRICT Q4 Exclusion
        ashad_instruction = ""
        if closing_year_str:
            ashad_instruction = f"""
- AMBIGUITY HANDLING ("Ashad End" vs Annual):
  - "Ashad End {closing_year_str}" is the closing date for Fiscal Year {nepali_fy}.
  - CRITICAL: Banks publish TWO reports with this date:
    1. "Fourth Quarter / Q4 Report" (Unaudited) -> DO NOT PICK THIS.
    2. "Annual Report" (Audited) -> PICK THIS.
  - IF the link text says "Ashad End {closing_year_str}" BUT also says "Unaudited", "Interim", "Quarterly", or "Q4" -> IGNORE IT.
  - "Ashad End {prev_closing_year_str}" -> IGNORE (Previous Year)."""

        return f"""Extract ONLY the AUDITED ANNUAL REPORT for fiscal year {nepali_fy} or {english_fy}.
IMPORTANT CRITERIA:
- Must be the FINAL AUDITED ANNUAL report.
- STRICTLY EXCLUDE: Any document labeled "Unaudited", "Interim", "Quarterly", "Q4", "Fourth Quarter", "Financial Highlights", or "Statement of Financial Position" (unless explicitly marked Annual/Audited).
- Fiscal Year: {nepali_fy} (Nepali) or {english_fy} (English).
{ashad_instruction}
- Positive Keywords (Look for these): "Annual Report", "Yearly Report", "Audited", "AGM Report", "Annual Financial Statements".
- Negative Keywords (Reject if present): "Unaudited", "Quarterly", "Interim", "Q1", "Q2", "Q3", "Q4", "त्रैमासिक".
Return ONLY ONE report in this exact JSON format:
{{
  "found": true/false,
  "report": {{
    "fiscal_year": "{nepali_fy}",
    "report_type": "annual",
    "quarter": null,
    "file_url": "direct PDF link",
    "report_title": "exact title of the report"
  }}
}}
If the specific annual report for {nepali_fy} is NOT found, return: {{"found": false, "report": null}}"""

    else:
        # Quarterly Report Prompt
        quarter_names = {"Q1": "First Quarter", "Q2": "Second Quarter", "Q3": "Third Quarter", "Q4": "Fourth Quarter"}
        quarter_desc = quarter_names.get(quarter.upper(), quarter)

        q4_instruction = ""
        if quarter.upper() == "Q4" and closing_year_str:
            q4_instruction = f'- Note: "Ashad End {closing_year_str}" usually represents Q4 of {nepali_fy}. Match this ONLY if it is "Unaudited" or "Quarterly".'

        return f"""Extract ONLY the quarterly/interim report for {quarter_desc} of fiscal year {nepali_fy} or {english_fy}.
IMPORTANT CRITERIA:
- Must be a QUARTERLY/INTERIM/UNAUDITED report.
- Must be specifically for {quarter.upper()} (Quarter {quarter[1]}) of fiscal year {nepali_fy} or {english_fy}
{q4_instruction}
- Keywords for {quarter.upper()}: {quarter_desc}, "Unaudited", "Interim", "Financial Highlights".
- Keywords to AVOID: "Audited Annual Report", "Yearly Report", "AGM", "वार्षिक प्रतिवेदन".
Return ONLY ONE report in this exact JSON format:
{{
  "found": true/false,
  "report": {{
    "fiscal_year": "{nepali_fy}",
    "report_type": "quarterly",
    "quarter": "{quarter.upper()}",
    "file_url": "direct PDF link",
    "report_title": "exact title of the report"
  }}
}}
If the specific {quarter.upper()} report for {nepali_fy} is NOT found, return: {{"found": false, "report": null}}"""


def scrape_specific_report(bank: Dict, fiscal_year: str, report_type: str, quarter: Optional[str] = None,
                           max_retries: int = 3) -> Optional[Dict]:
    urls = get_scraping_urls(bank, report_type)
    if not urls: return None
    prompt = create_scraping_prompt(report_type, fiscal_year, quarter)
    for url, url_type in urls:
        print(f"🔍 Searching in {url_type}: {url}")
        for attempt in range(max_retries):
            try:
                if attempt > 0: time.sleep(5 * attempt)
                result = firecrawl.scrape(url, formats=["markdown", {"type": "json", "prompt": prompt}])
                if result.metadata and result.metadata.status_code and result.metadata.status_code >= 400:
                    if attempt < max_retries - 1:
                        time.sleep(20);
                        continue
                    else:
                        break
                if not result.json:
                    if attempt < max_retries - 1:
                        continue
                    else:
                        break
                found = result.json.get('found', False)
                report = result.json.get('report')
                if found and report and report.get('file_url'):
                    print(f"   ✅ Found report in {url_type}")
                    return report
                else:
                    break
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(10);
                    continue
                else:
                    break
        time.sleep(3)
    return None


def insert_document_to_db(bank_id: int, bank_symbol: str, report: Dict) -> Dict:
    """
    Insert document with strict PDF URL uniqueness
    - Checks if PDF URL already exists (prevents duplicates)
    - If duplicate: Uses Gemini AI to verify which metadata is correct
    - If new: Just inserts directly (no AI needed)
    """
    try:
        pdf_url = report['file_url']

        print(f"🔍 Checking if PDF URL already exists...")
        existing = supabase.table("financial_documents").select("*").eq("pdf_url", pdf_url).execute()

        if existing.data and len(existing.data) > 0:
            existing_doc = existing.data[0]

            print(f"⚠️  PDF URL already exists in database!")

            print(f"🤖 Using Gemini AI to verify correct metadata...")
            ai_metadata = extract_metadata_from_pdf_url(pdf_url, bank_symbol)

            if ai_metadata and ai_metadata.get('confidence') in ['high', 'medium']:

                ai_fiscal_year = ai_metadata.get('fiscal_year')
                ai_report_type = ai_metadata.get('report_type')
                ai_quarter = ai_metadata.get('quarter')

                # 🔥 FIX 1 — enforce quarter rule
                if ai_report_type == "annual":
                    ai_quarter = None
                else:
                    ai_quarter = ai_quarter or None

                metadata_matches = (
                        existing_doc.get('fiscal_year') == ai_fiscal_year and
                        existing_doc.get('report_type') == ai_report_type and
                        existing_doc.get('quarter') == ai_quarter
                )

                if metadata_matches:
                    print("   ✅ Existing metadata is correct.")
                    return existing_doc
                else:
                    print("   ⚠️ Updating incorrect metadata...")

                    update_data = {
                        'fiscal_year': ai_fiscal_year,
                        'report_type': ai_report_type,
                        'quarter': ai_quarter,  # ✅ fixed
                        'scraped_at': datetime.now().isoformat(),
                        'method': 'api'
                    }

                    updated = (
                        supabase.table("financial_documents")
                        .update(update_data)
                        .eq("id", existing_doc['id'])
                        .execute()
                    )

                    if updated.data:
                        return updated.data[0]

            return existing_doc

        # ✅ NEW INSERT
        print("✅ PDF URL is new. Inserting directly...")

        report_type = report['report_type']
        quarter = report.get('quarter')

        # 🔥 FIX 2 — enforce constraint BEFORE insert
        if report_type == "annual":
            quarter = None
        else:
            quarter = quarter or None

        doc_data = {
            'bank_id': bank_id,
            'bank_symbol': bank_symbol,
            'pdf_url': pdf_url,
            'fiscal_year': report['fiscal_year'],
            'report_type': report_type,
            'quarter': quarter,  # ✅ fixed
            'scraped_at': datetime.now().isoformat(),
            'method': 'api'
        }

        result = supabase.table("financial_documents").insert(doc_data).execute()

        if result.data:
            print("   ✅ Document inserted successfully!")
            return result.data[0]

        return None

    except Exception as e:
        if "unique constraint" in str(e).lower() or "duplicate" in str(e).lower():
            existing = supabase.table("financial_documents").select("*").eq("pdf_url", report['file_url']).execute()
            if existing.data:
                return existing.data[0]

        print(f"❌ Error inserting document: {e}")
        raise


def insert_dev_bank_document_to_db(bank_id: int, bank_symbol: str, report: Dict) -> Dict:
    """
    Insert development bank document with strict PDF URL uniqueness
    - Checks if PDF URL already exists (prevents duplicates)
    - If duplicate: Uses Gemini AI to verify which metadata is correct
    - If new: Just inserts directly (no AI needed)
    """
    try:
        # Development bank API handlers return 'pdf_url', not 'file_url'
        pdf_url = report.get('pdf_url') or report.get('file_url')

        # ✅ STEP 1: Check if PDF URL already exists in database
        print(f"🔍 Checking if PDF URL already exists in development banks...")
        existing = supabase.table("development_banks_documents").select("*").eq("pdf_url", pdf_url).execute()

        if existing.data and len(existing.data) > 0:
            # ⚠️ DUPLICATE FOUND - Use AI to determine correct metadata
            existing_doc = existing.data[0]
            print(f"⚠️  PDF URL already exists in database!")
            print(f"   Existing: fiscal_year={existing_doc.get('fiscal_year')}, report_type={existing_doc.get('report_type')}, quarter={existing_doc.get('quarter')}")
            print(f"   Requested: fiscal_year={report.get('fiscal_year')}, report_type={report.get('report_type')}, quarter={report.get('quarter')}")

            # ✅ STEP 2: Use Gemini AI to verify which metadata is correct
            print(f"🤖 Using Gemini AI to verify correct metadata...")
            ai_metadata = extract_metadata_from_pdf_url(pdf_url, bank_symbol)

            if ai_metadata and ai_metadata.get('confidence') in ['high', 'medium']:
                ai_fiscal_year = ai_metadata.get('fiscal_year')
                ai_report_type = ai_metadata.get('report_type')
                ai_quarter = ai_metadata.get('quarter')

                print(f"   AI Result: fiscal_year={ai_fiscal_year}, report_type={ai_report_type}, quarter={ai_quarter}")

                # Check if existing data matches AI extraction
                metadata_matches = (
                    existing_doc.get('fiscal_year') == ai_fiscal_year and
                    existing_doc.get('report_type') == ai_report_type and
                    existing_doc.get('quarter') == ai_quarter
                )

                if metadata_matches:
                    print(f"   ✅ Existing metadata is CORRECT. Returning existing record.")
                    return existing_doc
                else:
                    # ✅ STEP 3: Update existing record with correct AI-verified metadata
                    print(f"   ⚠️  Existing metadata is INCORRECT. Updating with AI-verified data...")
                    update_data = {
                        'fiscal_year': ai_fiscal_year,
                        'report_type': ai_report_type,
                        'quarter': ai_quarter,
                        'scraped_at': datetime.now().isoformat(),
                        'method': 'api'
                    }

                    updated = supabase.table("development_banks_documents")\
                        .update(update_data)\
                        .eq("id", existing_doc['id'])\
                        .execute()

                    if updated.data and len(updated.data) > 0:
                        print(f"   ✅ Metadata corrected successfully!")
                        return updated.data[0]
            else:
                print(f"   ⚠️  AI extraction failed or low confidence. Keeping existing record.")
                return existing_doc

        # ✅ NEW PDF URL - Just insert directly (no AI verification needed)
        print(f"✅ PDF URL is new. Inserting directly...")

        doc_data = {
            'bank_id': bank_id,
            'bank_symbol': bank_symbol,
            'pdf_url': pdf_url,
            'fiscal_year': report['fiscal_year'],
            'report_type': report['report_type'],
            'quarter': report.get('quarter'),
            'scraped_at': datetime.now().isoformat(),
            'method': 'api'
        }

        result = supabase.table("development_banks_documents").insert(doc_data).execute()
        if result.data and len(result.data) > 0:
            print(f"   ✅ Development bank document inserted successfully!")
            return result.data[0]
        return None

    except Exception as e:
        # Handle unique constraint violations gracefully
        if "unique constraint" in str(e).lower() or "duplicate" in str(e).lower():
            print(f"⚠️  Duplicate constraint violation detected")
            # Fetch and return existing document
            existing = supabase.table("development_banks_documents").select("*").eq("pdf_url", report['file_url']).execute()
            if existing.data:
                return existing.data[0]
        print(f"❌ Error inserting development bank document: {e}")
        raise


# ============================================================================
# DEVELOPMENT BANK DYNAMIC API HANDLERS
# ============================================================================

def fetch_from_jbbl_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch document from JBBL (Jyoti Bikas Bank) API"""
    config = DEV_BANK_DYNAMIC_API["JBBL"]
    try:
        print(f"  Fetching from JBBL API: {config['api_base']}")
        response = requests.get(config['api_base'], timeout=15)

        if response.status_code != 200:
            print(f"  ❌ JBBL API returned status {response.status_code}")
            return None

        data = response.json()

        if "data" not in data or "documentCategory" not in data["data"]:
            print(f"  ❌ Unexpected JBBL API structure")
            return None

        # Normalize target fiscal year
        target_fy = normalize_fiscal_year_format(fiscal_year)

        # Determine which category to search
        category_name = config['annual_category'] if report_type == 'annual' else config['quarterly_category']
        print(f"  Looking for category: {category_name}, Fiscal Year: {target_fy}")

        # Debug: Show all categories
        all_categories = [cat.get("name", "") for cat in data["data"]["documentCategory"]]
        print(f"  Available categories: {all_categories}")

        # Search through categories
        for category in data["data"]["documentCategory"]:
            if category_name.lower() not in category.get("name", "").lower():
                continue

            print(f"  ✓ Found category: {category.get('name')}")
            print(f"     Subcategories: {len(category.get('subCategories', []))}")

            docs_checked = 0
            docs_matching_fy = 0

            # Search through subcategories
            for sub_category in category.get("subCategories", []):
                # Search through documents
                for doc in sub_category.get("documents", []):
                    docs_checked += 1
                    doc_fy = normalize_fiscal_year_format(doc.get("fiscal_year", ""))

                    if doc_fy != target_fy:
                        continue

                    docs_matching_fy += 1

                    if doc_fy != target_fy:
                        continue

                    # For quarterly reports, check quarter
                    if report_type == "quarterly" and quarter:
                        doc_quarter = None
                        quater_obj = doc.get("quater")  # Note: API uses "quater" not "quarter"

                        # JBBL has nested quater object: {"systemName": "second_quater", "displayName": "Second Quater"}
                        if quater_obj:
                            if isinstance(quater_obj, dict):
                                # Extract from nested object
                                system_name = quater_obj.get("systemName", "")
                                display_name = quater_obj.get("displayName", "")

                                # Try to extract quarter from systemName or displayName
                                doc_quarter = extract_quarter_from_title(system_name) or extract_quarter_from_title(display_name)
                            elif isinstance(quater_obj, str):
                                # If it's a string, use it directly
                                doc_quarter = extract_quarter_from_title(quater_obj)

                        # If quater field didn't yield a quarter, try other sources
                        if not doc_quarter:
                            # Try to extract from document name first
                            doc_quarter = extract_quarter_from_title(doc.get("name", ""))

                            # If still not found, try subcategory name
                            if not doc_quarter:
                                doc_quarter = extract_quarter_from_title(sub_category.get("name", ""))

                        # Debug output for quarterly matching
                        quater_display = quater_obj.get("displayName", "") if isinstance(quater_obj, dict) else quater_obj
                        print(f"     Checking doc: {doc.get('name')[:50]}... | FY: {doc_fy} | Quarter obj: {quater_display} | Extracted: {doc_quarter}")

                        if doc_quarter != quarter:
                            continue

                    # Found matching document
                    file_path = doc.get("file", "")
                    if not file_path:
                        continue

                    pdf_url = f"{config['file_base'].rstrip('/')}/{file_path.lstrip('/')}"

                    print(f"  ✅ Found matching document: {doc.get('name')}")

                    return {
                        "fiscal_year": target_fy,
                        "report_type": report_type,
                        "quarter": quarter,
                        "pdf_url": pdf_url,
                        "document_name": doc.get("name", ""),
                        "source": "jbbl_api"
                    }

            # Show summary
            print(f"  📊 Summary: Checked {docs_checked} documents, {docs_matching_fy} matched fiscal year {target_fy}")

        print(f"  ❌ No matching document found for {target_fy} {quarter if quarter else ''}")
        return None

    except Exception as e:
        print(f"  ❌ JBBL API Error: {e}")
        return None


def fetch_from_grdbl_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch document from GRDBL (Green Development Bank) API"""
    config = DEV_BANK_DYNAMIC_API["GRDBL"]
    try:
        print(f"  Fetching from GRDBL API: {config['api_base']}")
        response = requests.get(config['api_base'], timeout=15)

        if response.status_code != 200:
            print(f"  ❌ GRDBL API returned status {response.status_code}")
            return None

        data = response.json()

        if not isinstance(data, list):
            print(f"  ❌ Unexpected GRDBL API structure")
            return None

        # Normalize target fiscal year (e.g., "2080/81")
        target_fy = normalize_fiscal_year_format(fiscal_year)
        print(f"  Target fiscal year: {target_fy}")

        # Search through reports
        for item in data:
            # --- FIX 1: Handle Fiscal Year Format ---
            fy_obj = item.get("fiscal_year", {})
            if isinstance(fy_obj, dict):
                raw_fy = fy_obj.get("title", "")
                # API returns "2080-2081", we need "2080/2081" to normalize correctly
                raw_fy = raw_fy.replace("-", "/")
                doc_fy = normalize_fiscal_year_format(raw_fy)
            else:
                doc_fy = ""

            if doc_fy != target_fy:
                continue

            # Check report type
            report_type_obj = item.get("report_type", {})
            report_type_name = ""
            if isinstance(report_type_obj, dict):
                report_type_name = report_type_obj.get("name", "").lower()

            # Match report type
            if report_type == "annual":
                # Matches "Annual report", "Tenth and Eleventh Combined Annual Report"
                if "annual" not in report_type_name:
                    continue
            elif report_type == "quarterly":
                # Matches "Quarterly Report", "NFRS Quarterly Report"
                if "quarterly" not in report_type_name and "interim" not in report_type_name:
                    continue

                # Check quarter
                if quarter:
                    doc_name = item.get("name", "")

                    # --- FIX 2: Handle specific GRDBL spellings locally if needed ---
                    # GRDBL uses "Aasadh" (ID 43), "Ashad", "Ashadh"
                    doc_quarter = extract_quarter_from_title(doc_name)

                    # Fallback for "Aasadh" if not in your main helper
                    if not doc_quarter and "aasadh" in doc_name.lower():
                        doc_quarter = "Q4"

                    if doc_quarter != quarter:
                        continue

            # Found matching document
            pdf_url = item.get("file", "")
            if not pdf_url:
                continue

            print(f"  ✅ Found matching document: {item.get('name')}")

            return {
                "fiscal_year": target_fy,
                "report_type": report_type,
                "quarter": quarter,
                "pdf_url": pdf_url,
                "document_name": item.get("name", ""),
                "source": "grdbl_api"
            }

        print(f"  ❌ No matching document found for {target_fy}")
        return None

    except Exception as e:
        print(f"  ❌ GRDBL API Error: {e}")
        return None


def fetch_from_sapdbl_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch document from SAPDBL (Saptakoshi Development Bank) API"""
    config = DEV_BANK_DYNAMIC_API["SAPDBL"]
    try:
        api_url = config["annual_api"] if report_type == "annual" else config["quarterly_api"]

        print(f"  Fetching from SAPDBL API: {api_url}")
        response = requests.get(api_url, timeout=15)

        if response.status_code != 200:
            print(f"  ❌ SAPDBL API returned status {response.status_code}")
            return None

        data = response.json()

        if "items" not in data or "en" not in data["items"]:
            print(f"  ❌ Unexpected SAPDBL API structure")
            return None

        # Normalize target fiscal year (e.g., "2081/82")
        target_fy = normalize_fiscal_year_format(fiscal_year)
        print(f"  Target fiscal year: {target_fy}")

        # Search through fiscal year groups
        for fy_group in data["items"]["en"]:
            raw_group_title = fy_group.get("title", "")

            # FIX 1: Handle Hyphens (e.g., "2081-82" -> "2081/82")
            clean_title = raw_group_title.replace("-", "/")

            # FIX 2: Handle Combined Years (e.g., "2077/78 2078/79")
            # Instead of exact match, check if target FY is IN the group title
            # We normalize both to ensure format consistency
            if target_fy not in normalize_fiscal_year_format(clean_title):
                continue

            print(f"  ✓ Found fiscal year group: {raw_group_title}")

            # Search through child documents
            for doc in fy_group.get("child", []):
                doc_name = doc.get("title", "")

                # For quarterly reports, check quarter
                if report_type == "quarterly" and quarter:
                    doc_quarter = extract_quarter_from_title(doc_name)

                    # Handle specific case if 'end' or 'ending' confuses the extractor
                    if not doc_quarter:
                        # Fallback for SAPDBL naming conventions if needed
                        if "ashoj" in doc_name.lower() or "asoj" in doc_name.lower():
                            doc_quarter = "Q1"
                        elif "poush" in doc_name.lower() or "pus" in doc_name.lower():
                            doc_quarter = "Q2"
                        elif "chaitra" in doc_name.lower():
                            doc_quarter = "Q3"
                        elif "ashadh" in doc_name.lower() or "ashad" in doc_name.lower():
                            doc_quarter = "Q4"

                    if doc_quarter != quarter:
                        continue

                # Found matching document
                pdf_url = doc.get("file", "")
                if not pdf_url:
                    continue

                print(f"  ✅ Found matching document: {doc_name}")

                return {
                    "fiscal_year": target_fy,
                    "report_type": report_type,
                    "quarter": quarter,
                    "pdf_url": pdf_url,
                    "document_name": doc_name,
                    "source": "sapdbl_api"
                }

        print(f"  ❌ No matching document found for {target_fy}")
        return None

    except Exception as e:
        print(f"  ❌ SAPDBL API Error: {e}")
        return None


def has_dev_bank_dynamic_api(bank_symbol: str) -> bool:
    """Check if development bank has dynamic API support"""
    return bank_symbol.upper() in DEV_BANK_DYNAMIC_API


def fetch_from_dev_bank_api(bank_symbol: str, fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Main dispatcher for development bank dynamic APIs"""
    bank_symbol = bank_symbol.upper()

    if not has_dev_bank_dynamic_api(bank_symbol):
        return None

    config = DEV_BANK_DYNAMIC_API[bank_symbol]
    print(f"  Using dynamic API for {bank_symbol} ({config['name']})")

    if bank_symbol == "JBBL":
        return fetch_from_jbbl_api(fiscal_year, report_type, quarter)
    elif bank_symbol == "GRDBL":
        return fetch_from_grdbl_api(fiscal_year, report_type, quarter)
    elif bank_symbol == "SAPDBL":
        return fetch_from_sapdbl_api(fiscal_year, report_type, quarter)

    return None


# ============================================================================
# FINANCE COMPANY API HANDLERS
# ============================================================================

def fetch_from_pfl_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch from Pokhara Finance (PFL) API"""
    config = FINANCE_COMPANY_DYNAMIC_API["PFL"]
    api_url = config['annual_api'] if report_type == 'annual' else config['quarterly_api']

    try:
        print(f"  Fetching from PFL API: {api_url}")
        response = requests.get(api_url, timeout=15)
        if response.status_code != 200: return None

        data = response.json()
        target_fy = normalize_fiscal_year_format(fiscal_year)

        # Structure: {"FY": {"en": [{"title": "FY 2079-80", "child": [...]}]}}
        fy_list = data.get("FY", {}).get("en", [])

        for group in fy_list:
            # Check if group title matches FY (e.g. "FY 2079-80")
            group_title = group.get("title", "").replace("-", "/")
            if target_fy not in normalize_fiscal_year_format(group_title):
                continue

            # Iterate children
            for doc in group.get("child", []):
                doc_title = doc.get("title", "")

                if report_type == 'quarterly' and quarter:
                    doc_quarter = extract_quarter_from_title(doc_title)
                    if doc_quarter != quarter: continue

                return {
                    "fiscal_year": target_fy,
                    "report_type": report_type,
                    "quarter": quarter,
                    "pdf_url": doc.get("DocPath"),
                    "document_name": doc_title,
                    "source": "pfl_api"
                }
        return None
    except Exception as e:
        print(f"  PFL API Error: {e}")
        return None


def fetch_from_gmfil_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch from Guheswori Finance (GMFIL) API"""
    config = FINANCE_COMPANY_DYNAMIC_API["GMFIL"]
    api_url = config['annual_api'] if report_type == 'annual' else config['quarterly_api']

    try:
        print(f"  Fetching from GMFIL API: {api_url}")
        response = requests.get(api_url, timeout=15)
        if response.status_code != 200: return None

        data = response.json()
        target_fy = normalize_fiscal_year_format(fiscal_year)

        items = data.get("items", {}).get("en", [])

        for doc in items:
            title = doc.get("title", "")
            # FIX: Replace hyphen with slash for extraction (e.g., 2080-81 -> 2080/81)
            clean_title = title.replace("-", "/")

            doc_fy = extract_fiscal_year_from_title(clean_title)
            if not doc_fy or normalize_fiscal_year_format(doc_fy) != target_fy:
                continue

            if report_type == 'quarterly' and quarter:
                doc_quarter = extract_quarter_from_title(title)
                if doc_quarter != quarter: continue

            return {
                "fiscal_year": target_fy,
                "report_type": report_type,
                "quarter": quarter,
                "pdf_url": doc.get("DocPath"),
                "document_name": title,
                "source": "gmfil_api"
            }
        return None
    except Exception as e:
        print(f"  GMFIL API Error: {e}")
        return None


def fetch_from_icfc_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch from ICFC Finance API"""
    config = FINANCE_COMPANY_DYNAMIC_API["ICFC"]
    api_url = config['annual_api'] if report_type == 'annual' else config['quarterly_api']

    try:
        print(f"  Fetching from ICFC API: {api_url}")
        response = requests.get(api_url, timeout=15)
        if response.status_code != 200: return None

        data = response.json()
        target_fy = normalize_fiscal_year_format(fiscal_year)

        items = data.get("items", {}).get("en", [])

        for doc in items:
            title = doc.get("title", "")
            # FIX: Replace hyphen with slash for extraction (e.g., 2080-81 -> 2080/81)
            clean_title = title.replace("-", "/")

            doc_fy = extract_fiscal_year_from_title(clean_title)

            if not doc_fy or normalize_fiscal_year_format(doc_fy) != target_fy:
                continue

            if report_type == 'quarterly' and quarter:
                doc_quarter = extract_quarter_from_title(title)
                if doc_quarter != quarter: continue

            return {
                "fiscal_year": target_fy,
                "report_type": report_type,
                "quarter": quarter,
                "pdf_url": doc.get("DocPath"),
                "document_name": title,
                "source": "icfc_api"
            }
        return None
    except Exception as e:
        print(f"  ICFC API Error: {e}")
        return None


def fetch_from_mfil_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch from Manjushree Finance (MFIL) API"""
    config = FINANCE_COMPANY_DYNAMIC_API["MFIL"]

    try:
        print(f"  Fetching from MFIL API: {config['api_base']}")
        response = requests.get(config['api_base'], timeout=15)
        if response.status_code != 200: return None

        data = response.json()
        target_fy = normalize_fiscal_year_format(fiscal_year)
        target_cat = config['annual_category'] if report_type == 'annual' else config['quarterly_category']

        # Manjushree structure: documentCategory -> subCategories -> documents
        categories = data.get("data", {}).get("documentCategory", [])

        for cat in categories:
            if target_cat.lower() not in cat.get("name", "").lower():
                continue

            # Iterate subcategories (often organized by FY)
            all_docs = []
            for sub in cat.get("subCategories", []):
                all_docs.extend(sub.get("documents", []))

            for doc in all_docs:
                # Check FY
                doc_fy = normalize_fiscal_year_format(doc.get("fiscal_year", ""))
                if doc_fy != target_fy: continue

                # Check Quarter
                if report_type == 'quarterly' and quarter:
                    doc_quarter = None
                    # Try extraction from quarter object
                    q_obj = doc.get("quater")
                    if q_obj and isinstance(q_obj, dict):
                        doc_quarter = extract_quarter_from_title(q_obj.get("systemName", ""))

                    if not doc_quarter:
                        doc_quarter = extract_quarter_from_title(doc.get("name", ""))

                    if doc_quarter != quarter: continue

                # Build URL
                file_path = doc.get("file", "")
                full_url = f"{config['file_base']}{file_path.lstrip('/')}"

                return {
                    "fiscal_year": target_fy,
                    "report_type": report_type,
                    "quarter": quarter,
                    "pdf_url": full_url,
                    "document_name": doc.get("name", ""),
                    "source": "mfil_api"
                }
        return None
    except Exception as e:
        print(f"  MFIL API Error: {e}")
        return None


def fetch_from_profl_api(fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Fetch from Progressive Finance (PROFL) API"""
    config = FINANCE_COMPANY_DYNAMIC_API["PROFL"]

    try:
        print(f"  Fetching from PROFL API: {config['api_url']}")

        headers = {
            "x-api-token": config['api_token'],
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        response = requests.get(config['api_url'], headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"  PROFL API returned status {response.status_code}")
            return None

        documents = response.json()
        if not isinstance(documents, list):
            print(f"  Unexpected PROFL API response format")
            return None

        target_fy = normalize_fiscal_year_format(fiscal_year)

        # Filter by report type
        if report_type == 'annual':
            target_file_type = "Annual Report"
        elif report_type == 'quarterly':
            target_file_type = "Quarterly Report"
        else:
            return None

        print(f"  Looking for: {target_file_type}, FY: {target_fy}, Quarter: {quarter}")

        candidates = []
        for doc in documents:
            file_type = doc.get("file_type", "")

            # Check report type
            if target_file_type not in file_type:
                continue

            # Extract fiscal year from various fields
            fiscal_year_field = doc.get("fiscal_year", "")

            if not fiscal_year_field:
                continue

            # Parse fiscal years from the field
            # Handle formats like:
            # - "F.Y. 079/80 & 080/81" (combined two years)
            # - "F.Y. 078/79"
            # - "F.Y 076/77"
            # - "Year 2082/83"
            # - "2078/09/08" (date format - skip)

            # Find all fiscal year patterns
            fy_matches = re.findall(r'(\d{3,4})/(\d{2,4})', fiscal_year_field)

            if not fy_matches:
                continue

            # Check if any of the found fiscal years match our target
            doc_matches = False
            for year1, year2 in fy_matches:
                # Normalize to 4 digits
                if len(year1) == 3:
                    year1 = '2' + year1  # Assume 20XX for 3-digit years (e.g., 079 -> 2079)
                if len(year2) == 2:
                    year2 = year1[:2] + year2
                elif len(year2) == 3:
                    year2 = '2' + year2  # Assume 20XX for 3-digit years

                # Create normalized fiscal year
                doc_fy = f"{year1}/{year2[-2:]}"

                if doc_fy == target_fy:
                    doc_matches = True
                    break

            if not doc_matches:
                continue

            # For quarterly reports, check quarter
            if report_type == 'quarterly' and quarter:
                title = doc.get("file_title", "").lower()
                doc_quarter = None

                if 'first' in title or '1st' in title:
                    doc_quarter = 'Q1'
                elif 'second' in title or '2nd' in title:
                    doc_quarter = 'Q2'
                elif 'third' in title or '3rd' in title:
                    doc_quarter = 'Q3'
                elif 'fourth' in title or '4th' in title:
                    doc_quarter = 'Q4'

                if doc_quarter != quarter:
                    continue

            candidates.append(doc)

        if not candidates:
            print(f"  No matching document found")
            return None

        # Select best candidate (first one for now, or prioritize most recent)
        selected = candidates[0]

        # Build full URL
        file_path = selected.get("file_path_url", "")
        if not file_path:
            print(f"  No file_path_url in document")
            return None

        print(f"  ✅ Found matching document: {selected.get('file_title', '')}")

        return {
            "fiscal_year": target_fy,
            "report_type": report_type,
            "quarter": quarter,
            "pdf_url": file_path,
            "document_name": selected.get("file_title", ""),
            "source": "profl_api"
        }

    except Exception as e:
        print(f"  PROFL API Error: {e}")
        return None



def fetch_from_finance_company_api(company_symbol: str, fiscal_year: str, report_type: str,
                                   quarter: Optional[str] = None) -> Optional[Dict]:
    """Dispatcher for Finance Company Dynamic APIs"""
    company_symbol = company_symbol.upper()

    if company_symbol == "PFL":
        return fetch_from_pfl_api(fiscal_year, report_type, quarter)
    elif company_symbol == "GMFIL":
        return fetch_from_gmfil_api(fiscal_year, report_type, quarter)
    elif company_symbol == "ICFC":
        return fetch_from_icfc_api(fiscal_year, report_type, quarter)
    elif company_symbol == "MFIL":
        return fetch_from_mfil_api(fiscal_year, report_type, quarter)
    elif company_symbol == "PROFL":
        return fetch_from_profl_api(fiscal_year, report_type, quarter)

    return None
# ============================================================================
# COMMERCIAL BANK DYNAMIC API DISPATCHER
# ============================================================================

def fetch_from_dynamic_api(bank_symbol: str, fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> \
        Optional[Dict]:
    bank_symbol = bank_symbol.upper()
    if not has_dynamic_api(bank_symbol): return None
    if bank_symbol == "NABIL":
        return fetch_from_nabil_api(fiscal_year, report_type, quarter)
    elif bank_symbol == "PCBL":
        return fetch_from_prime_api(fiscal_year, report_type, quarter)
    elif bank_symbol == "SANIMA":
        return fetch_from_sanima_api(fiscal_year, report_type, quarter)
    elif bank_symbol == "GBIME":
        return fetch_from_gbime_api(fiscal_year, report_type, quarter)
    elif bank_symbol == "NIMB":
        return fetch_from_nimb_api(fiscal_year, report_type, quarter)
    return None


def extract_fiscal_year_from_title(title: str) -> Optional[str]:
    title = title.lower()
    for part in title.split():
        if '/' in part and len(part) <= 7: return part
        if 'fy' in part and len(part) == 6: return part[3:] + '/' + part[5:]
    return None


def extract_quarter_from_title(title: str) -> Optional[str]:
    """
    Extract quarter from title - handles English, Nepali months, and various formats
    """
    if not title:
        return None

    title = title.lower()

    # Nepali month to quarter mapping
    nepali_months = {
        # Q1 months (Shrawan to Ashwin - July to October)
        'shrawan': 'Q1', 'sawan': 'Q1', 'ashwin': 'Q1', 'ashoj': 'Q1', 'asoj': 'Q1',
        # Q2 months (Kartik to Poush - November to January)
        'kartik': 'Q2', 'mangsir': 'Q2', 'poush': 'Q2', 'magh': 'Q2',
        # Q3 months (Falgun to Chaitra - February to April)
        'falgun': 'Q3', 'chaitra': 'Q3', 'chait': 'Q3',
        # Q4 months (Baisakh to Ashadh - May to July)
        'baisakh': 'Q4', 'jestha': 'Q4', 'ashadh': 'Q4', 'ashad': 'Q4', 'ashar': 'Q4'
    }

    # Check Nepali months first
    for month, qtr in nepali_months.items():
        if month in title:
            return qtr

    # English keywords
    keywords = {
        'q1': 'Q1', 'q2': 'Q2', 'q3': 'Q3', 'q4': 'Q4',
        '1st': 'Q1', '2nd': 'Q2', '3rd': 'Q3', '4th': 'Q4',
        'first': 'Q1', 'second': 'Q2', 'third': 'Q3', 'fourth': 'Q4',
        'quarter 1': 'Q1', 'quarter 2': 'Q2', 'quarter 3': 'Q3', 'quarter 4': 'Q4',
        'quarter-1': 'Q1', 'quarter-2': 'Q2', 'quarter-3': 'Q3', 'quarter-4': 'Q4',
        # JBBL API uses "quater" (typo) instead of "quarter"
        'first_quater': 'Q1', 'second_quater': 'Q2', 'third_quater': 'Q3', 'fourth_quater': 'Q4',
        'first quater': 'Q1', 'second quater': 'Q2', 'third quater': 'Q3', 'fourth quater': 'Q4'
    }

    for k, v in keywords.items():
        if k in title:
            return v

    return None


@app.get("/")
def root():
    return {"message": "Financial Documents API", "version": "1.0.0"}


@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/diagnose/{bank_symbol}")
def diagnose_bank_website(bank_symbol: str):
    bank_symbol = bank_symbol.upper()
    bank = get_bank_info(bank_symbol)
    if not bank: raise HTTPException(status_code=404, detail=f"Bank '{bank_symbol}' not found")
    results = {"bank_symbol": bank_symbol, "bank_name": bank['bank_name'], "timestamp": datetime.now().isoformat(),
               "urls_tested": {}}
    test_urls = []
    if bank.get('report_page'): test_urls.append(('report_page', bank['report_page']))
    if bank.get('annual_report_url'): test_urls.append(('annual_report_url', bank['annual_report_url']))
    if bank.get('quarter_report_url'): test_urls.append(('quarter_report_url', bank['quarter_report_url']))
    for url_type, url in test_urls:
        try:
            result = firecrawl.scrape(url, formats=["markdown"])
            status_code = result.metadata.status_code if result.metadata else None
            results["urls_tested"][url_type] = {"url": url, "status_code": status_code,
                                                "accessible": status_code == 200}
        except Exception as e:
            results["urls_tested"][url_type] = {"url": url, "error": str(e), "accessible": False}
    return results


@app.get("/annual-report")
def get_annual_report(bank_symbol: str, fiscal_year: str):
    bank_symbol = bank_symbol.upper()
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
    bank = get_bank_info(bank_symbol)
    if not bank: raise HTTPException(status_code=404, detail=f"Bank '{bank_symbol}' not found")

    existing = check_document_exists(bank['id'], nepali_fy, 'annual') or check_document_exists(bank['id'], english_fy,
                                                                                               'annual')
    if existing:
        return {"status": "found", "source": "database", "bank_symbol": bank_symbol,
                "fiscal_year": existing['fiscal_year'], "pdf_url": existing['pdf_url']}

    if has_dynamic_api(bank_symbol):
        api_doc = fetch_from_dynamic_api(bank_symbol, nepali_fy, 'annual')
        if api_doc:
            inserted = insert_document_from_api(bank['id'], bank_symbol, api_doc)
            return {"status": "found", "source": "dynamic_api", "bank_symbol": bank_symbol,
                    "fiscal_year": inserted['fiscal_year'], "pdf_url": inserted['pdf_url']}

    report = scrape_specific_report(bank, nepali_fy, 'annual')
    if not report:
        raise HTTPException(status_code=404, detail="Report not found via API or Scraper")
    insert_document_to_db(bank['id'], bank_symbol, report)
    return {"status": "found", "source": "scraped", "bank_symbol": bank_symbol, "fiscal_year": report['fiscal_year'],
            "pdf_url": report['file_url']}


@app.get("/quarterly-report")
def get_quarterly_report(bank_symbol: str, fiscal_year: str, quarter: str):
    bank_symbol = bank_symbol.upper()
    quarter = quarter.upper()
    if quarter not in ['Q1', 'Q2', 'Q3', 'Q4']: raise HTTPException(status_code=400, detail="Invalid Quarter")
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
    bank = get_bank_info(bank_symbol)
    if not bank: raise HTTPException(status_code=404, detail=f"Bank '{bank_symbol}' not found")

    existing = check_document_exists(bank['id'], nepali_fy, 'quarterly', quarter) or check_document_exists(bank['id'],
                                                                                                           english_fy,
                                                                                                           'quarterly',
                                                                                                           quarter)
    if existing:
        return {"status": "found", "source": "database", "bank_symbol": bank_symbol,
                "fiscal_year": existing['fiscal_year'], "pdf_url": existing['pdf_url']}

    if has_dynamic_api(bank_symbol):
        api_doc = fetch_from_dynamic_api(bank_symbol, nepali_fy, 'quarterly', quarter)
        if api_doc:
            inserted = insert_document_from_api(bank['id'], bank_symbol, api_doc)
            return {"status": "found", "source": "dynamic_api", "bank_symbol": bank_symbol,
                    "fiscal_year": inserted['fiscal_year'], "pdf_url": inserted['pdf_url']}

    report = scrape_specific_report(bank, nepali_fy, 'quarterly', quarter)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found via API or Scraper")
    insert_document_to_db(bank['id'], bank_symbol, report)
    return {"status": "found", "source": "scraped", "bank_symbol": bank_symbol, "fiscal_year": report['fiscal_year'],
            "pdf_url": report['file_url']}


@app.post("/sync-dynamic-bank/{bank_symbol}")
def sync_dynamic_bank_documents(bank_symbol: str):
    bank_symbol = bank_symbol.upper()
    if not has_dynamic_api(bank_symbol):
        raise HTTPException(status_code=400, detail=f"Bank '{bank_symbol}' does not have dynamic API support")
    bank = get_bank_info(bank_symbol)
    if not bank: raise HTTPException(status_code=404, detail=f"Bank '{bank_symbol}' not found")

    # --- NABIL SYNC (Original Logic) ---
    if bank_symbol == "NABIL":
        config = DYNAMIC_API_BANKS["NABIL"]
        try:
            api_url = f"{config['api_base']}/get_financial_document_subcategories_by_category/{config['category_id']}"
            print(f"Fetching from: {api_url}")
            response = requests.get(api_url, timeout=30)
            if response.status_code != 200: raise HTTPException(status_code=503,
                                                                detail=f"Nabil API returned status {response.status_code}")
            data = response.json()
            subcategories = data.get('data', [])
            existing_docs = supabase.table("financial_documents").select("pdf_url").eq("bank_id", bank['id']).execute()
            existing_urls = set(doc['pdf_url'] for doc in existing_docs.data) if existing_docs.data else set()
            results = {"bank_symbol": bank_symbol, "synced_at": datetime.now().isoformat(), "new_documents": 0,
                       "existing_documents": 0, "errors": []}
            for subcategory in subcategories:
                subcat_id = subcategory.get('subcategory_id')
                if subcat_id not in [config['quarterly_subcategory_id'], config['annual_subcategory_id']]: continue
                report_type = "quarterly" if subcat_id == config['quarterly_subcategory_id'] else "annual"
                documents = subcategory.get('documents', [])
                documents_by_key = {}
                for doc in documents:
                    if doc.get('name_np') or 'nepali' in doc.get('name', '').lower(): continue
                    fiscal_year_normalized = normalize_fiscal_year_format(doc.get('fiscal_year', ''))
                    quarter = None
                    if report_type == "quarterly":
                        name_lower = doc.get('name', '').lower()
                        if 'first' in name_lower or 'q1' in name_lower:
                            quarter = 'Q1'
                        elif 'second' in name_lower or 'q2' in name_lower:
                            quarter = 'Q2'
                        elif 'third' in name_lower or 'q3' in name_lower:
                            quarter = 'Q3'
                        elif 'fourth' in name_lower or 'q4' in name_lower:
                            quarter = 'Q4'
                    doc_key = (fiscal_year_normalized, quarter)
                    if doc_key not in documents_by_key: documents_by_key[doc_key] = []
                    documents_by_key[doc_key].append(doc)
                for doc_key, docs in documents_by_key.items():
                    fiscal_year_normalized, quarter = doc_key
                    selected_doc = docs[0]
                    file_path = selected_doc.get('file', '')
                    full_url = f"{config['file_base']}/{file_path}" if file_path else None
                    if not full_url: continue
                    if full_url in existing_urls:
                        results["existing_documents"] += 1
                        continue
                    try:
                        doc_data = {"bank_id": bank['id'], "bank_symbol": bank_symbol, "pdf_url": full_url,
                                    "fiscal_year": fiscal_year_normalized, "report_type": report_type,
                                    "quarter": quarter, "scraped_at": datetime.now().isoformat(), "method": "dynamic"}
                        supabase.table("financial_documents").insert(doc_data).execute()
                        results["new_documents"] += 1
                        existing_urls.add(full_url)
                    except Exception as e:
                        results["errors"].append(str(e))
            return results
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error syncing: {str(e)}")

    # --- PRIME BANK SYNC (Original Logic) ---
    elif bank_symbol == "PCBL":
        config = DYNAMIC_API_BANKS["PCBL"]
        try:
            existing_docs = supabase.table("financial_documents").select("pdf_url").eq("bank_id", bank['id']).execute()
            existing_urls = set(doc['pdf_url'] for doc in existing_docs.data) if existing_docs.data else set()
            results = {"bank_symbol": bank_symbol, "synced_at": datetime.now().isoformat(), "new_documents": 0,
                       "existing_documents": 0, "errors": []}
            for report_type, endpoint_template in [('annual', config['annual_endpoint']),
                                                   ('quarterly', config['quarterly_endpoint'])]:
                page = 1
                while page <= 20:
                    api_url = f"{config['api_base']}{endpoint_template.format(page=page)}"
                    response = requests.get(api_url, timeout=10)
                    if response.status_code != 200: break
                    api_response = response.json()
                    if api_response.get('status') != 'Success': break
                    items = api_response.get('items', [])
                    if not items: break
                    for record in items:
                        title = record.get('Title', '')
                        doc_path = record.get('DocPath', '')
                        if not title or not doc_path or 'kankai' in title.lower(): continue
                        if doc_path in existing_urls:
                            results["existing_documents"] += 1
                            continue
                        fiscal_year = extract_fiscal_year_from_title(title)
                        if not fiscal_year: continue
                        fiscal_year_normalized = normalize_fiscal_year_format(fiscal_year)
                        quarter = None
                        if report_type == 'quarterly': quarter = extract_quarter_from_title(title)
                        try:
                            doc_data = {"bank_id": bank['id'], "bank_symbol": bank_symbol, "pdf_url": doc_path,
                                        "fiscal_year": fiscal_year_normalized, "report_type": report_type,
                                        "quarter": quarter, "scraped_at": datetime.now().isoformat(),
                                        "method": "dynamic"}
                            supabase.table("financial_documents").insert(doc_data).execute()
                            results["new_documents"] += 1
                            existing_urls.add(doc_path)
                        except Exception as e:
                            results["errors"].append(str(e))
                    page += 1
            return results
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error syncing Prime: {str(e)}")

    # --- SANIMA BANK SYNC (Original Logic) ---
    elif bank_symbol == "SANIMA":
        config = DYNAMIC_API_BANKS["SANIMA"]
        try:
            response = requests.get(config['api_base'], timeout=15)
            api_response = response.json()
            categories = api_response.get('data', {}).get('documentCategory', [])
            existing_docs = supabase.table("financial_documents").select("pdf_url").eq("bank_id", bank['id']).execute()
            existing_urls = set(doc['pdf_url'] for doc in existing_docs.data) if existing_docs.data else set()
            results = {"bank_symbol": bank_symbol, "synced_at": datetime.now().isoformat(), "new_documents": 0,
                       "existing_documents": 0, "errors": []}
            for category in categories:
                category_name = category.get('name', '')
                if category_name not in ['Annual Report', 'Financial Report']: continue
                report_type = 'annual' if category_name == 'Annual Report' else 'quarterly'
                documents_by_key = {}
                for subcategory in category.get('subCategories', []):
                    for doc in subcategory.get('documents', []):
                        fiscal_year = doc.get('fiscal_year', '')
                        if not fiscal_year: continue
                        fiscal_year_normalized = normalize_fiscal_year_format(fiscal_year)
                        quarter = None
                        if report_type == 'quarterly':
                            quater_obj = doc.get('quater')
                            if quater_obj:
                                sys_name = quater_obj.get('systemName', '')
                                q_map = {'first_quater': 'Q1', 'second_quater': 'Q2', 'third_quater': 'Q3',
                                         'fourth_quater': 'Q4'}
                                quarter = q_map.get(sys_name)
                        doc_key = (fiscal_year_normalized, quarter)
                        if doc_key not in documents_by_key: documents_by_key[doc_key] = []
                        documents_by_key[doc_key].append(doc)
                for doc_key, docs in documents_by_key.items():
                    fiscal_year_normalized, quarter = doc_key
                    selected_doc = None
                    if len(docs) == 1:
                        selected_doc = docs[0]
                    else:
                        english_docs = [d for d in docs if
                                        'english' in d.get('name', '').lower() or '(eng)' in d.get('name',
                                                                                                   '').lower() or not d.get(
                                            'name_np')]
                        nepali_docs = [d for d in docs if d not in english_docs]
                        selected_doc = english_docs[0] if english_docs else nepali_docs[0]
                    if not selected_doc: continue
                    file_path = selected_doc.get('file', '')
                    full_url = f"{config['file_base']}{file_path}" if file_path else None
                    if not full_url or full_url in existing_urls:
                        if full_url: results["existing_documents"] += 1
                        continue
                    try:
                        doc_data = {"bank_id": bank['id'], "bank_symbol": bank_symbol, "pdf_url": full_url,
                                    "fiscal_year": fiscal_year_normalized, "report_type": report_type,
                                    "quarter": quarter, "scraped_at": datetime.now().isoformat(), "method": "dynamic"}
                        supabase.table("financial_documents").insert(doc_data).execute()
                        results["new_documents"] += 1
                        existing_urls.add(full_url)
                    except Exception as e:
                        results["errors"].append(str(e))
            return results
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error syncing Sanima: {str(e)}")

    # --- GBIME SYNC (New Logic) ---
    elif bank_symbol == "GBIME":
        config = DYNAMIC_API_BANKS["GBIME"]
        try:
            existing_docs = supabase.table("financial_documents").select("pdf_url").eq("bank_id", bank['id']).execute()
            existing_urls = set(doc['pdf_url'] for doc in existing_docs.data) if existing_docs.data else set()
            results = {"bank_symbol": bank_symbol, "synced_at": datetime.now().isoformat(), "new_documents": 0,
                       "existing_documents": 0, "errors": []}

            for report_type, api_url in [('annual', config['annual_api']), ('quarterly', config['quarterly_api'])]:
                print(f"Fetching GBIME {report_type}...")
                response = requests.get(api_url, timeout=20)
                if response.status_code != 200: continue

                all_docs = flatten_gbime_documents(response.json())

                # Group by FY+Quarter
                docs_map = {}
                for doc in all_docs:
                    fy = normalize_fiscal_year_format(doc.get('fiscal_year'))
                    if not fy: continue
                    q = None
                    if report_type == 'quarterly':
                        q = extract_gbime_quarter(doc.get('quater'), doc.get('name', ''))
                        if not q: continue
                    key = (fy, q)
                    if key not in docs_map: docs_map[key] = []
                    docs_map[key].append(doc)

                # Process groups
                for (fy, q), dlist in docs_map.items():
                    sel = dlist[0]
                    if len(dlist) > 1:
                        eng = next((d for d in dlist if "english" in d.get('name', '').lower()), None)
                        if eng: sel = eng

                    path = sel.get('file', '')
                    full_url = f"{config['file_base']}{path.lstrip('/')}"

                    if full_url in existing_urls:
                        results["existing_documents"] += 1
                        continue

                    try:
                        doc_data = {"bank_id": bank['id'], "bank_symbol": bank_symbol, "pdf_url": full_url,
                                    "fiscal_year": fy, "report_type": report_type, "quarter": q,
                                    "scraped_at": datetime.now().isoformat(), "method": "dynamic"}
                        supabase.table("financial_documents").insert(doc_data).execute()
                        results["new_documents"] += 1
                        existing_urls.add(full_url)
                    except Exception as e:
                        results["errors"].append(str(e))
            return results
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error syncing GBIME: {str(e)}")

    # --- NIMB SYNC (New Logic) ---
    elif bank_symbol == "NIMB":
        config = DYNAMIC_API_BANKS["NIMB"]
        try:
            existing_docs = supabase.table("financial_documents").select("pdf_url").eq("bank_id", bank['id']).execute()
            existing_urls = set(doc['pdf_url'] for doc in existing_docs.data) if existing_docs.data else set()
            results = {"bank_symbol": bank_symbol, "synced_at": datetime.now().isoformat(), "new_documents": 0,
                       "existing_documents": 0, "errors": []}

            print(f"Fetching from NIMB API: {config['api_base']}")
            response = requests.get(config['api_base'], timeout=20)
            if response.status_code != 200:
                raise HTTPException(status_code=503, detail=f"NIMB API returned status {response.status_code}")

            api_response = response.json()
            categories = api_response.get('data', {}).get('documentCategory', [])

            # Map report types to keyword lists
            report_types = [
                ('annual', config['annual_keywords']),
                ('quarterly', config['quarterly_keywords'])
            ]

            for report_type, keywords in report_types:
                for category in categories:
                    # Check if category matches NIMB specific keywords
                    if not any(kw in category.get('name', '') for kw in keywords): continue

                    # Flatten documents from subCategories and direct documents
                    all_cat_docs = []
                    for sub in category.get('subCategories', []) or []:
                        all_cat_docs.extend(sub.get('documents', []) or [])
                    all_cat_docs.extend(category.get('documents', []) or [])

                    for doc in all_cat_docs:
                        fiscal_year = doc.get('fiscal_year', '')
                        if not fiscal_year: continue

                        # Normalize fiscal year
                        fiscal_year_normalized = normalize_fiscal_year_format(fiscal_year)

                        # Determine Quarter
                        quarter = None
                        if report_type == 'quarterly':
                            quater_obj = doc.get('quater')
                            if quater_obj:
                                sys_name = quater_obj.get('systemName', '').lower()
                                q_map = {'first_quater': 'Q1', 'second_quater': 'Q2', 'third_quater': 'Q3',
                                         'fourth_quater': 'Q4'}
                                quarter = q_map.get(sys_name)
                            # Fallback text check if quarter object missing but unlikely based on JSON
                            if not quarter:
                                if 'q1' in doc.get('name', '').lower():
                                    quarter = 'Q1'
                                elif 'q2' in doc.get('name', '').lower():
                                    quarter = 'Q2'
                                elif 'q3' in doc.get('name', '').lower():
                                    quarter = 'Q3'
                                elif 'q4' in doc.get('name', '').lower():
                                    quarter = 'Q4'

                        # Build URL
                        file_path = doc.get('file', '')
                        if not file_path: continue
                        file_path = file_path.replace(' ', '%20')  # Fix spaces
                        full_url = f"{config['file_base']}{file_path}"

                        if full_url in existing_urls:
                            results["existing_documents"] += 1
                            continue

                        try:
                            doc_data = {
                                "bank_id": bank['id'],
                                "bank_symbol": bank_symbol,
                                "pdf_url": full_url,
                                "fiscal_year": fiscal_year_normalized,
                                "report_type": report_type,
                                "quarter": quarter,
                                "scraped_at": datetime.now().isoformat(),
                                "method": "dynamic"
                            }
                            supabase.table("financial_documents").insert(doc_data).execute()
                            results["new_documents"] += 1
                            existing_urls.add(full_url)
                        except Exception as e:
                            # Unique violation usually handled by check above, but for safety
                            if "unique constraint" not in str(e).lower():
                                results["errors"].append(str(e))

            return results
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error syncing NIMB: {str(e)}")

    raise HTTPException(status_code=501, detail=f"Sync not implemented for {bank_symbol}")


# ============================================================================
# DEVELOPMENT BANK ENDPOINTS
# ============================================================================

@app.get("/dev-bank/annual-report")
def get_dev_bank_annual_report(bank_symbol: str, fiscal_year: str):
    """
    Get annual report for a development bank
    Similar to commercial bank endpoint but uses development_banks and development_banks_documents tables
    """
    bank_symbol = bank_symbol.upper()
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    print(f"\n{'='*80}")
    print(f"📊 DEVELOPMENT BANK ANNUAL REPORT REQUEST: {bank_symbol} - {nepali_fy}")
    print(f"{'='*80}")
    print(f"📅 Fiscal Year: {nepali_fy} (Nepali) / {english_fy} (English)")

    # Get bank info from development_banks table
    bank = get_development_bank_info(bank_symbol)
    if not bank:
        raise HTTPException(status_code=404, detail=f"Development Bank '{bank_symbol}' not found")

    print(f"🏦 Bank: {bank.get('bank_name', bank_symbol)} ({bank_symbol})")

    # Check if document exists in database
    print(f"🔍 Checking database...")
    existing = check_dev_bank_document_exists(bank['id'], nepali_fy, 'annual') or \
               check_dev_bank_document_exists(bank['id'], english_fy, 'annual')

    if existing:
        print(f"✅ Found in database!")
        return {
            "status": "found",
            "source": "database",
            "bank_symbol": bank_symbol,
            "fiscal_year": existing['fiscal_year'],
            "pdf_url": existing['pdf_url']
        }

    print(f"❌ Not in database.")

    # Check if development bank has dynamic API support
    if has_dev_bank_dynamic_api(bank_symbol):
        print(f"🔌 Development Bank has dynamic API support - fetching from API...")
        api_doc = fetch_from_dev_bank_api(bank_symbol, nepali_fy, 'annual')
        if api_doc:
            print(f"✅ Found via dynamic API")
            # Insert to development banks table
            inserted = insert_dev_bank_document_to_db(bank['id'], bank_symbol, api_doc)
            return {
                "status": "found",
                "source": "dynamic_api",
                "bank_symbol": bank_symbol,
                "fiscal_year": inserted['fiscal_year'],
                "pdf_url": inserted['pdf_url']
            }
        print(f"❌ Not found via dynamic API")

    # Try scraping
    print(f"🔍 Starting Firecrawl scraping...")
    report = scrape_specific_report(bank, nepali_fy, 'annual')

    if not report:
        print(f"❌ Report not found after scraping")
        raise HTTPException(
            status_code=404,
            detail=f"Report not found for {bank_symbol} {nepali_fy} annual. Use /add-document endpoint to add the document first."
        )

    # Insert to database
    print(f"💾 Saving to database...")
    inserted_doc = insert_dev_bank_document_to_db(bank['id'], bank_symbol, report)

    return {
        "status": "found",
        "source": "scraped",
        "bank_symbol": bank_symbol,
        "fiscal_year": report['fiscal_year'],
        "pdf_url": report.get('pdf_url') or report.get('file_url')
    }


@app.get("/dev-bank/quarterly-report")
def get_dev_bank_quarterly_report(bank_symbol: str, fiscal_year: str, quarter: str):
    """
    Get quarterly report for a development bank
    Similar to commercial bank endpoint but uses development_banks and development_banks_documents tables
    """
    bank_symbol = bank_symbol.upper()
    quarter = quarter.upper()

    if quarter not in ['Q1', 'Q2', 'Q3', 'Q4']:
        raise HTTPException(status_code=400, detail="Invalid Quarter. Must be Q1, Q2, Q3, or Q4")

    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    print(f"\n{'='*80}")
    print(f"📊 DEVELOPMENT BANK QUARTERLY REPORT REQUEST: {bank_symbol} - {nepali_fy} {quarter}")
    print(f"{'='*80}")
    print(f"📅 Fiscal Year: {nepali_fy} (Nepali) / {english_fy} (English)")
    print(f"📅 Quarter: {quarter}")

    # Get bank info from development_banks table
    bank = get_development_bank_info(bank_symbol)
    if not bank:
        raise HTTPException(status_code=404, detail=f"Development Bank '{bank_symbol}' not found")

    print(f"🏦 Bank: {bank.get('bank_name', bank_symbol)} ({bank_symbol})")

    # Check if document exists in database
    print(f"🔍 Checking database...")
    existing = check_dev_bank_document_exists(bank['id'], nepali_fy, 'quarterly', quarter) or \
               check_dev_bank_document_exists(bank['id'], english_fy, 'quarterly', quarter)

    if existing:
        print(f"✅ Found in database!")
        return {
            "status": "found",
            "source": "database",
            "bank_symbol": bank_symbol,
            "fiscal_year": existing['fiscal_year'],
            "quarter": quarter,
            "pdf_url": existing['pdf_url']
        }

    print(f"❌ Not in database.")

    # Check if development bank has dynamic API support
    if has_dev_bank_dynamic_api(bank_symbol):
        print(f"🔌 Development Bank has dynamic API support - fetching from API...")
        api_doc = fetch_from_dev_bank_api(bank_symbol, nepali_fy, 'quarterly', quarter)
        if api_doc:
            print(f"✅ Found via dynamic API")
            # Insert to development banks table
            inserted = insert_dev_bank_document_to_db(bank['id'], bank_symbol, api_doc)
            return {
                "status": "found",
                "source": "dynamic_api",
                "bank_symbol": bank_symbol,
                "fiscal_year": inserted['fiscal_year'],
                "quarter": quarter,
                "pdf_url": inserted['pdf_url']
            }
        print(f"❌ Not found via dynamic API")

    # Try scraping
    print(f"🔍 Starting Firecrawl scraping...")
    report = scrape_specific_report(bank, nepali_fy, 'quarterly', quarter)

    if not report:
        print(f"❌ Report not found after scraping")
        raise HTTPException(
            status_code=404,
            detail=f"Report not found for {bank_symbol} {nepali_fy} {quarter}. Use /add-document endpoint to add the document first."
        )

    # Insert to database
    print(f"💾 Saving to database...")
    inserted_doc = insert_dev_bank_document_to_db(bank['id'], bank_symbol, report)

    return {
        "status": "found",
        "source": "scraped",
        "bank_symbol": bank_symbol,
        "fiscal_year": report['fiscal_year'],
        "quarter": quarter,
        "pdf_url": report.get('pdf_url') or report.get('file_url')
    }


# ============================================================================
# UPDATED FINANCE COMPANY ENDPOINT HANDLERS
# ============================================================================

@app.get("/finance-company/annual-report")
def get_finance_company_annual_report(company_symbol: str, fiscal_year: str):
    company_symbol = company_symbol.upper()
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    print(f"📊 FINANCE COMPANY ANNUAL: {company_symbol} {nepali_fy}")

    company = get_finance_company_info(company_symbol)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # 1. Database Check
    existing = check_finance_company_document_exists(company['id'], nepali_fy, 'annual') or \
               check_finance_company_document_exists(company['id'], english_fy, 'annual')

    if existing:
        return {"status": "found", "source": "database", "pdf_url": existing['pdf_url']}

    # 2. Dynamic API Check
    if has_finance_company_dynamic_api(company_symbol):
        print("  🔌 Using Dynamic API")
        api_doc = fetch_from_finance_company_api(company_symbol, nepali_fy, 'annual')
        if api_doc:
            inserted = insert_finance_company_document_to_db(company['id'], company_symbol, api_doc)
            return {"status": "found", "source": "dynamic_api", "pdf_url": inserted['pdf_url']}

    # 3. Firecrawl Scraping (Paginated & Static)
    print("  🔍 Starting Scraping...")

    # Configure URLs based on type
    urls = []

    # Check Pagination Config
    if has_finance_company_pagination(company_symbol):
        config = FINANCE_COMPANY_PAGINATED[company_symbol]
        # Generate URLs for first 2 pages to be safe
        base_url = config.get('annual_url', '')
        if "{page}" in base_url:
            urls = [base_url.format(page=i) for i in range(1, 3)]
    else:
        # Static URLs
        static_config = FINANCE_COMPANY_STATIC.get(company_symbol, {})
        if static_config.get('annual_url'): urls.append(static_config['annual_url'])
        if static_config.get('report_page'): urls.append(static_config['report_page'])
        if company.get('annual_report_url'): urls.append(company['annual_report_url'])

    # Special Prompt for SIFC (Shree) ordinal matching
    ordinal_instruction = ""
    if company_symbol == "SIFC":
        ordinal_instruction = f"""
        - IMPORTANT: This site uses ordinal titles like "30th Annual Report".
        - 30th = 2080/81
        - 29th = 2079/80
        - 28th = 2078/79
        - Calculate the target ordinal for {nepali_fy} and match that title.
        """

    prompt = f"""Find the AUDITED ANNUAL REPORT for fiscal year {nepali_fy} or {english_fy}.
    {ordinal_instruction}
    Return JSON: {{ "found": true, "report": {{ "fiscal_year": "{nepali_fy}", "report_type": "annual", "file_url": "url" }} }}"""

    for url in urls:
        print(f"  Scanning: {url}")
        try:
            result = firecrawl.scrape(url, formats=["markdown", {"type": "json", "prompt": prompt}])
            if result.json and result.json.get('found'):
                report = result.json.get('report')
                if report and report.get('file_url'):
                    report['source'] = 'firecrawl'
                    inserted = insert_finance_company_document_to_db(company['id'], company_symbol, report)
                    return {"status": "found", "source": "scraped", "pdf_url": inserted['pdf_url']}
        except Exception as e:
            print(f"  Error scraping {url}: {e}")

    raise HTTPException(status_code=404, detail="Report not found")


@app.get("/finance-company/quarterly-report")
def get_finance_company_quarterly_report(company_symbol: str, fiscal_year: str, quarter: str):
    company_symbol = company_symbol.upper()
    quarter = quarter.upper()
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    print(f"📊 FINANCE COMPANY QUARTERLY: {company_symbol} {nepali_fy} {quarter}")

    company = get_finance_company_info(company_symbol)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # 1. Database Check
    existing = check_finance_company_document_exists(company['id'], nepali_fy, 'quarterly', quarter) or \
               check_finance_company_document_exists(company['id'], english_fy, 'quarterly', quarter)

    if existing:
        return {"status": "found", "source": "database", "pdf_url": existing['pdf_url']}

    # 2. Dynamic API Check
    if has_finance_company_dynamic_api(company_symbol):
        print("  🔌 Using Dynamic API")
        api_doc = fetch_from_finance_company_api(company_symbol, nepali_fy, 'quarterly', quarter)
        if api_doc:
            inserted = insert_finance_company_document_to_db(company['id'], company_symbol, api_doc)
            return {"status": "found", "source": "dynamic_api", "pdf_url": inserted['pdf_url']}

    # 3. Firecrawl Scraping
    print("  🔍 Starting Scraping...")

    urls = []
    if has_finance_company_pagination(company_symbol):
        config = FINANCE_COMPANY_PAGINATED[company_symbol]
        base_url = config.get('quarterly_url', '')
        if "{page}" in base_url:
            urls = [base_url.format(page=i) for i in range(1, 3)]
    else:
        static_config = FINANCE_COMPANY_STATIC.get(company_symbol, {})
        if static_config.get('quarterly_url'): urls.append(static_config['quarterly_url'])
        if static_config.get('report_page'): urls.append(static_config['report_page'])
        if company.get('quarter_report_url'): urls.append(company['quarter_report_url'])

    # Special Handling for Nepali Months (Goodwill)
    month_hint = ""
    if company_symbol == "GFCL":
        month_map = {'Q1': 'Ashoj/Ashwin', 'Q2': 'Poush', 'Q3': 'Chaitra', 'Q4': 'Ashadh'}
        month_hint = f"Look for month: {month_map.get(quarter, '')}"

    prompt = f"""Find the {quarter} REPORT for {nepali_fy}.
    {month_hint}
    Keywords: {quarter}, Quarterly, Interim, Unaudited.
    Return JSON: {{ "found": true, "report": {{ "fiscal_year": "{nepali_fy}", "report_type": "quarterly", "quarter": "{quarter}", "file_url": "url" }} }}"""

    for url in urls:
        print(f"  Scanning: {url}")
        try:
            result = firecrawl.scrape(url, formats=["markdown", {"type": "json", "prompt": prompt}])
            if result.json and result.json.get('found'):
                report = result.json.get('report')
                if report and report.get('file_url'):
                    report['source'] = 'firecrawl'
                    inserted = insert_finance_company_document_to_db(company['id'], company_symbol, report)
                    return {"status": "found", "source": "scraped", "pdf_url": inserted['pdf_url']}
        except Exception as e:
            print(f"  Error scraping {url}: {e}")

    raise HTTPException(status_code=404, detail="Report not found")


# ============================================================================
# MICROFINANCE COMPANY API ENDPOINTS
# ============================================================================

@app.get("/microfinance/annual-report")
def get_microfinance_annual_report(microfinance_symbol: str, fiscal_year: str):
    """
    Get annual report for a microfinance company with dynamic API support
    Example: /microfinance/annual-report?microfinance_symbol=VLBS&fiscal_year=2078/79

    Supports:
    - Dynamic APIs (VLBS, NICLBSL, PROFL, GILB)
    - CSRF Forms (DDBL)
    - Pagination
    - Static Firecrawl
    """
    microfinance_symbol = microfinance_symbol.upper()
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    print(f"\n📊 MICROFINANCE ANNUAL REPORT REQUEST: {microfinance_symbol} - {nepali_fy}")
    print("="*80)
    print(f"📅 Fiscal Year: {nepali_fy} (Nepali) / {english_fy} (English)")

    # Get microfinance company info
    company = get_microfinance_company_info(microfinance_symbol)
    if not company:
        raise HTTPException(status_code=404, detail=f"Microfinance company {microfinance_symbol} not found in database")

    print(f"🏦 Company: {company.get('microfinance_name')} (ID: {company['id']})")

    # 1. Check database first
    print("🔍 Checking database...")
    existing = check_microfinance_company_document_exists(company['id'], nepali_fy, 'annual') or \
               check_microfinance_company_document_exists(company['id'], english_fy, 'annual')

    if existing:
        print("✅ FOUND IN DATABASE!")
        return {
            "status": "found_in_database",
            "source": "database",
            "document": existing
        }

    print("❌ Not in database.")

    # 2. Check for Dynamic API support
    if has_microfinance_dynamic_api(microfinance_symbol):
        print(f"🔌 Microfinance company has dynamic API support - fetching from API...")

        try:
            api_doc = None

            if microfinance_symbol == "VLBS":
                print("  Using Vijaya JWT API")
                api_doc = fetch_from_vijaya_jwt_api(fiscal_year, 'annual')

            elif microfinance_symbol == "NICLBSL":
                print("  Using NicAsia Laghubitta API")
                api_doc = fetch_from_nicbl_api(fiscal_year, 'annual')

            elif microfinance_symbol == "GILB":
                print("  Using Global IME Ninja Tables")
                api_doc = fetch_from_gilb_ninja_tables(fiscal_year, 'annual')

            elif microfinance_symbol == "PROFL":
                print("  Using Progressive Finance API")
                # Use existing PROFL handler
                config = MICROFINANCE_DYNAMIC_API["PROFL"]
                headers = {"x-api-token": config["api_token"]}
                response = requests.get(config["api_url"], headers=headers, timeout=15)
                if response.status_code == 200:
                    documents = response.json()
                    for doc in documents:
                        if doc.get('file_type') == 'Annual Report':
                            doc_fy = doc.get('fiscal_year', '')
                            if fiscal_year in doc_fy or nepali_fy in doc_fy:
                                file_path = doc.get('file_path', '') or doc.get('file_path_url', '')
                                api_doc = {
                                    'pdf_url': file_path,
                                    'fiscal_year': nepali_fy,
                                    'report_type': 'annual',
                                    'source': 'profl_api'
                                }
                                break

            if api_doc:
                print("  ✅ Found via dynamic API")
                inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, api_doc)
                return {
                    "status": "found_via_api",
                    "source": "dynamic_api",
                    "document": inserted
                }
            else:
                print("  ❌ Not found via dynamic API")

        except Exception as e:
            print(f"  ❌ API error: {e}")

    # 3. Check for CSRF Form support
    if has_microfinance_csrf_form(microfinance_symbol):
        print(f"📝 Microfinance company uses CSRF form - fetching...")
        try:
            csrf_doc = fetch_from_ddbl_csrf_form(fiscal_year, 'annual')
            if csrf_doc:
                print("  ✅ Found via CSRF form")
                inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, csrf_doc)
                return {
                    "status": "found_via_csrf",
                    "source": "csrf_form",
                    "document": inserted
                }
            else:
                print("  ❌ Not found via CSRF form")
        except Exception as e:
            print(f"  ❌ CSRF form error: {e}")

    # 4. Check for Pagination
    if has_microfinance_pagination(microfinance_symbol):
        print(f"📄 Microfinance company uses pagination - Checking multiple pages...")
        config = MICROFINANCE_PAGINATED[microfinance_symbol]
        max_pages = config.get("max_pages", 5)
        base_url = config.get("annual_url")
        
        # Only proceed if we have an Annual URL configured
        if base_url:
            for page in range(1, max_pages + 1):
                target_url = base_url.format(page=page)
                print(f"   🔍 Scanning Page {page}: {target_url}")
                
                try:
                    result = firecrawl.scrape(target_url, formats=[
                        "markdown",
                        {
                            "type": "json",
                            "prompt": f"Extract the EXACT direct PDF link for the annual report of fiscal year {nepali_fy} or {english_fy}. Return JSON: {{'fiscal_year': '{nepali_fy}', 'report_type': 'annual', 'pdf_url': '<link>'}}"
                        }
                    ])
                    
                    if result.json and isinstance(result.json, dict):
                        pdf_url = result.json.get('pdf_url')
                        if pdf_url and pdf_url.endswith('.pdf'):
                            # Success! Found it on this page
                            print(f"   ✅ Found on Page {page}")
                            report = {
                                'pdf_url': pdf_url,
                                'fiscal_year': nepali_fy,
                                'report_type': 'annual',
                                'source': 'paginated_scrape'
                            }
                            inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, report)
                            return {
                                "status": "found_via_pagination",
                                "source": "paginated_scrape",
                                "page": page,
                                "document": inserted
                            }
                except Exception as e:
                    print(f"   ⚠️ Error on page {page}: {e}")
                    # Continue to next page even if error
                    continue
        else:
            print("   ⚠️ No annual_url configured for pagination, skipping.")

    # 5. Fallback to Firecrawl scraping
    print("🔍 Falling back to Firecrawl scraping...")

    urls = []
    if company.get('annual_report_url'):
        urls.append(company['annual_report_url'])
    if company.get('report_page'):
        urls.append(company['report_page'])

    if not urls:
        raise HTTPException(status_code=404, detail=f"No annual report URLs configured for {microfinance_symbol}")

    # Try each URL
    for url in urls:
        try:
            print(f"🔍 Scraping: {url}")
            result = firecrawl.scrape(url, formats=[
                "markdown",
                {
                    "type": "json",
                    "prompt": f"Extract the EXACT direct PDF link for the annual report of fiscal year {nepali_fy} or {english_fy}. Return: {{\"fiscal_year\": \"{nepali_fy}\", \"report_type\": \"annual\", \"pdf_url\": \"<direct_pdf_link>\"}}"
                }
            ])

            if result.json and isinstance(result.json, dict):
                pdf_url = result.json.get('pdf_url')
                if pdf_url and pdf_url.endswith('.pdf'):
                    report = {
                        'pdf_url': pdf_url,
                        'fiscal_year': nepali_fy,
                        'report_type': 'annual',
                        'source': 'static'
                    }
                    inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, report)
                    print("✅ FOUND AND SAVED!")
                    return {
                        "status": "found_via_scraping",
                        "source": "firecrawl",
                        "document": inserted
                    }
        except Exception as e:
            print(f"  ❌ Error scraping {url}: {e}")
            continue

    raise HTTPException(status_code=404, detail=f"Annual report for {microfinance_symbol} {nepali_fy} not found")


@app.get("/microfinance/quarterly-report")
def get_microfinance_quarterly_report(microfinance_symbol: str, fiscal_year: str, quarter: str):
    """
    Get quarterly report for a microfinance company with dynamic API support
    Example: /microfinance/quarterly-report?microfinance_symbol=VLBS&fiscal_year=2078/79&quarter=Q1

    Supports:
    - Dynamic APIs (VLBS, NICLBSL, PROFL, GILB)
    - CSRF Forms (DDBL)
    - Pagination
    - Static Firecrawl
    """
    microfinance_symbol = microfinance_symbol.upper()
    quarter = quarter.upper()
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    print(f"\n📊 MICROFINANCE QUARTERLY REPORT REQUEST: {microfinance_symbol} - {nepali_fy} {quarter}")
    print("="*80)
    print(f"📅 Fiscal Year: {nepali_fy} (Nepali) / {english_fy} (English)")
    print(f"📅 Quarter: {quarter}")

    # Validate quarter
    if quarter not in ['Q1', 'Q2', 'Q3', 'Q4']:
        raise HTTPException(status_code=400, detail="Quarter must be Q1, Q2, Q3, or Q4")

    # Get microfinance company info
    company = get_microfinance_company_info(microfinance_symbol)
    if not company:
        raise HTTPException(status_code=404, detail=f"Microfinance company {microfinance_symbol} not found in database")

    print(f"🏦 Company: {company.get('microfinance_name')} (ID: {company['id']})")

    # 1. Check database first
    print(f"🔍 Checking database for {quarter}...")
    existing = check_microfinance_company_document_exists(company['id'], nepali_fy, 'quarterly', quarter) or \
               check_microfinance_company_document_exists(company['id'], english_fy, 'quarterly', quarter)

    if existing:
        print(f"✅ FOUND {quarter} IN DATABASE!")
        return {
            "status": "found_in_database",
            "source": "database",
            "document": existing
        }

    print(f"❌ {quarter} not in database.")

    # 2. Check for Dynamic API support
    if has_microfinance_dynamic_api(microfinance_symbol):
        print(f"🔌 Microfinance company has dynamic API support - fetching from API...")

        try:
            api_doc = None

            if microfinance_symbol == "VLBS":
                print("  Using Vijaya JWT API")
                api_doc = fetch_from_vijaya_jwt_api(fiscal_year, 'quarterly', quarter)

            elif microfinance_symbol == "NICLBSL":
                print("  Using NicAsia Laghubitta API")
                api_doc = fetch_from_nicbl_api(fiscal_year, 'quarterly', quarter)

            elif microfinance_symbol == "GILB":
                print("  Using Global IME Ninja Tables")
                api_doc = fetch_from_gilb_ninja_tables(fiscal_year, 'quarterly', quarter)

            elif microfinance_symbol == "PROFL":
                print("  Using Progressive Finance API")
                config = MICROFINANCE_DYNAMIC_API["PROFL"]
                headers = {"x-api-token": config["api_token"]}
                response = requests.get(config["api_url"], headers=headers, timeout=15)
                if response.status_code == 200:
                    documents = response.json()
                    for doc in documents:
                        if doc.get('file_type') == 'Quarterly Report':
                            doc_fy = doc.get('fiscal_year', '')
                            doc_title = doc.get('file_title', '').lower()

                            # Check fiscal year and quarter
                            if (fiscal_year in doc_fy or nepali_fy in doc_fy):
                                quarter_match = False
                                if quarter == 'Q1' and any(k in doc_title for k in ['first', '1st', 'q1']):
                                    quarter_match = True
                                elif quarter == 'Q2' and any(k in doc_title for k in ['second', '2nd', 'q2']):
                                    quarter_match = True
                                elif quarter == 'Q3' and any(k in doc_title for k in ['third', '3rd', 'q3']):
                                    quarter_match = True
                                elif quarter == 'Q4' and any(k in doc_title for k in ['fourth', '4th', 'q4']):
                                    quarter_match = True

                                if quarter_match:
                                    file_path = doc.get('file_path', '') or doc.get('file_path_url', '')
                                    api_doc = {
                                        'pdf_url': file_path,
                                        'fiscal_year': nepali_fy,
                                        'report_type': 'quarterly',
                                        'quarter': quarter,
                                        'source': 'profl_api'
                                    }
                                    break

            if api_doc:
                print(f"  ✅ Found {quarter} via dynamic API")
                inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, api_doc)
                return {
                    "status": "found_via_api",
                    "source": "dynamic_api",
                    "document": inserted
                }
            else:
                print(f"  ❌ {quarter} not found via dynamic API")

        except Exception as e:
            print(f"  ❌ API error: {e}")

    # 3. Check for CSRF Form support
    if has_microfinance_csrf_form(microfinance_symbol):
        print(f"📝 Microfinance company uses CSRF form - fetching...")
        try:
            csrf_doc = fetch_from_ddbl_csrf_form(fiscal_year, 'quarterly', quarter)
            if csrf_doc:
                print(f"  ✅ Found {quarter} via CSRF form")
                inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, csrf_doc)
                return {
                    "status": "found_via_csrf",
                    "source": "csrf_form",
                    "document": inserted
                }
            else:
                print(f"  ❌ {quarter} not found via CSRF form")
        except Exception as e:
            print(f"  ❌ CSRF form error: {e}")

    # 4. Check for Pagination
    if has_microfinance_pagination(microfinance_symbol):
        print(f"📄 Microfinance company uses pagination - Checking multiple pages...")
        config = MICROFINANCE_PAGINATED[microfinance_symbol]
        max_pages = config.get("max_pages", 5)
        base_url = config.get("quarterly_url")
        
        # Determine keywords for the quarter
        q_keywords = {
            'Q1': 'First,1st,Ashwin,Asoj',
            'Q2': 'Second,2nd,Poush,Mid-Year',
            'Q3': 'Third,3rd,Chaitra',
            'Q4': 'Fourth,4th,Ashad,Ashadh,Annual'
        }
        keywords = q_keywords.get(quarter, quarter)
        
        if base_url:
            for page in range(1, max_pages + 1):
                target_url = base_url.format(page=page)
                print(f"   🔍 Scanning Page {page}: {target_url}")
                
                try:
                    result = firecrawl.scrape(target_url, formats=[
                        "markdown",
                        {
                            "type": "json",
                            "prompt": f"Find the {quarter} ({keywords}) quarterly/interim report for {nepali_fy}. Return JSON: {{'fiscal_year': '{nepali_fy}', 'quarter': '{quarter}', 'report_type': 'quarterly', 'pdf_url': '<link>'}}"
                        }
                    ])
                    
                    if result.json and isinstance(result.json, dict):
                        pdf_url = result.json.get('pdf_url')
                        if pdf_url:  # Allow images too if needed, but prefer PDF
                            print(f"   ✅ Found on Page {page}")
                            report = {
                                'pdf_url': pdf_url,
                                'fiscal_year': nepali_fy,
                                'report_type': 'quarterly',
                                'quarter': quarter,
                                'source': 'paginated_scrape'
                            }
                            inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, report)
                            return {
                                "status": "found_via_pagination",
                                "source": "paginated_scrape",
                                "page": page,
                                "document": inserted
                            }
                except Exception as e:
                    print(f"   ⚠️ Error on page {page}: {e}")
                    continue
        else:
            print("   ⚠️ No quarterly_url configured for pagination, skipping.")

    # 5. Fallback to Firecrawl scraping
    print(f"🔍 Falling back to Firecrawl scraping for {quarter}...")

    urls = []
    if company.get('quarter_report_url'):
        urls.append(company['quarter_report_url'])
    if company.get('report_page'):
        urls.append(company['report_page'])

    if not urls:
        raise HTTPException(status_code=404, detail=f"No quarterly report URLs configured for {microfinance_symbol}")

    # Map quarter to keywords
    quarter_keywords = {
        'Q1': 'first|1st|ashwin',
        'Q2': 'second|2nd|poush|mid-term',
        'Q3': 'third|3rd|chaitra|nine month',
        'Q4': 'fourth|4th|ashad'
    }

    # Try each URL
    for url in urls:
        try:
            print(f"🔍 Scraping: {url}")
            result = firecrawl.scrape(url, formats=[
                "markdown",
                {
                    "type": "json",
                    "prompt": f"Extract the EXACT direct PDF link for the {quarter} ({quarter_keywords[quarter]}) quarterly/interim report of fiscal year {nepali_fy} or {english_fy}. Return: {{\"fiscal_year\": \"{nepali_fy}\", \"report_type\": \"quarterly\", \"quarter\": \"{quarter}\", \"pdf_url\": \"<direct_pdf_link>\"}}"
                }
            ])

            if result.json and isinstance(result.json, dict):
                pdf_url = result.json.get('pdf_url')
                if pdf_url and pdf_url.endswith('.pdf'):
                    report = {
                        'pdf_url': pdf_url,
                        'fiscal_year': nepali_fy,
                        'report_type': 'quarterly',
                        'quarter': quarter,
                        'source': 'static'
                    }
                    inserted = insert_microfinance_company_document_to_db(company['id'], microfinance_symbol, report)
                    print(f"✅ FOUND {quarter} AND SAVED!")
                    return {
                        "status": "found_via_scraping",
                        "source": "firecrawl",
                        "document": inserted
                    }
        except Exception as e:
            print(f"  ❌ Error scraping {url}: {e}")
            continue

    raise HTTPException(status_code=404, detail=f"Quarterly report {quarter} for {microfinance_symbol} {nepali_fy} not found")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8002)

