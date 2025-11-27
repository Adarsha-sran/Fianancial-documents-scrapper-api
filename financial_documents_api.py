"""
Financial Documents API - On-Demand Report Scraping and Retrieval
Provides endpoints to fetch specific annual/quarterly reports with intelligent scraping
"""

import os
import re
import time
import requests
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

# Validate required environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

if not FIRECRAWL_API_KEY:
    raise ValueError("FIRECRAWL_API_KEY must be set in environment variables")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)

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


def normalize_fiscal_year_format(fiscal_year: str) -> str:
    """Normalize fiscal year to short format (2078/79)"""
    if not fiscal_year or '/' not in fiscal_year:
        return fiscal_year
    parts = fiscal_year.split('/')
    if len(parts) != 2:
        return fiscal_year
    year1 = parts[0].strip()
    year2 = parts[1].strip()
    if len(year2) == 4:
        year2 = year2[-2:]
    return f"{year1}/{year2}"


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
    try:
        doc_data = {
            'bank_id': bank_id, 'bank_symbol': bank_symbol, 'pdf_url': report['file_url'],
            'fiscal_year': report['fiscal_year'], 'report_type': report['report_type'],
            'quarter': report.get('quarter'), 'scraped_at': datetime.now().isoformat(), 'method': 'api'
        }
        result = supabase.table("financial_documents").insert(doc_data).execute()
        if result.data and len(result.data) > 0: return result.data[0]
        return None
    except Exception as e:
        print(f"Error inserting document: {e}")
        raise


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
    title = title.lower()
    keywords = {'q1': 'Q1', 'q2': 'Q2', 'q3': 'Q3', 'q4': 'Q4', '1st': 'Q1', '2nd': 'Q2', '3rd': 'Q3', '4th': 'Q4',
                'first': 'Q1', 'second': 'Q2', 'third': 'Q3', 'fourth': 'Q4'}
    for k, v in keywords.items():
        if k in title: return v
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8002)
