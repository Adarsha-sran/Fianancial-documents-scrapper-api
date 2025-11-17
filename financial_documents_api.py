"""
Financial Documents API - On-Demand Report Scraping and Retrieval
Provides endpoints to fetch specific annual/quarterly reports with intelligent scraping
"""

import os
from datetime import datetime
from typing import Optional, Dict, List
from dotenv import load_dotenv
from supabase import create_client
from firecrawl import Firecrawl
from fastapi import FastAPI, HTTPException
import time

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




def normalize_fiscal_year(fiscal_year: str) -> tuple:
    """
    Normalize fiscal year to Nepali format and return both formats
    Returns: (nepali_format, english_format)
    """
    fiscal_year = fiscal_year.strip()

    # Check if it's in English format (< 2030)
    try:
        year_start = int(fiscal_year.split('/')[0])
        if year_start < 2030:
            # Convert to Nepali
            nepali_fy = FISCAL_YEAR_CONVERSION.get(fiscal_year, fiscal_year)
            return nepali_fy, fiscal_year
    except:
        pass

    # Already in Nepali format or invalid
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


def check_document_exists(bank_id: int, fiscal_year: str, report_type: str, quarter: Optional[str] = None) -> Optional[Dict]:
    """Check if document already exists in financial_documents table"""
    try:
        # Check with primary fiscal year format
        query = supabase.table("financial_documents").select("*").eq("bank_id", bank_id).eq("fiscal_year", fiscal_year).eq("report_type", report_type)

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
            query_alt = supabase.table("financial_documents").select("*").eq("bank_id", bank_id).eq("fiscal_year", alt_fy).eq("report_type", report_type)

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
        # Priority: annual_report_url > report_page > website
        if bank.get('annual_report_url'):
            urls.append((bank['annual_report_url'], 'annual_report_url'))
        if bank.get('report_page'):
            urls.append((bank['report_page'], 'report_page'))

    elif report_type == "quarterly":
        # Priority: quarter_report_url > report_page > website
        if bank.get('quarter_report_url'):
            urls.append((bank['quarter_report_url'], 'quarter_report_url'))
        if bank.get('report_page'):
            urls.append((bank['report_page'], 'report_page'))

    # Fallback to website if no specific URLs
    if not urls and bank.get('website'):
        urls.append((bank['website'], 'website'))

    return urls


def create_scraping_prompt(report_type: str, fiscal_year: str, quarter: Optional[str] = None) -> str:
    """Create highly specific scraping prompt based on report type"""

    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)

    if report_type == "annual":
        return f"""Extract ONLY the annual report (also called yearly report, annual financial report, or audited annual report) for fiscal year {nepali_fy} or {english_fy}.

IMPORTANT CRITERIA:
- Must be an ANNUAL report (NOT quarterly, NOT interim, NOT unaudited quarterly)
- Must be for fiscal year {nepali_fy} (Nepali calendar) or {english_fy} (English calendar)
- Keywords to look for: "Annual Report", "Yearly Report", "Audited Annual", "वार्षिक प्रतिवेदन"
- Keywords to AVOID: "Quarterly", "Interim", "Unaudited", "Q1", "Q2", "Q3", "Q4", "त्रैमासिक"

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

    else:  # quarterly
        quarter_names = {
            "Q1": "First Quarter (Q1, 1st Quarter, Ashad End, आषाढ अन्त)",
            "Q2": "Second Quarter (Q2, 2nd Quarter, Ashwin End, आश्विन अन्त, Mid-term, Half Yearly)",
            "Q3": "Third Quarter (Q3, 3rd Quarter, Poush End, पौष अन्त, Nine Month)",
            "Q4": "Fourth Quarter (Q4, 4th Quarter, Chaitra End, चैत्र अन्त, Pre-final)"
        }

        quarter_desc = quarter_names.get(quarter.upper(), quarter)

        return f"""Extract ONLY the quarterly/interim report for {quarter_desc} of fiscal year {nepali_fy} or {english_fy}.

IMPORTANT CRITERIA:
- Must be a QUARTERLY/INTERIM/UNAUDITED report (NOT annual report)
- Must be specifically for {quarter.upper()} (Quarter {quarter[1]}) of fiscal year {nepali_fy} or {english_fy}
- Keywords for {quarter.upper()}: {quarter_desc}
- Keywords to AVOID: "Annual Report", "Yearly Report", "Audited Annual", "वार्षिक प्रतिवेदन"

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


def scrape_specific_report(bank: Dict, fiscal_year: str, report_type: str, quarter: Optional[str] = None, max_retries: int = 3) -> Optional[Dict]:
    """Scrape for a specific report with intelligent URL selection and prompting"""

    urls = get_scraping_urls(bank, report_type)

    if not urls:
        print(f"   ⚠️ No URLs available for scraping")
        return None

    prompt = create_scraping_prompt(report_type, fiscal_year, quarter)

    # Try each URL until we find the report
    for url, url_type in urls:
        print(f"🔍 Searching in {url_type}: {url}")

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"   🔄 Retry {attempt + 1}/{max_retries}")
                    time.sleep(5 * attempt)  # Increased wait time

                result = firecrawl.scrape(url, formats=[
                    "markdown",
                    {
                        "type": "json",
                        "prompt": prompt
                    }
                ])

                # Check for HTTP errors in metadata
                if result.metadata:
                    status_code = result.metadata.status_code
                    error = result.metadata.error

                    if status_code and status_code >= 400:
                        print(f"   ⚠️ HTTP {status_code}: {error or 'Error'}")

                        # Handle specific error codes
                        if status_code == 502 or status_code == 503:
                            print(f"   ⚠️ Server unavailable ({status_code}) - Attempt {attempt + 1}/{max_retries}")
                            if attempt < max_retries - 1:
                                print(f"   ⏳ Waiting 20 seconds before retry...")
                                time.sleep(20)
                                continue
                            else:
                                print(f"   ❌ Server unavailable after {max_retries} attempts, trying next URL...")
                                break
                        elif status_code == 429:
                            print(f"   ⚠️ Rate limited - Attempt {attempt + 1}/{max_retries}")
                            if attempt < max_retries - 1:
                                print(f"   ⏳ Waiting 30 seconds before retry...")
                                time.sleep(30)
                                continue
                            else:
                                print(f"   ❌ Rate limited after {max_retries} attempts")
                                break
                        else:
                            # Other error codes - try next URL
                            print(f"   ❌ Skipping to next URL due to HTTP error")
                            break

                if not result.json:
                    print(f"   ⚠️ No JSON response from {url_type}")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        break  # Try next URL

                # Check if report was found
                found = result.json.get('found', False)
                report = result.json.get('report')

                if found and report and report.get('file_url'):
                    print(f"   ✅ Found report in {url_type}")
                    return report
                else:
                    print(f"   ❌ Report not found in {url_type}")
                    break  # No need to retry if report not found

            except Exception as e:
                error_msg = str(e)

                if '503' in error_msg or 'upstream connect error' in error_msg.lower() or 'server' in error_msg.lower():
                    print(f"   ⚠️ Server unavailable (503) - Attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        print(f"   ⏳ Waiting 15 seconds before retry...")
                        time.sleep(15)
                        continue
                    else:
                        print(f"   ❌ Server unavailable after {max_retries} attempts, trying next URL...")
                        break

                elif '429' in error_msg or 'rate limit' in error_msg.lower():
                    print(f"   ⚠️ Rate limited - Attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        print(f"   ⏳ Waiting 30 seconds before retry...")
                        time.sleep(30)
                        continue
                    else:
                        print(f"   ❌ Rate limited after {max_retries} attempts")
                        break

                elif 'timeout' in error_msg.lower():
                    print(f"   ⚠️ Timeout - Attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(10)
                        continue
                    else:
                        print(f"   ❌ Timeout after {max_retries} attempts, trying next URL...")
                        break

                else:
                    print(f"   ❌ Error: {error_msg[:150]}")
                    if attempt < max_retries - 1:
                        time.sleep(5)
                        continue
                    else:
                        break

        # Small delay before trying next URL
        print(f"   ⏸️ Waiting 3 seconds before trying next URL...")
        time.sleep(3)

    # Report not found in any URL
    print(f"❌ Report not found after checking all available URLs")
    return None


def insert_document_to_db(bank_id: int, bank_symbol: str, report: Dict) -> Dict:
    """Insert scraped report into financial_documents table"""
    try:
        doc_data = {
            'bank_id': bank_id,
            'bank_symbol': bank_symbol,
            'pdf_url': report['file_url'],
            'fiscal_year': report['fiscal_year'],
            'report_type': report['report_type'],
            'quarter': report.get('quarter'),
            'scraped_at': datetime.now().isoformat(),
            'method': 'api'  # Using api for API-based scraping
        }

        result = supabase.table("financial_documents").insert(doc_data).execute()

        if result.data and len(result.data) > 0:
            return result.data[0]

        return None
    except Exception as e:
        print(f"Error inserting document: {e}")
        raise


@app.get("/")
def root():
    """API root endpoint"""
    return {
        "message": "Financial Documents API",
        "version": "1.0.0",
        "endpoints": {
            "annual_report": "/annual-report?bank_symbol=ADBL&fiscal_year=2078/79",
            "quarterly_report": "/quarterly-report?bank_symbol=ADBL&fiscal_year=2078/79&quarter=Q1",
            "health": "/health"
        }
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": "connected",
        "scraper": "ready"
    }


@app.get("/diagnose/{bank_symbol}")
def diagnose_bank_website(bank_symbol: str):
    """
    Diagnose if a bank's website is accessible by Firecrawl
    Helps identify if issues are with the bank's website or the API
    """
    bank_symbol = bank_symbol.upper()

    # Get bank info
    bank = get_bank_info(bank_symbol)
    if not bank:
        raise HTTPException(status_code=404, detail=f"Bank '{bank_symbol}' not found")

    results = {
        "bank_symbol": bank_symbol,
        "bank_name": bank['bank_name'],
        "timestamp": datetime.now().isoformat(),
        "urls_tested": {}
    }

    # Test each URL
    test_urls = []
    if bank.get('report_page'):
        test_urls.append(('report_page', bank['report_page']))
    if bank.get('annual_report_url'):
        test_urls.append(('annual_report_url', bank['annual_report_url']))
    if bank.get('quarter_report_url'):
        test_urls.append(('quarter_report_url', bank['quarter_report_url']))

    for url_type, url in test_urls:
        print(f"Testing {url_type}: {url}")
        try:
            result = firecrawl.scrape(url, formats=["markdown"])

            status_code = result.metadata.status_code if result.metadata else None
            error = result.metadata.error if result.metadata else None
            content_length = len(result.markdown) if result.markdown else 0

            results["urls_tested"][url_type] = {
                "url": url,
                "status_code": status_code,
                "error": error,
                "content_length": content_length,
                "accessible": status_code == 200 and content_length > 0,
                "issue": None if status_code == 200 else f"HTTP {status_code}: {error}"
            }

        except Exception as e:
            results["urls_tested"][url_type] = {
                "url": url,
                "status_code": None,
                "error": str(e),
                "content_length": 0,
                "accessible": False,
                "issue": str(e)
            }

        time.sleep(2)  # Be polite between requests

    # Overall diagnosis
    accessible_count = sum(1 for r in results["urls_tested"].values() if r["accessible"])
    total_count = len(results["urls_tested"])

    results["diagnosis"] = {
        "accessible_urls": f"{accessible_count}/{total_count}",
        "overall_status": "OK" if accessible_count > 0 else "UNAVAILABLE",
        "recommendation": (
            "Bank website is accessible. API should work normally."
            if accessible_count > 0
            else "Bank website is currently unavailable or blocking scrapers. Please try again later."
        )
    }

    return results


@app.get("/annual-report")
def get_annual_report(bank_symbol: str, fiscal_year: str):
    """
    Get annual report for a specific bank and fiscal year

    - Checks database first
    - If not found, scrapes the bank's website
    - Stores and returns the report
    """

    print(f"\n{'='*80}")
    print(f"📊 ANNUAL REPORT REQUEST: {bank_symbol} - {fiscal_year}")
    print(f"{'='*80}")

    bank_symbol = bank_symbol.upper()

    # Normalize fiscal year
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
    print(f"📅 Fiscal Year: {nepali_fy} (Nepali) / {english_fy} (English)")

    # Get bank info
    bank = get_bank_info(bank_symbol)
    if not bank:
        raise HTTPException(status_code=404, detail=f"Bank '{bank_symbol}' not found")

    print(f"🏦 Bank: {bank['bank_name']} ({bank_symbol})")

    # Check if document exists (try both formats)
    print(f"🔍 Checking database...")
    existing = check_document_exists(bank['id'], nepali_fy, 'annual')

    if not existing:
        existing = check_document_exists(bank['id'], english_fy, 'annual')

    if existing:
        print(f"✅ Found in database!")
        return {
            "status": "found",
            "source": "database",
            "bank_symbol": bank_symbol,
            "bank_name": bank['bank_name'],
            "fiscal_year": existing['fiscal_year'],
            "report_type": "annual",
            "quarter": None,
            "pdf_url": existing['pdf_url'],
            "scraped_at": existing['scraped_at'],
            "method": existing.get('method', 'unknown')
        }

    # Not in database - scrape
    print(f"❌ Not in database. Starting scraping...")

    report = scrape_specific_report(bank, nepali_fy, 'annual')

    if not report:
        print(f"❌ Report not found after scraping")

        # Check if we can diagnose the issue
        urls = get_scraping_urls(bank, 'annual')
        error_detail = f"Annual report for {bank_symbol} {nepali_fy} not found. "

        if urls:
            error_detail += "The bank's website might be temporarily unavailable or blocking our scraper. "
            error_detail += f"Try using /diagnose/{bank_symbol} endpoint to check website accessibility."
        else:
            error_detail += "No report URLs configured for this bank in the database."

        raise HTTPException(
            status_code=404,
            detail=error_detail
        )

    # Insert into database
    print(f"💾 Saving to database...")
    inserted = insert_document_to_db(bank['id'], bank_symbol, report)

    print(f"✅ Success!")

    return {
        "status": "found",
        "source": "scraped",
        "bank_symbol": bank_symbol,
        "bank_name": bank['bank_name'],
        "fiscal_year": report['fiscal_year'],
        "report_type": "annual",
        "quarter": None,
        "pdf_url": report['file_url'],
        "scraped_at": datetime.now().isoformat(),
        "method": "api"
    }


@app.get("/quarterly-report")
def get_quarterly_report(bank_symbol: str, fiscal_year: str, quarter: str):
    """
    Get quarterly report for a specific bank, fiscal year, and quarter

    - Checks database first
    - If not found, scrapes the bank's website
    - Stores and returns the report
    """

    print(f"\n{'='*80}")
    print(f"📊 QUARTERLY REPORT REQUEST: {bank_symbol} - {fiscal_year} - {quarter}")
    print(f"{'='*80}")

    bank_symbol = bank_symbol.upper()
    quarter = quarter.upper()

    # Validate quarter
    if quarter not in ['Q1', 'Q2', 'Q3', 'Q4']:
        raise HTTPException(status_code=400, detail="Quarter must be Q1, Q2, Q3, or Q4")

    # Normalize fiscal year
    nepali_fy, english_fy = normalize_fiscal_year(fiscal_year)
    print(f"📅 Fiscal Year: {nepali_fy} (Nepali) / {english_fy} (English)")
    print(f"📅 Quarter: {quarter}")

    # Get bank info
    bank = get_bank_info(bank_symbol)
    if not bank:
        raise HTTPException(status_code=404, detail=f"Bank '{bank_symbol}' not found")

    print(f"🏦 Bank: {bank['bank_name']} ({bank_symbol})")

    # Check if document exists (try both formats)
    print(f"🔍 Checking database...")
    existing = check_document_exists(bank['id'], nepali_fy, 'quarterly', quarter)

    if not existing:
        existing = check_document_exists(bank['id'], english_fy, 'quarterly', quarter)

    if existing:
        print(f"✅ Found in database!")
        return {
            "status": "found",
            "source": "database",
            "bank_symbol": bank_symbol,
            "bank_name": bank['bank_name'],
            "fiscal_year": existing['fiscal_year'],
            "report_type": "quarterly",
            "quarter": quarter,
            "pdf_url": existing['pdf_url'],
            "scraped_at": existing['scraped_at'],
            "method": existing.get('method', 'unknown')
        }

    # Not in database - scrape
    print(f"❌ Not in database. Starting scraping...")

    report = scrape_specific_report(bank, nepali_fy, 'quarterly', quarter)

    if not report:
        print(f"❌ Report not found after scraping")

        # Check if we can diagnose the issue
        urls = get_scraping_urls(bank, 'quarterly')
        error_detail = f"Quarterly report {quarter} for {bank_symbol} {nepali_fy} not found. "

        if urls:
            error_detail += "The bank's website might be temporarily unavailable or blocking our scraper. "
            error_detail += f"Try using /diagnose/{bank_symbol} endpoint to check website accessibility."
        else:
            error_detail += "No report URLs configured for this bank in the database."

        raise HTTPException(
            status_code=404,
            detail=error_detail
        )

    # Insert into database
    print(f"💾 Saving to database...")
    inserted = insert_document_to_db(bank['id'], bank_symbol, report)

    print(f"✅ Success!")

    return {
        "status": "found",
        "source": "scraped",
        "bank_symbol": bank_symbol,
        "bank_name": bank['bank_name'],
        "fiscal_year": report['fiscal_year'],
        "report_type": "quarterly",
        "quarter": quarter,
        "pdf_url": report['file_url'],
        "scraped_at": datetime.now().isoformat(),
        "method": "api"
    }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)

