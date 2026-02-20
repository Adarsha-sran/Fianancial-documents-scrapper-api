# Financial Documents API

> **Internal Company API** - On-demand scraping and retrieval of Nepali bank financial reports

## Overview

The Financial Documents API provides intelligent, on-demand scraping and retrieval of annual and quarterly financial reports from Nepali commercial banks. It automatically checks the database for existing reports before scraping, ensuring efficient resource usage and avoiding duplicate API calls.

## Features

- ✅ **Smart Database Check**: Automatically checks if report exists before scraping
- ✅ **On-Demand Scraping**: Fetches reports only when needed
- ✅ **Dual Format Support**: Handles both Nepali (2078/79) and English (2021/22) fiscal year formats
- ✅ **Annual Reports**: Full audited yearly financial reports
- ✅ **Quarterly Reports**: Q1, Q2, Q3, Q4 interim/unaudited reports
- ✅ **Multi-URL Strategy**: Tries multiple bank URLs (report page, annual URL, quarterly URL)
- ✅ **Automatic Storage**: Saves scraped reports to database for future use
- ✅ **Website Diagnostics**: Check bank website accessibility before scraping

## Technology Stack

- **Framework**: FastAPI (Python)
- **Database**: Supabase (PostgreSQL)
- **Web Scraping**: Firecrawl API (handles JavaScript-rendered content)
- **AI Model**: Gemini API (for intelligent document extraction)

## Prerequisites

1. **Python 3.10+** installed
2. **Environment Variables** configured in `.env` file:
   ```env
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_KEY=your_supabase_anon_key
   FIRECRAWL_API_KEY=your_firecrawl_api_key
   ```

## Installation

1. **Clone the repository** (or ensure you have the file)
   ```bash
   cd C:\Users\Adarsha Rimal\Desktop\sran_beta
   ```

2. **Create and activate virtual environment** (if not already created)
   ```powershell
   python -m venv sran_beta
   .\sran_beta\Scripts\Activate.ps1
   ```

3. **Install dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

4. **Verify `.env` file** contains required variables

## Running the API

**Start the server:**
```powershell
python financial_documents_api.py
```

The API will be available at:
- **Local**: `http://127.0.0.1:8000`
- **Swagger UI**: `http://127.0.0.1:8000/docs`
- **ReDoc**: `http://127.0.0.1:8000/redoc`

## API Endpoints

### 1. Get Annual Report
**GET** `/annual-report`

Retrieves annual (yearly/audited) report for a specific bank and fiscal year.

**Parameters:**
- `bank_symbol` (string, required): Bank symbol (e.g., ADBL, NICA, HBL)
- `fiscal_year` (string, required): Fiscal year in Nepali (2078/79) or English (2021/22) format

**Example Request:**
```
GET /annual-report?bank_symbol=ADBL&fiscal_year=2078/79
```

**Example Response:**
```json
{
  "status": "found",
  "source": "database",
  "bank_symbol": "ADBL",
  "bank_name": "Agricultural Development Bank Limited",
  "fiscal_year": "2078/79",
  "report_type": "annual",
  "quarter": null,
  "pdf_url": "https://adb1backend.adbl.gov.np/storage/reports/2024/07/4341-ADBL_WEB_Annual_Report__2078_79.pdf",
  "scraped_at": "2025-11-07T12:30:00",
  "method": "static"
}
```

### 2. Get Quarterly Report
**GET** `/quarterly-report`

Retrieves quarterly/interim report for a specific bank, fiscal year, and quarter.

**Parameters:**
- `bank_symbol` (string, required): Bank symbol (e.g., CZBIL, NBL)
- `fiscal_year` (string, required): Fiscal year in Nepali (2078/79) or English (2021/22) format
- `quarter` (string, required): Quarter (Q1, Q2, Q3, or Q4)

**Example Request:**
```
GET /quarterly-report?bank_symbol=CZBIL&fiscal_year=2080/81&quarter=Q2
```

**Example Response:**
```json
{
  "status": "found",
  "source": "scraped",
  "bank_symbol": "CZBIL",
  "bank_name": "Citizen Bank International Limited",
  "fiscal_year": "2080/81",
  "report_type": "quarterly",
  "quarter": "Q2",
  "pdf_url": "https://www.ctznbank.com/uploads/reports/Q2_2080_81.pdf",
  "scraped_at": "2025-11-07T14:45:00",
  "method": "api"
}
```

### 3. Diagnose Bank Website
**GET** `/diagnose/{bank_symbol}`

Checks accessibility of a bank's website URLs before attempting to scrape.

**Example Request:**
```
GET /diagnose/ADBL
```

**Example Response:**
```json
{
  "bank_symbol": "ADBL",
  "bank_name": "Agricultural Development Bank Limited",
  "timestamp": "2025-11-07T15:00:00",
  "urls_tested": {
    "report_page": {
      "url": "https://adbl.gov.np/en/reports",
      "status_code": 200,
      "content_length": 45678,
      "accessible": true,
      "issue": null
    },
    "annual_report_url": {
      "url": "https://adbl.gov.np/en/reports/financial-annual-report",
      "status_code": 200,
      "content_length": 32145,
      "accessible": true,
      "issue": null
    }
  },
  "diagnosis": {
    "accessible_urls": "2/2",
    "overall_status": "OK",
    "recommendation": "Bank website is accessible. API should work normally."
  }
}
```

### 4. Health Check
**GET** `/health`

Simple health check endpoint to verify API is running.

**Example Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-07T15:30:00"
}
```

## Supported Banks

The API supports all banks configured in the `banks` table. Common bank symbols:

| Symbol | Bank Name |
|--------|-----------|
| ADBL | Agricultural Development Bank Limited |
| NICA | NIC Asia Bank Limited |
| HBL | Himalayan Bank Limited |
| NABIL | Nabil Bank Limited |
| CZBIL | Citizen Bank International Limited |
| NBL | Nepal Bank Limited |
| LSL | Laxmi Sunrise Bank Limited |
| MBL | Machhapuchchhre Bank Limited |
| ...and more | (19 banks total) |

## Fiscal Year Format

The API intelligently handles both Nepali and English fiscal year formats:

| English (AD) | Nepali (BS) |
|--------------|-------------|
| 2018/19 | 2075/76 |
| 2019/20 | 2076/77 |
| 2020/21 | 2077/78 |
| 2021/22 | 2078/79 |
| 2022/23 | 2079/80 |
| 2023/24 | 2080/81 |
| 2024/25 | 2081/82 |

**Note**: You can use either format in your requests - the API will automatically convert and search both formats.

## Database Schema

### `financial_documents` Table

| Column | Type | Description |
|--------|------|-------------|
| id | integer | Primary key |
| bank_id | integer | Foreign key to banks table |
| bank_symbol | varchar(10) | Bank symbol (e.g., ADBL) |
| pdf_url | text | Direct PDF download link |
| fiscal_year | varchar(10) | Fiscal year (e.g., 2078/79) |
| report_type | varchar(20) | 'annual' or 'quarterly' |
| quarter | varchar(5) | Q1/Q2/Q3/Q4 (null for annual) |
| scraped_at | timestamp | When the report was scraped |
| method | varchar(20) | 'static', 'dynamic', 'manual', or 'api' |
| added_by | varchar(100) | Name of person (for manual entries) |

## How It Works

### Workflow for Annual Report Request

1. **Request Received**: `/annual-report?bank_symbol=ADBL&fiscal_year=2078/79`

2. **Normalize Fiscal Year**: Convert to both formats (2078/79 ↔ 2021/22)

3. **Database Check**: Search `financial_documents` table for existing report
   - Check Nepali format first
   - If not found, check English format
   - If found → Return from database ✅

4. **Scraping** (if not in database):
   - Get bank URLs from `banks` table
   - Try in priority order: `annual_report_url` → `report_page` → `website`
   - Use Firecrawl to scrape page content
   - Use AI prompt to extract specific report
   - Validate extracted data

5. **Storage**: Save scraped report to `financial_documents` table

6. **Response**: Return report details with PDF URL

### Workflow for Quarterly Report Request

Similar to annual reports, but:
- Checks `quarter_report_url` first
- Uses quarter-specific AI prompts
- Validates quarter format (Q1-Q4)
- Stores with quarter information

## Error Handling

### Common Errors

| Status Code | Error | Cause |
|-------------|-------|-------|
| 404 | Bank not found | Invalid bank symbol |
| 404 | Report not found | Report doesn't exist on bank website |
| 400 | Invalid quarter | Quarter must be Q1, Q2, Q3, or Q4 |
| 500 | Scraping failed | Website unavailable or blocking |
| 503 | Service unavailable | Bank server is down |

### Handling Failed Scraping

If scraping fails:
1. Use `/diagnose/{bank_symbol}` to check website accessibility
2. Wait a few minutes and retry
3. Check if the bank website structure changed
4. Consider manual entry if persistent

## Best Practices

### For Developers

1. **Always check database first** - The API does this automatically
2. **Use `/diagnose` endpoint** before bulk scraping operations
3. **Handle 404 gracefully** - Report might not exist
4. **Respect rate limits** - API includes 3-second delays between requests
5. **Cache responses** - Database already caches, but client-side caching helps

### For Users

1. **Use correct bank symbols** - Check the banks table
2. **Both fiscal year formats work** - Use whichever is convenient
3. **For quarterly reports, specify Q1-Q4** - Be precise
4. **Check availability** - Not all banks publish all quarters

## Troubleshooting

### API Not Starting

```powershell
# Check Python version
python --version  # Should be 3.10+

# Check if port is in use
netstat -ano | findstr :8000

# Verify environment variables
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('SUPABASE_URL'))"
```

### Reports Not Found

1. **Verify bank symbol**: Check `banks` table for correct symbol
2. **Check fiscal year**: Ensure fiscal year exists (2060/61 to 2081/82)
3. **Diagnose website**: Use `/diagnose/{bank_symbol}` endpoint
4. **Manual check**: Visit bank website to confirm report exists

### Scraping Errors

- **503 errors**: Bank server temporarily down - retry later
- **Timeout errors**: Bank website slow - retry with longer timeout
- **Empty response**: Page structure changed - may need prompt update

## Performance Considerations

- **First request**: May take 10-30 seconds (scraping + AI extraction)
- **Subsequent requests**: < 1 second (from database)
- **Rate limiting**: Built-in 3-second delays to be respectful
- **Firecrawl credits**: Monitor API usage to avoid exhaustion

## Maintenance

### Regular Tasks

1. **Monitor Firecrawl credits** - Check usage dashboard
2. **Verify bank URLs** - Bank websites may change
3. **Update fiscal year conversion** - Add new years as needed
4. **Check database size** - Archive old reports if needed

### Updating Bank URLs

If a bank changes their website:
1. Update `banks` table with new URLs
2. Test with `/diagnose/{bank_symbol}`
3. Verify scraping works with `/annual-report` or `/quarterly-report`

## API Integration Examples

### Python (requests)

```python
import requests

# Get annual report
response = requests.get(
    "http://127.0.0.1:8000/annual-report",
    params={"bank_symbol": "ADBL", "fiscal_year": "2078/79"}
)
data = response.json()
pdf_url = data['pdf_url']
print(f"PDF URL: {pdf_url}")

# Get quarterly report
response = requests.get(
    "http://127.0.0.1:8000/quarterly-report",
    params={
        "bank_symbol": "CZBIL",
        "fiscal_year": "2080/81",
        "quarter": "Q2"
    }
)
data = response.json()
```

### JavaScript (fetch)

```javascript
// Get annual report
const response = await fetch(
  'http://127.0.0.1:8000/annual-report?bank_symbol=NABIL&fiscal_year=2079/80'
);
const data = await response.json();
console.log(data.pdf_url);

// Get quarterly report
const response2 = await fetch(
  'http://127.0.0.1:8000/quarterly-report?bank_symbol=HBL&fiscal_year=2080/81&quarter=Q1'
);
const data2 = await response2.json();
```

### cURL

```bash
# Get annual report
curl "http://127.0.0.1:8000/annual-report?bank_symbol=NICA&fiscal_year=2078/79"

# Get quarterly report
curl "http://127.0.0.1:8000/quarterly-report?bank_symbol=NBL&fiscal_year=2080/81&quarter=Q3"

# Diagnose bank
curl "http://127.0.0.1:8000/diagnose/ADBL"
```

## Security Notes

- **Internal Use Only**: This API is for company internal use
- **API Keys**: Never commit `.env` file to version control
- **Rate Limiting**: Respect Firecrawl API limits
- **Database Access**: Ensure Supabase RLS policies are properly configured

## Support & Contact

For issues or questions:
- Check the Swagger UI documentation: `http://127.0.0.1:8000/docs`
- Review the database schema in Supabase
- Contact the development team


