import os
import requests
from dotenv import load_dotenv

load_dotenv()

MATOMO_API_TOKEN = os.getenv('MATOMO_API_TOKEN')
MATOMO_URL = os.getenv('MATOMO_URL')
SITE_ID = int(os.getenv('SITE_ID'))

params = {
    "module": "API",
    "method": "VisitsSummary.getUniqueVisitors",  # Unique visitors metric
    "idSite": SITE_ID,
    "period": "day",          # Daily breakdown
    "date": "last10",         # Last 10 days
    "format": "json",          
    "token_auth": MATOMO_API_TOKEN,
}

response = requests.post(f"{MATOMO_URL}/index.php", data=params)

if response.status_code == 200:
    print("Unique Visitors (Last 10 Days):")
    print(response.text)
else:
    print("Error:", response.status_code, response.text)
