import pandas as pd
import aiohttp
import asyncio
import os
import time
from dotenv import load_dotenv
from tqdm import tqdm

# --- THE WAR ROOM CONFIGURATION ---
INPUT_CSV = "input_data.csv"   # The original Apollo file (with capital 'Email', 'First Name', 'Last Name')
OUTPUT_CSV = "output_data.csv" # The generated AI file (with lowercase 'email')

# The endpoint from the Instantly v2 API docs
API_URL = "https://api.instantly.ai/api/v2/leads"

# Instantly allows high throughput, 20 is safe and fast
CONCURRENCY_LIMIT = 20  

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv()
INSTANTLY_API_KEY = os.getenv("INSTANTLY_API_KEY")
INSTANTLY_CAMPAIGN_ID = os.getenv("INSTANTLY_CAMPAIGN_ID") 

if not INSTANTLY_API_KEY or not INSTANTLY_CAMPAIGN_ID:
    print("🚨 ERROR: Missing Instantly API Key or Campaign ID! Check your .env file.")
    exit(1)

# Make sure the API key starts with "Bearer "
if not INSTANTLY_API_KEY.startswith("Bearer "):
    INSTANTLY_API_KEY = f"Bearer {INSTANTLY_API_KEY}"

# Global trackers
stats = {"uploaded": 0, "failed": 0, "duplicates": 0}
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

async def upload_single_lead(session, unified_row, pbar):
    """
    Async worker to construct the exact payload and upload a lead.
    Maps data precisely to the Instantly v2 API requirements.
    """
    async with semaphore:
        company_name = str(unified_row.get('Company Name', 'Unknown')).strip()
        email = unified_row.get('email', '')

        # Skip if there's no email to send to
        if not email or pd.isna(email):
             pbar.update(1)
             stats["failed"] += 1
             return

        # --- NAME SCRUBBER ---
        # Explicitly grab Apollo's exact columns and clean them of any 'NaN' float values
        raw_first = unified_row.get('First Name', '')
        raw_last = unified_row.get('Last Name', '')
        
        clean_first = str(raw_first).strip() if pd.notna(raw_first) else ""
        clean_last = str(raw_last).strip() if pd.notna(raw_last) else ""

        # --- THE PROMISED LAND: PAYLOAD CONSTRUCTION ---
        payload = {
            "campaign": INSTANTLY_CAMPAIGN_ID,                  # Routes to the exact campaign flow
            "email": email,                                     
            
            # Mapped exactly to the Instantly API Docs
            "first_name": clean_first,   
            "last_name": clean_last,     
            
            "company_name": company_name,                       
            "title": str(unified_row.get('Title', '')) if pd.notna(unified_row.get('Title')) else "",              
            
            # --- NATIVE DUPLICATE PREVENTION ---
            "skip_if_in_workspace": True,
            "skip_if_in_campaign": True,
            
            # --- THE PERSONALIZATION ---
            "custom_variables": {
                "icebreaker": str(unified_row.get('icebreaker', '')).strip(),         
                "customCompanyName": str(unified_row.get('Companyname', '')).strip()
            }
        }
        
        # Optional: Safely pass website data if it exists
        website = unified_row.get('Website', '')
        if website and pd.notna(website):
            payload["website"] = str(website).strip()

        headers = {
            "Authorization": INSTANTLY_API_KEY,
            "Content-Type": "application/json"
        }

        try:
            async with session.post(API_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status in [200, 201]:
                    stats["uploaded"] += 1
                elif resp.status == 422: # Common validation/duplicate error
                     response_text = await resp.text()
                     if "duplicate" in response_text.lower():
                         stats["duplicates"] += 1
                     else:
                         stats["failed"] += 1
                else:
                    stats["failed"] += 1
                pbar.update(1) # Tick the progress bar forward
        except asyncio.TimeoutError:
            stats["failed"] += 1
            pbar.update(1)
        except Exception:
            stats["failed"] += 1
            pbar.update(1)

async def run_upload_pipeline():
    start_time = time.time()
    
    print(f"Loading data to merge and upload...")
    try:
        df_input = pd.read_csv(INPUT_CSV)
        df_output = pd.read_csv(OUTPUT_CSV)
    except FileNotFoundError:
        print("Whoops! Couldn't find one of the CSV files. Double check paths.")
        return

    # --- THE MERGE FIX ---
    # left_on maps to df_output ('email', lowercase)
    # right_on maps to df_input ('Email', uppercase)
    df_unified = pd.merge(df_output, df_input, left_on='email', right_on='Email', how='left')
    total_unified = len(df_unified)
    
    print(f"Merged successfully. Routing {total_unified} leads to Campaign: {INSTANTLY_CAMPAIGN_ID}")
    print("Initializing async uploader...\n")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create a visual progress bar
        with tqdm(total=total_unified, desc="Uploading Leads", unit="lead") as pbar:
             tasks = [
                 upload_single_lead(session, row, pbar)
                 for _, row in df_unified.iterrows()
             ]
             await asyncio.gather(*tasks)

    # Final Receipt
    elapsed = time.time() - start_time
    print(f"\n🏁 UPLOAD COMPLETE in {int(elapsed//60)}m {int(elapsed%60)}s")
    print(f"✅ Leads Uploaded: {stats['uploaded']}")
    print(f"👯 Duplicates Caught: {stats['duplicates']}")
    print(f"❌ Failures: {stats['failed']}")

if __name__ == "__main__":
    asyncio.run(run_upload_pipeline())