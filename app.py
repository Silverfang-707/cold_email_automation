import pandas as pd
from bs4 import BeautifulSoup
import trafilatura
import json
import re
import csv
import os
from datetime import datetime, timezone
import asyncio
import aiohttp
import itertools
import time
import sys
from dotenv import load_dotenv

# --- THE WAR ROOM CONFIGURATION ---
INPUT_CSV = "input_data.csv"
OUTPUT_CSV = "output_data.csv"
DEBUG_CSV = "debug_data.csv"

load_dotenv()
# Deal your API keys like cards
API_KEYS = os.getenv("OPENROUTER_API_KEYS", "").split(",")
CONCURRENCY_LIMIT = 40  
key_pool = itertools.cycle(API_KEYS)

# --- MODEL ROUTING ---
PRIMARY_MODEL = "meta-llama/llama-4-scout"
ENABLE_SMART_ROUTING = False
FALLBACK_MODEL = "google/gemini-2.0-flash-001" 

# --- FILTERS & RETRIES ---
SKIP_BOUNCED_LEADS = True   
ENABLE_FAILED_RETRY = True
# We set this to 2: One retry for the Main Model, one for the Fallback
MAX_RETRY_PASSES = 2 

# --- SYSTEM PROMPT ---
SYSTEM_PROMPT = """You are an elite outbound personalization engine.

Your job is to generate highly personalized, sharp, and natural-sounding icebreakers for cold outreach messages.
The icebreaker will be read by the individual whose details are provided below, so ensure the message is clearly tailored to them or the company.

You MUST return ONLY a strictly formatted JSON object. Do not include any conversational preamble, markdown blocks, or extra text. You must use this exact JSON schema:
{
  "normalName": "A conversational, cleaned-up version of their company name.",
  "icebreaker": "A highly personalized, natural-sounding, single-sentence opening line for a cold email."
}

You will be given structured data about:
- The person (name, role, title)
- The company (description, industry, size)
- Signals (revenue, funding, tech, etc.)
First Name	
Last Name	
Title	
Company Name	
Company Name for Emails	
Email	
Seniority	
Departments	
Sub Departments	
# Employees	
Industry	
Keywords	
Person Linkedin Url	
Website	
Company Linkedin Url	
Facebook Url	
Twitter Url	
City	
Technologies	
Annual Revenue	
Total Funding
Latest Funding	
Latest Funding Amount
Last Raised At

Your output MUST:
- Feel written by a real human (not AI)
- Be specific to the input (no generic phrases)
- Be 1 or 2 lines maximum
- Be concise, confident, and slightly insightful
- Create curiosity or subtle tension
- Never use — in the ice breaker

---

### CORE OBJECTIVE

Write an opening line that makes the recipient feel:
> “This person actually understands what I'm doing.”

---

### WRITING RULES (STRICT)

1. NEVER say:
- “I came across your profile”
- “I saw your company”
- “We help businesses like yours”
- Anything generic
- Never use — in the ice breaker

2. ALWAYS:
- Reference a SPECIFIC detail from the input
- Convert that detail into:
  → a problem
  → an insight
  → or an opportunity

3. Use this structure:

[Observation about company/person]  
→ [Insight, implication, or subtle problem]

---

### STYLE

- Natural, conversational, slightly informal
- No buzzwords
- No long sentences
- No emojis
- No fluff
- No —

---

### HIGH-CONVERTING PATTERNS

Use one of these patterns(Select one of the best from this that suits the client profiles):

1. Opportunity angle
Positive framing
“Feels like there's a big opportunity to improve [area].”
Example:
“Feels like there's a big opportunity to streamline your operations with automation.”

2. Website based
“Came across your site, really liked [specific thing].”
Example:
“Came across your website, really liked your positioning around premium care, feels like the booking flow could be smoother.”

3. Role-based  
"As a [role], you're probably dealing with [specific pain]"

4. Insight-based 
“In [industry], I've noticed most teams struggle with [specific issue].”
Example:
“In real estate, I've noticed most teams lose deals just because follow-ups aren't fast enough.”

5. Curiosity  
"Curious how you're currently handling [specific process]"

6. Opportunity  
"Feels like there's a big opportunity to improve [area]"

7. Problem identification
Direct pain
“Looks like [problem] might be slowing things down.”
Example:
“Looks like manual workflows might be slowing your scaling a bit.”


---

### INPUT FORMAT

You will receive:

Name:
Role:
Company:
Company Description:
Industry:
Signals:

---

### OUTPUT FORMAT

Return ONLY the icebreaker line.
No explanations.
No extra text.

---

### EXAMPLES

Input:
Name: John
Role: Founder
Company: XYZ Realty
Company Description: Real estate firm helping clients buy and sell luxury homes
Industry: Real Estate
Signals: Hiring sales agents
Output:
"Saw you're expanding your sales team, that's usually when lead follow-ups start slipping."

---

Input:
Name: Sarah
Role: Director
Company: CarePlus Clinic
Company Description: Multi-location healthcare clinic focused on patient care
Industry: Healthcare
Signals: Expanding locations
Output:
"Noticed you're expanding into multiple locations, that usually makes patient coordination and follow-ups messy."

---

Input:
Name: Ahmed
Role: CEO
Company: TradeCore
Company Description: Forex trading platform connecting physical assets with digital markets
Industry: Finance
Signals: Platform launch soon
Output:
"Launching a trading platform around physical assets is interesting, trust and execution usually become the hardest parts to get right."

---

### FINAL INSTRUCTION

Generate a highly specific, sharp, and human-like icebreaker using the input provided.

Avoid generic statements at all costs.

Make it feel like it was written only for THIS person."""

# Global lock and stats
file_lock = asyncio.Lock()
stats = {"success": 0, "skipped": 0}

# --- CORE UTILITIES ---

def salvage_json(raw_text, fallback_company):
    """Rescues data from broken JSON formats."""
    try:
        data = json.loads(raw_text)
        return str(data.get('normalName', fallback_company)).strip(), str(data.get('icebreaker', '')).strip(), "Success - Clean JSON"
    except:
        pass

    name_match = re.search(r'"normalName"\s*:\s*"([^"]*)"', raw_text, re.IGNORECASE)
    ice_match = re.search(r'"icebreaker"\s*:\s*"(.*)', raw_text, re.IGNORECASE | re.DOTALL)

    if ice_match:
        salvaged_name = name_match.group(1).strip() if name_match else fallback_company
        salvaged_ice = ice_match.group(1).strip()
        while salvaged_ice.endswith(('"', '}', ' ', '\n', '\r')): salvaged_ice = salvaged_ice[:-1]
        if salvaged_ice: return salvaged_name, salvaged_ice.strip(), "Success - Regex Salvaged"

    return "", "", "Failed - Irrecoverable Junk"

async def write_to_csv(file_path, data):
    """Appends a single row to CSV immediately (Stateless Saving)."""
    async with file_lock:
        file_exists = os.path.isfile(file_path)
        headers = data.keys()
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

def build_smart_prompt(row, extracted_text_array, website_title):
    """Constructs the prompt, handling cases where website data is missing."""
    raw_data = {
        "--- THE PROSPECT ---": "",
        "Name": f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip(),
        "Title": row.get('Title', ''),
        "LinkedIn": row.get('Person Linkedin Url', ''),
        
        "\n--- THE COMPANY ---": "",
        "Company Name": row.get('Company Name', ''),
        "Industry": row.get('Industry', ''),
        "Company Size": row.get('# Employees', ''),
        "Keywords": row.get('Keywords', ''),
        "Location": f"{row.get('City', '')}, {row.get('State', '')}, {row.get('Country', '')}".strip(', '),
        
        "\n--- FINANCIALS & TECH ---": "",
        "Technologies": row.get('Technologies', ''),
        "Annual Revenue": row.get('Annual Revenue', ''),
        "Total Funding": row.get('Total Funding', ''),
        "Latest Funding": row.get('Latest Funding Amount', '')
    }

    clean_data_lines = []
    for key, value in raw_data.items():
        is_empty = pd.isna(value) or str(value).strip() == "" or str(value).lower() in ['nan', 'n/a', 'none']
        if "---" in key or not is_empty:
            clean_data_lines.append(key if "---" in key else f"{key}: {value}")

    if extracted_text_array:
        scraped_section = f"\n--- SCRAPED WEBSITE CONTEXT ---\nWebsite Title: {website_title}\nContent:\n{json.dumps(extracted_text_array, indent=2)}"
    else:
        # User Instruction: If scrape fails, use other data instead of failing the row.
        scraped_section = "\n--- SCRAPED WEBSITE CONTEXT ---\n[Web data unavailable. Pivot strictly to Funding, Location, or Tech info above.]"

    return "\n".join(clean_data_lines) + scraped_section

async def fetch_website(session, url, company):
    if not url or pd.isna(url) or str(url).strip() == "":
        return "N/A", [], "No URL"
    
    clean_url = str(url).strip()
    if not clean_url.startswith('http'): clean_url = 'https://' + clean_url

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
    try:
        async with session.get(clean_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                title = soup.title.string.strip() if soup.title else "N/A"
                text = trafilatura.extract(html, include_links=False, include_formatting=False)
                if text:
                    array = [l.strip() for l in text.split('\n') if len(l.strip().split()) > 3]
                    return title, array[:20], "Success"
                return title, [], "No Content"
            return "N/A", [], f"HTTP {response.status}"
    except:
        return "N/A", [], "Connection Failed"

async def ask_openrouter_async(session, system_prompt, user_prompt, model):
    api_key = next(key_pool)
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "response_format": {"type": "json_object"},
        "max_tokens": 150
    }

    try:
        async with session.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=20) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw_text = data['choices'][0]['message']['content'].strip()
                raw_text = re.sub(r'^```json\s*|```$', '', raw_text, flags=re.MULTILINE).strip()
                return raw_text, "Success"
            return None, f"API {resp.status}"
    except Exception as e:
        return None, str(e)

# --- WORKER ---

async def process_row(index, row, session, semaphore, total_rows, pass_num=0):
    async with semaphore:
        company = str(row.get('Company Name', 'Unknown'))
        email_status = str(row.get('Email Status', '')).lower()
        email = row.get('Email', '')

        # Filter out Apollo junk
        if SKIP_BOUNCED_LEADS and (pd.isna(email) or email_status == "bounced"):
            stats["skipped"] += 1
            return None

        # Determine Model: 
        # Pass 0 & 1 = Main Model. Pass 2 = Fallback.
        if pass_num <= 1:
            model_to_use = PRIMARY_MODEL
        else:
            model_to_use = FALLBACK_MODEL if ENABLE_SMART_ROUTING else PRIMARY_MODEL

        print(f"[{index + 1}/{total_rows}] Hitting: {company} (Pass {pass_num} using {model_to_use.split('/')[-1]})")

        # Scrape & Build
        title, content, s_status = await fetch_website(session, row.get('Website'), company)
        u_prompt = build_smart_prompt(row, content, title)

        # AI Call
        raw_ai, ai_status = await ask_openrouter_async(session, SYSTEM_PROMPT, u_prompt, model_to_use)
        
        # Parse & Validate
        name, ice, p_status = salvage_json(raw_ai, company) if raw_ai else ("", "", "No Response")
        is_success = name != "" and ice != "" and "error" not in ice.lower()

        # Instant Log
        debug_log = {
            'Row': index + 1, 'Pass': pass_num, 'Company': company, 
            'Model': model_to_use, 'Scrape': s_status, 'Status': p_status, 'Result': ice
        }
        await write_to_csv(DEBUG_CSV, debug_log)

        if is_success:
            stats["success"] += 1
            out_row = {
                'icebreaker': ice, 'Companyname': name, 'email': email,
                'Firstname': row.get('First Name', ''), 'lastname': row.get('Last Name', ''),
                'createdAt': datetime.now(timezone.utc).isoformat()
            }
            await write_to_csv(OUTPUT_CSV, out_row)
            return None
        else:
            return (index, row)

async def run_pipeline():
    start_time = time.time()
    df = pd.read_csv(INPUT_CSV)
    total_rows = len(df)
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # PASS 0: Main Run
        tasks = [process_row(i, r, session, semaphore, total_rows) for i, r in df.iterrows()]
        failed_items = [res for res in await asyncio.gather(*tasks) if res is not None]

        # PASS 1: Retry with MAIN model
        if ENABLE_FAILED_RETRY and failed_items:
            print(f"\n--- INITIATING MAIN MODEL RETRY ({len(failed_items)} items) ---")
            tasks = [process_row(idx, row, session, semaphore, total_rows, pass_num=1) for idx, row in failed_items]
            failed_items = [res for res in await asyncio.gather(*tasks) if res is not None]

        # PASS 2: Retry with FALLBACK model
        if ENABLE_FAILED_RETRY and ENABLE_SMART_ROUTING and failed_items:
            print(f"\n--- INITIATING FALLBACK MODEL ROUTING ({len(failed_items)} items) ---")
            tasks = [process_row(idx, row, session, semaphore, total_rows, pass_num=2) for idx, row in failed_items]
            failed_items = [res for res in await asyncio.gather(*tasks) if res is not None]

    elapsed = time.time() - start_time
    print(f"\n🏁 SNIPE COMPLETE in {int(elapsed//60)}m {int(elapsed%60)}s")
    print(f"✅ Success: {stats['success']} | ⏭️ Skipped: {stats['skipped']} | ❌ Permanently Failed: {len(failed_items)}")

if __name__ == "__main__":
    # --- WINDOWS EVENT LOOP FIX ---
    # This silences the "ProactorBasePipeTransport" and "Event loop is closed" errors
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(run_pipeline())