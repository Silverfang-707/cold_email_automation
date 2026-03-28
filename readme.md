# AI Lead Sniper & Instantly.ai Uploader

An asynchronous, high-performance Python pipeline that takes raw lead lists (from Apollo, etc.), scrapes their websites, generates highly personalized AI icebreakers using OpenRouter, and pushes the enriched leads directly into an Instantly.ai campaign.

---

## Features

- **Stateless Processing:** Saves data row-by-row in real-time. If your computer crashes halfway through a 10,000 lead list, you don't lose a single byte of data.
- **Smart Model Routing (Waterfall):** Uses a cheaper, faster primary model (e.g., `llama-4-scout`) for the first pass, and automatically falls back to a heavier model (e.g., `gemini-2.0-flash`) if the first model fails to generate valid JSON.
- **Resilient Web Scraping:** Uses `trafilatura` and `BeautifulSoup` to bypass basic blockers and extract clean, readable text from prospect websites.
- **Auto-Healing JSON:** Catches and repairs hallucinated formatting or trailing commas from LLM outputs.
- **Direct Instantly.ai Integration:** Merges original lead data with generated icebreakers and uploads them concurrently to the Instantly v2 API.
- **Native Duplicate Catching:** Automatically skips leads that already exist in your Instantly workspace or campaign to protect your sender reputation.

---

## Installation & Setup

### 1. Prerequisites

Ensure you have Python 3.8+ installed. Then, install the required dependencies:

```bash
pip install pandas beautifulsoup4 trafilatura aiohttp lxml python-dotenv tqdm
```

### 2. Environment Variables

Create a file named `.env` in the root directory of this project and add your API keys.

> **Note:** You can provide multiple OpenRouter keys separated by commas to round-robin requests and avoid rate limits.

```env
OPENROUTER_API_KEYS=sk-or-v1-key-one,sk-or-v1-key-two
INSTANTLY_API_KEY=Bearer YOUR_INSTANTLY_API_KEY_HERE
INSTANTLY_CAMPAIGN_ID=YOUR_CAMPAIGN_UUID_HERE
```

### 3. Data Preparation

Drop your raw lead list into the project folder and name it `input_data.csv`. Ensure your CSV has the following headers (standard Apollo export format):

| Column | Description |
|---|---|
| `First Name` | Contact's first name |
| `Last Name` | Contact's last name |
| `Company Name` | Company the contact belongs to |
| `Email` | Contact's email address |
| `Website` | Company website URL |

---

## How to Run

The pipeline is split into two distinct phases to ensure data safety and allow for manual review if desired.

### Phase 1 — Icebreaker Generator

Reads `input_data.csv`, scrapes the websites, and generates personalized icebreakers via AI.

```bash
python app.py
```

| Output File | Description |
|---|---|
| `output_data.csv` | Generated icebreakers and cleaned company names |
| `debug_data.csv` | Full audit log of every API call, scrape status, and failure reason |

### Phase 2 — Instantly Uploader

Merges `input_data.csv` with `output_data.csv` (matched on email address), cleans the names, and pushes leads live to your Instantly campaign.

```bash
python uploader.py
```

Watch the progress bar as leads are uploaded concurrently. Duplicates and invalid emails are automatically skipped and logged.

---

## Advanced Configuration

The following parameters can be tweaked inside `app.py` and `uploader.py`:

| Parameter | Default | Description |
|---|---|---|
| `CONCURRENCY_LIMIT` | `40` / `20` | Lower this if you are hitting OpenRouter or Instantly rate limits |
| `PRIMARY_MODEL` | `llama-4-scout` | Primary model used for icebreaker generation |
| `FALLBACK_MODEL` | `gemini-2.0-flash` | Fallback model if the primary fails to produce valid JSON |
| `SKIP_BOUNCED_LEADS` | `False` | Set to `True` to skip leads marked as "Bounced" in Apollo and save API credits |

---

## Troubleshooting

**`RuntimeError: Event loop is closed`**

This is a known Windows `asyncio` quirk when closing network pipes. The script includes a `sys.platform == 'win32'` fix at the bottom of each file to silence this automatically.