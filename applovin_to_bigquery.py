"""
AppLovin → BigQuery  ·  FULL DATA PIPELINE
===========================================
Pulls everything available from AppLovin:

MAX (Monetization):
  1. max_revenue_daily        — ad revenue per app per day
  2. max_revenue_by_country   — revenue broken down by country
  3. max_revenue_by_network   — revenue broken down by ad network (Google, Meta, Unity etc.)
  4. max_revenue_by_format    — revenue broken down by ad format (interstitial, rewarded, banner)
  5. max_user_revenue         — user-level ad revenue (per device ID)
  6. max_cohort_revenue       — cohort revenue D0-D30 per app

AppDiscovery (User Acquisition):
  7. ua_campaigns_daily       — spend, installs, CPI, ROAS per campaign per day
  8. ua_creatives_daily       — spend, installs per creative per day
  9. ua_by_country            — spend, installs broken down by country
  10. ua_by_app               — spend, installs broken down by target app
"""

import os
import csv
import json
import logging
import requests
import io
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ────────────────────────────────────────────────────────────────────
APPLOVIN_REPORT_KEY  = os.environ["APPLOVIN_REPORT_KEY"]   # Account → Keys → Report Key
GCP_PROJECT          = os.environ["GCP_PROJECT"]           # e.g. aspireai-b0834
BQ_DATASET           = os.environ.get("BQ_DATASET", "applovin_data")
GCP_CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS_JSON"]
LOOKBACK_DAYS        = int(os.environ.get("LOOKBACK_DAYS", "30"))

# ─── API BASE URLS ─────────────────────────────────────────────────────────────
MAX_REPORT_URL   = "https://r.applovin.com/maxReport"
MAX_USER_URL     = "https://r.applovin.com/max/userAdRevenueReport"
MAX_COHORT_URL   = "https://r.applovin.com/maxCohort"
UA_REPORT_URL    = "https://r.applovin.com/report"

# ─── BQ SCHEMAS ───────────────────────────────────────────────────────────────
SCHEMAS = {
    # ── 1. MAX Revenue Daily ──────────────────────────────────────────────────
    "max_revenue_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("app",                 "STRING"),   # app package name
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("platform",            "STRING"),   # ios / android
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("fill_rate",           "FLOAT"),
        bigquery.SchemaField("requests",            "INTEGER"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 2. MAX Revenue by Country ─────────────────────────────────────────────
    "max_revenue_by_country": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 3. MAX Revenue by Network ─────────────────────────────────────────────
    "max_revenue_by_network": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("network",             "STRING"),   # google, facebook, unity etc.
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 4. MAX Revenue by Ad Format ───────────────────────────────────────────
    "max_revenue_by_format": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("ad_format",           "STRING"),   # inter, rewarded, banner, mrec
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 5. MAX User-Level Revenue ─────────────────────────────────────────────
    "max_user_revenue": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("user_id",             "STRING"),   # device advertising ID
        bigquery.SchemaField("idfa",                "STRING"),
        bigquery.SchemaField("idfv",                "STRING"),
        bigquery.SchemaField("gaid",                "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("network",             "STRING"),
        bigquery.SchemaField("ad_format",           "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 6. MAX Cohort Revenue ─────────────────────────────────────────────────
    "max_cohort_revenue": [
        bigquery.SchemaField("cohort_date",         "DATE"),     # install date
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("d0_revenue",          "FLOAT"),
        bigquery.SchemaField("d1_revenue",          "FLOAT"),
        bigquery.SchemaField("d2_revenue",          "FLOAT"),
        bigquery.SchemaField("d3_revenue",          "FLOAT"),
        bigquery.SchemaField("d4_revenue",          "FLOAT"),
        bigquery.SchemaField("d5_revenue",          "FLOAT"),
        bigquery.SchemaField("d6_revenue",          "FLOAT"),
        bigquery.SchemaField("d7_revenue",          "FLOAT"),
        bigquery.SchemaField("d10_revenue",         "FLOAT"),
        bigquery.SchemaField("d14_revenue",         "FLOAT"),
        bigquery.SchemaField("d21_revenue",         "FLOAT"),
        bigquery.SchemaField("d30_revenue",         "FLOAT"),
        bigquery.SchemaField("d45_revenue",         "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 7. UA Campaigns Daily ─────────────────────────────────────────────────
    "ua_campaigns_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign_id",         "STRING"),
        bigquery.SchemaField("campaign_name",       "STRING"),
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("installs",            "INTEGER"),
        bigquery.SchemaField("spend",               "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("ctr",                 "FLOAT"),
        bigquery.SchemaField("ir",                  "FLOAT"),    # install rate
        bigquery.SchemaField("cpi",                 "FLOAT"),    # cost per install
        bigquery.SchemaField("revenue",             "FLOAT"),    # attributed revenue
        bigquery.SchemaField("roas",                "FLOAT"),    # revenue / spend
        bigquery.SchemaField("d0_roas",             "FLOAT"),
        bigquery.SchemaField("d3_roas",             "FLOAT"),
        bigquery.SchemaField("d7_roas",             "FLOAT"),
        bigquery.SchemaField("d14_roas",            "FLOAT"),
        bigquery.SchemaField("d30_roas",            "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 8. UA Creatives Daily ─────────────────────────────────────────────────
    "ua_creatives_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign_id",         "STRING"),
        bigquery.SchemaField("campaign_name",       "STRING"),
        bigquery.SchemaField("creative_id",         "STRING"),
        bigquery.SchemaField("creative_name",       "STRING"),
        bigquery.SchemaField("creative_type",       "STRING"),   # video, image, playable
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("installs",            "INTEGER"),
        bigquery.SchemaField("spend",               "FLOAT"),
        bigquery.SchemaField("ctr",                 "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 9. UA by Country ──────────────────────────────────────────────────────
    "ua_by_country": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign_id",         "STRING"),
        bigquery.SchemaField("campaign_name",       "STRING"),
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("installs",            "INTEGER"),
        bigquery.SchemaField("spend",               "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],

    # ── 10. UA by App ─────────────────────────────────────────────────────────
    "ua_by_app": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("app",                 "STRING"),
        bigquery.SchemaField("app_id",              "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("installs",            "INTEGER"),
        bigquery.SchemaField("spend",               "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("roas",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def safe_float(val):
    try: return float(val) if val not in (None, "", "N/A") else None
    except: return None

def safe_int(val):
    try: return int(float(val)) if val not in (None, "", "N/A") else None
    except: return None

def now_ts():
    return datetime.utcnow().isoformat()

def date_range():
    end   = datetime.utcnow().date()
    start = end - timedelta(days=LOOKBACK_DAYS)
    return str(start), str(end)

def fetch_csv(url, params):
    """Fetch a CSV report from AppLovin API and return list of dicts."""
    params["api_key"] = APPLOVIN_REPORT_KEY
    params["format"]  = "csv"
    log.info(f"  GET {url} columns={params.get('columns','')}")
    resp = requests.get(url, params=params, timeout=120)
    if resp.status_code != 200:
        log.warning(f"  API error {resp.status_code}: {resp.text[:200]}")
        return []
    text = resp.text.strip()
    if not text or text.startswith("error"):
        log.warning(f"  Empty or error response: {text[:200]}")
        return []
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)

# ─── BQ HELPERS ───────────────────────────────────────────────────────────────
def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=GCP_PROJECT, credentials=creds)

def ensure_dataset(client):
    try: client.get_dataset(BQ_DATASET)
    except Exception:
        log.info(f"Creating dataset {BQ_DATASET}")
        client.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}"))

def ensure_table(client, name):
    ref = client.dataset(BQ_DATASET).table(name)
    try: client.get_table(ref)
    except Exception:
        log.info(f"Creating table {name}")
        client.create_table(bigquery.Table(ref, schema=SCHEMAS[name]))

def load_to_bq(client, name, rows):
    if not rows: log.info(f"  No rows for {name}"); return
    errors = client.insert_rows_json(f"{GCP_PROJECT}.{BQ_DATASET}.{name}", rows)
    if errors: log.error(f"BQ errors [{name}]: {errors[:3]}")
    else: log.info(f"  ✅ {len(rows):,} rows → {name}")

# ─── 1. MAX REVENUE DAILY ─────────────────────────────────────────────────────
def fetch_max_revenue_daily():
    log.info("Fetching MAX Revenue Daily...")
    start, end = date_range()
    rows_raw = fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,app_id,platform,impressions,estimated_revenue,ecpm,fill_rate,requests",
        "breakdown": "day,application,platform",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "date":               r.get("day"),
            "app":                r.get("application"),
            "app_id":             r.get("app_id"),
            "platform":           r.get("platform"),
            "impressions":        safe_int(r.get("impressions")),
            "estimated_revenue":  safe_float(r.get("estimated_revenue")),
            "ecpm":               safe_float(r.get("ecpm")),
            "fill_rate":          safe_float(r.get("fill_rate")),
            "requests":           safe_int(r.get("requests")),
            "_ingested_at":       now_ts(),
        })
    return rows

# ─── 2. MAX REVENUE BY COUNTRY ────────────────────────────────────────────────
def fetch_max_revenue_by_country():
    log.info("Fetching MAX Revenue by Country...")
    start, end = date_range()
    rows_raw = fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,app_id,platform,country,impressions,estimated_revenue,ecpm",
        "breakdown": "day,application,platform,country",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "date":              r.get("day"),
            "app":               r.get("application"),
            "app_id":            r.get("app_id"),
            "platform":          r.get("platform"),
            "country":           r.get("country"),
            "impressions":       safe_int(r.get("impressions")),
            "estimated_revenue": safe_float(r.get("estimated_revenue")),
            "ecpm":              safe_float(r.get("ecpm")),
            "_ingested_at":      now_ts(),
        })
    return rows

# ─── 3. MAX REVENUE BY NETWORK ────────────────────────────────────────────────
def fetch_max_revenue_by_network():
    log.info("Fetching MAX Revenue by Network...")
    start, end = date_range()
    rows_raw = fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,app_id,platform,network,impressions,estimated_revenue,ecpm",
        "breakdown": "day,application,platform,network",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "date":              r.get("day"),
            "app":               r.get("application"),
            "app_id":            r.get("app_id"),
            "platform":          r.get("platform"),
            "network":           r.get("network"),
            "impressions":       safe_int(r.get("impressions")),
            "estimated_revenue": safe_float(r.get("estimated_revenue")),
            "ecpm":              safe_float(r.get("ecpm")),
            "_ingested_at":      now_ts(),
        })
    return rows

# ─── 4. MAX REVENUE BY AD FORMAT ─────────────────────────────────────────────
def fetch_max_revenue_by_format():
    log.info("Fetching MAX Revenue by Ad Format...")
    start, end = date_range()
    rows_raw = fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,app_id,platform,ad_format,impressions,estimated_revenue,ecpm",
        "breakdown": "day,application,platform,ad_format",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "date":              r.get("day"),
            "app":               r.get("application"),
            "app_id":            r.get("app_id"),
            "platform":          r.get("platform"),
            "ad_format":         r.get("ad_format"),
            "impressions":       safe_int(r.get("impressions")),
            "estimated_revenue": safe_float(r.get("estimated_revenue")),
            "ecpm":              safe_float(r.get("ecpm")),
            "_ingested_at":      now_ts(),
        })
    return rows

# ─── 5. MAX USER-LEVEL REVENUE ────────────────────────────────────────────────
def fetch_max_user_revenue():
    log.info("Fetching MAX User-Level Revenue...")
    # User revenue is available per day, pull yesterday only (large data)
    yesterday = str(datetime.utcnow().date() - timedelta(days=1))
    rows_raw = fetch_csv(MAX_USER_URL, {
        "date": yesterday,
        "columns": "application,app_id,user_id,idfa,idfv,google_advertising_id,platform,country,network,ad_format,impressions,estimated_revenue",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "date":              yesterday,
            "app":               r.get("application"),
            "app_id":            r.get("app_id"),
            "user_id":           r.get("user_id"),
            "idfa":              r.get("idfa"),
            "idfv":              r.get("idfv"),
            "gaid":              r.get("google_advertising_id"),
            "platform":          r.get("platform"),
            "country":           r.get("country"),
            "network":           r.get("network"),
            "ad_format":         r.get("ad_format"),
            "impressions":       safe_int(r.get("impressions")),
            "estimated_revenue": safe_float(r.get("estimated_revenue")),
            "_ingested_at":      now_ts(),
        })
    return rows

# ─── 6. MAX COHORT REVENUE ────────────────────────────────────────────────────
def fetch_max_cohort_revenue():
    log.info("Fetching MAX Cohort Revenue...")
    # Cohort API max 45 days window
    end   = datetime.utcnow().date()
    start = end - timedelta(days=44)
    rows_raw = fetch_csv(MAX_COHORT_URL, {
        "start":   str(start),
        "end":     str(end),
        "columns": "cohort_date,application,app_id,platform,country,d0,d1,d2,d3,d4,d5,d6,d7,d10,d14,d21,d30,d45",
        "breakdown": "cohort_date,application,platform,country",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "cohort_date":   r.get("cohort_date"),
            "app":           r.get("application"),
            "app_id":        r.get("app_id"),
            "platform":      r.get("platform"),
            "country":       r.get("country"),
            "d0_revenue":    safe_float(r.get("d0")),
            "d1_revenue":    safe_float(r.get("d1")),
            "d2_revenue":    safe_float(r.get("d2")),
            "d3_revenue":    safe_float(r.get("d3")),
            "d4_revenue":    safe_float(r.get("d4")),
            "d5_revenue":    safe_float(r.get("d5")),
            "d6_revenue":    safe_float(r.get("d6")),
            "d7_revenue":    safe_float(r.get("d7")),
            "d10_revenue":   safe_float(r.get("d10")),
            "d14_revenue":   safe_float(r.get("d14")),
            "d21_revenue":   safe_float(r.get("d21")),
            "d30_revenue":   safe_float(r.get("d30")),
            "d45_revenue":   safe_float(r.get("d45")),
            "_ingested_at":  now_ts(),
        })
    return rows

# ─── 7. UA CAMPAIGNS DAILY ────────────────────────────────────────────────────
def fetch_ua_campaigns_daily():
    log.info("Fetching UA Campaigns Daily...")
    start, end = date_range()
    rows_raw = fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,campaign_id,campaign,package_name,app_id,platform,country,impressions,clicks,installs,spend,ecpm,ctr,ir,cpi,revenue,d0_roas,d3_roas,d7_roas,d14_roas,d30_roas",
        "breakdown": "day,campaign,country",
        "report_type": "advertiser",
    })
    rows = []
    for r in rows_raw:
        spend   = safe_float(r.get("spend")) or 0.0
        revenue = safe_float(r.get("revenue")) or 0.0
        rows.append({
            "date":          r.get("day"),
            "campaign_id":   r.get("campaign_id"),
            "campaign_name": r.get("campaign"),
            "app":           r.get("package_name"),
            "app_id":        r.get("app_id"),
            "platform":      r.get("platform"),
            "country":       r.get("country"),
            "impressions":   safe_int(r.get("impressions")),
            "clicks":        safe_int(r.get("clicks")),
            "installs":      safe_int(r.get("installs")),
            "spend":         spend,
            "ecpm":          safe_float(r.get("ecpm")),
            "ctr":           safe_float(r.get("ctr")),
            "ir":            safe_float(r.get("ir")),
            "cpi":           safe_float(r.get("cpi")),
            "revenue":       revenue,
            "roas":          round(revenue / spend, 4) if spend else None,
            "d0_roas":       safe_float(r.get("d0_roas")),
            "d3_roas":       safe_float(r.get("d3_roas")),
            "d7_roas":       safe_float(r.get("d7_roas")),
            "d14_roas":      safe_float(r.get("d14_roas")),
            "d30_roas":      safe_float(r.get("d30_roas")),
            "_ingested_at":  now_ts(),
        })
    return rows

# ─── 8. UA CREATIVES DAILY ────────────────────────────────────────────────────
def fetch_ua_creatives_daily():
    log.info("Fetching UA Creatives Daily...")
    start, end = date_range()
    rows_raw = fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,campaign_id,campaign,creative_id,creative,creative_type,package_name,app_id,impressions,clicks,installs,spend,ctr,cpi",
        "breakdown": "day,campaign,creative",
        "report_type": "advertiser",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "date":           r.get("day"),
            "campaign_id":    r.get("campaign_id"),
            "campaign_name":  r.get("campaign"),
            "creative_id":    r.get("creative_id"),
            "creative_name":  r.get("creative"),
            "creative_type":  r.get("creative_type"),
            "app":            r.get("package_name"),
            "app_id":         r.get("app_id"),
            "impressions":    safe_int(r.get("impressions")),
            "clicks":         safe_int(r.get("clicks")),
            "installs":       safe_int(r.get("installs")),
            "spend":          safe_float(r.get("spend")),
            "ctr":            safe_float(r.get("ctr")),
            "cpi":            safe_float(r.get("cpi")),
            "_ingested_at":   now_ts(),
        })
    return rows

# ─── 9. UA BY COUNTRY ─────────────────────────────────────────────────────────
def fetch_ua_by_country():
    log.info("Fetching UA by Country...")
    start, end = date_range()
    rows_raw = fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,campaign_id,campaign,package_name,app_id,country,impressions,clicks,installs,spend,cpi",
        "breakdown": "day,campaign,country",
        "report_type": "advertiser",
    })
    rows = []
    for r in rows_raw:
        rows.append({
            "date":          r.get("day"),
            "campaign_id":   r.get("campaign_id"),
            "campaign_name": r.get("campaign"),
            "app":           r.get("package_name"),
            "app_id":        r.get("app_id"),
            "country":       r.get("country"),
            "impressions":   safe_int(r.get("impressions")),
            "clicks":        safe_int(r.get("clicks")),
            "installs":      safe_int(r.get("installs")),
            "spend":         safe_float(r.get("spend")),
            "cpi":           safe_float(r.get("cpi")),
            "_ingested_at":  now_ts(),
        })
    return rows

# ─── 10. UA BY APP ────────────────────────────────────────────────────────────
def fetch_ua_by_app():
    log.info("Fetching UA by App...")
    start, end = date_range()
    rows_raw = fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,package_name,app_id,platform,impressions,clicks,installs,spend,cpi,revenue",
        "breakdown": "day,application,platform",
        "report_type": "advertiser",
    })
    rows = []
    for r in rows_raw:
        spend   = safe_float(r.get("spend")) or 0.0
        revenue = safe_float(r.get("revenue")) or 0.0
        rows.append({
            "date":         r.get("day"),
            "app":          r.get("package_name"),
            "app_id":       r.get("app_id"),
            "platform":     r.get("platform"),
            "impressions":  safe_int(r.get("impressions")),
            "clicks":       safe_int(r.get("clicks")),
            "installs":     safe_int(r.get("installs")),
            "spend":        spend,
            "cpi":          safe_float(r.get("cpi")),
            "revenue":      revenue,
            "roas":         round(revenue / spend, 4) if spend else None,
            "_ingested_at": now_ts(),
        })
    return rows

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 AppLovin → BigQuery FULL sync")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")

    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    # ── MAX Monetization ──────────────────────────────────────────────────────
    log.info("── MAX (Monetization) ──")
    load_to_bq(bq, "max_revenue_daily",       fetch_max_revenue_daily())
    load_to_bq(bq, "max_revenue_by_country",  fetch_max_revenue_by_country())
    load_to_bq(bq, "max_revenue_by_network",  fetch_max_revenue_by_network())
    load_to_bq(bq, "max_revenue_by_format",   fetch_max_revenue_by_format())
    load_to_bq(bq, "max_user_revenue",        fetch_max_user_revenue())
    load_to_bq(bq, "max_cohort_revenue",      fetch_max_cohort_revenue())

    # ── AppDiscovery User Acquisition ─────────────────────────────────────────
    log.info("── AppDiscovery (User Acquisition) ──")
    load_to_bq(bq, "ua_campaigns_daily",      fetch_ua_campaigns_daily())
    load_to_bq(bq, "ua_creatives_daily",      fetch_ua_creatives_daily())
    load_to_bq(bq, "ua_by_country",           fetch_ua_by_country())
    load_to_bq(bq, "ua_by_app",               fetch_ua_by_app())

    log.info("✅ Full AppLovin sync complete!")

if __name__ == "__main__":
    main()
