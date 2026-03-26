"""
AppLovin → BigQuery  ·  COMPLETE PIPELINE (EVERYTHING)
=======================================================
Every available endpoint and column from AppLovin APIs:

MAX (Monetization):
  1.  max_revenue_daily          — daily revenue per app
  2.  max_revenue_by_country     — revenue by country
  3.  max_revenue_by_network     — revenue by ad network
  4.  max_revenue_by_format      — revenue by ad format
  5.  max_revenue_by_device      — revenue by device type
  6.  max_revenue_by_hour        — hourly revenue (last 30 days)
  7.  max_user_revenue           — user-level revenue (android)
  8.  max_cohort_revenue         — cohort revenue D0-D45
  9.  max_cohort_impressions     — cohort impressions D0-D45
  10. max_cohort_sessions        — cohort sessions/retention D0-D45

AppDiscovery (User Acquisition):
  11. ua_campaigns_daily         — campaign spend/installs/ROAS daily
  12. ua_by_country              — UA by country
  13. ua_by_platform             — UA by platform
  14. ua_by_creative_type        — UA by ad creative type
  15. ua_by_app                  — UA by promoted app
  16. ua_creatives_daily         — asset/creative performance

Publisher (Ad Revenue from showing ads):
  17. pub_revenue_daily          — publisher revenue daily
  18. pub_revenue_by_country     — publisher revenue by country
  19. pub_revenue_by_adtype      — publisher revenue by ad type
  20. pub_revenue_by_platform    — publisher revenue by platform
"""

import os, csv, json, logging, requests, io
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

APPLOVIN_REPORT_KEY  = os.environ["APPLOVIN_REPORT_KEY"]
GCP_PROJECT          = os.environ["GCP_PROJECT"]
BQ_DATASET           = os.environ.get("BQ_DATASET", "applovin_data")
GCP_CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS_JSON"]
LOOKBACK_DAYS        = int(os.environ.get("LOOKBACK_DAYS", "30"))

MAX_REPORT_URL  = "https://r.applovin.com/maxReport"
MAX_USER_URL    = "https://r.applovin.com/max/userAdRevenueReport"
MAX_COHORT_URL  = "https://r.applovin.com/maxCohort"
UA_REPORT_URL   = "https://r.applovin.com/report"
UA_ASSET_URL    = "https://r.applovin.com/assetReport"

# ─── BQ SCHEMAS ───────────────────────────────────────────────────────────────
SCHEMAS = {
    "max_revenue_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("requests",            "INTEGER"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_revenue_by_country": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_revenue_by_network": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("network",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_revenue_by_format": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("ad_format",           "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_revenue_by_device": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("device_type",         "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_revenue_by_hour": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("hour",                "STRING"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_user_revenue": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("network",             "STRING"),
        bigquery.SchemaField("ad_format",           "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("estimated_revenue",   "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_cohort_revenue": [
        bigquery.SchemaField("day",                 "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("installs",            "INTEGER"),
        bigquery.SchemaField("pub_revenue_0",       "FLOAT"),
        bigquery.SchemaField("pub_revenue_1",       "FLOAT"),
        bigquery.SchemaField("pub_revenue_3",       "FLOAT"),
        bigquery.SchemaField("pub_revenue_7",       "FLOAT"),
        bigquery.SchemaField("pub_revenue_14",      "FLOAT"),
        bigquery.SchemaField("pub_revenue_30",      "FLOAT"),
        bigquery.SchemaField("pub_revenue_45",      "FLOAT"),
        bigquery.SchemaField("rpi_0",               "FLOAT"),
        bigquery.SchemaField("rpi_1",               "FLOAT"),
        bigquery.SchemaField("rpi_3",               "FLOAT"),
        bigquery.SchemaField("rpi_7",               "FLOAT"),
        bigquery.SchemaField("rpi_14",              "FLOAT"),
        bigquery.SchemaField("rpi_30",              "FLOAT"),
        bigquery.SchemaField("rpi_45",              "FLOAT"),
        bigquery.SchemaField("inter_pub_revenue_7", "FLOAT"),
        bigquery.SchemaField("reward_pub_revenue_7","FLOAT"),
        bigquery.SchemaField("banner_pub_revenue_7","FLOAT"),
        bigquery.SchemaField("mrec_pub_revenue_7",  "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_cohort_impressions": [
        bigquery.SchemaField("day",                 "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("installs",            "INTEGER"),
        bigquery.SchemaField("imp_0",               "INTEGER"),
        bigquery.SchemaField("imp_1",               "INTEGER"),
        bigquery.SchemaField("imp_3",               "INTEGER"),
        bigquery.SchemaField("imp_7",               "INTEGER"),
        bigquery.SchemaField("imp_14",              "INTEGER"),
        bigquery.SchemaField("imp_30",              "INTEGER"),
        bigquery.SchemaField("imp_45",              "INTEGER"),
        bigquery.SchemaField("imp_per_user_0",      "FLOAT"),
        bigquery.SchemaField("imp_per_user_1",      "FLOAT"),
        bigquery.SchemaField("imp_per_user_7",      "FLOAT"),
        bigquery.SchemaField("imp_per_user_30",     "FLOAT"),
        bigquery.SchemaField("inter_imp_7",         "INTEGER"),
        bigquery.SchemaField("reward_imp_7",        "INTEGER"),
        bigquery.SchemaField("banner_imp_7",        "INTEGER"),
        bigquery.SchemaField("user_count_0",        "INTEGER"),
        bigquery.SchemaField("user_count_1",        "INTEGER"),
        bigquery.SchemaField("user_count_7",        "INTEGER"),
        bigquery.SchemaField("user_count_30",       "INTEGER"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "max_cohort_sessions": [
        bigquery.SchemaField("day",                 "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("installs",            "INTEGER"),
        bigquery.SchemaField("retention_1",         "FLOAT"),
        bigquery.SchemaField("retention_3",         "FLOAT"),
        bigquery.SchemaField("retention_7",         "FLOAT"),
        bigquery.SchemaField("retention_14",        "FLOAT"),
        bigquery.SchemaField("retention_30",        "FLOAT"),
        bigquery.SchemaField("session_count_0",     "INTEGER"),
        bigquery.SchemaField("session_count_1",     "INTEGER"),
        bigquery.SchemaField("session_count_7",     "INTEGER"),
        bigquery.SchemaField("session_count_30",    "INTEGER"),
        bigquery.SchemaField("daily_usage_0",       "FLOAT"),
        bigquery.SchemaField("daily_usage_1",       "FLOAT"),
        bigquery.SchemaField("daily_usage_7",       "FLOAT"),
        bigquery.SchemaField("daily_usage_30",      "FLOAT"),
        bigquery.SchemaField("user_count_1",        "INTEGER"),
        bigquery.SchemaField("user_count_7",        "INTEGER"),
        bigquery.SchemaField("user_count_30",       "INTEGER"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ua_campaigns_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign",            "STRING"),
        bigquery.SchemaField("campaign_id_external","STRING"),
        bigquery.SchemaField("campaign_package_name","STRING"),
        bigquery.SchemaField("campaign_type",       "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("conversions",         "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("ctr",                 "FLOAT"),
        bigquery.SchemaField("conversion_rate",     "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("roas",                "FLOAT"),
        bigquery.SchemaField("roas_0d",             "FLOAT"),
        bigquery.SchemaField("roas_1d",             "FLOAT"),
        bigquery.SchemaField("roas_3d",             "FLOAT"),
        bigquery.SchemaField("roas_7d",             "FLOAT"),
        bigquery.SchemaField("roas_14d",            "FLOAT"),
        bigquery.SchemaField("roas_30d",            "FLOAT"),
        bigquery.SchemaField("iap_roas_7d",         "FLOAT"),
        bigquery.SchemaField("iap_roas_30d",        "FLOAT"),
        bigquery.SchemaField("ad_roas_7d",          "FLOAT"),
        bigquery.SchemaField("ad_roas_30d",         "FLOAT"),
        bigquery.SchemaField("iap_rev_7d",          "FLOAT"),
        bigquery.SchemaField("iap_rev_30d",         "FLOAT"),
        bigquery.SchemaField("ad_rev_7d",           "FLOAT"),
        bigquery.SchemaField("ad_rev_30d",          "FLOAT"),
        bigquery.SchemaField("total_rev_7d",        "FLOAT"),
        bigquery.SchemaField("total_rev_30d",       "FLOAT"),
        bigquery.SchemaField("ret_1d",              "FLOAT"),
        bigquery.SchemaField("ret_3d",              "FLOAT"),
        bigquery.SchemaField("ret_7d",              "FLOAT"),
        bigquery.SchemaField("ret_14d",             "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ua_by_country": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign",            "STRING"),
        bigquery.SchemaField("campaign_package_name","STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("conversions",         "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("roas_7d",             "FLOAT"),
        bigquery.SchemaField("roas_30d",            "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ua_by_platform": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign",            "STRING"),
        bigquery.SchemaField("campaign_package_name","STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("conversions",         "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ua_by_creative_type": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign",            "STRING"),
        bigquery.SchemaField("campaign_package_name","STRING"),
        bigquery.SchemaField("ad_creative_type",    "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("conversions",         "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("ctr",                 "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ua_by_app": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign_package_name","STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("conversions",         "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("roas",                "FLOAT"),
        bigquery.SchemaField("roas_7d",             "FLOAT"),
        bigquery.SchemaField("roas_30d",            "FLOAT"),
        bigquery.SchemaField("total_rev_7d",        "FLOAT"),
        bigquery.SchemaField("total_rev_30d",       "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "ua_creatives_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("asset_id",            "STRING"),
        bigquery.SchemaField("asset_name",          "STRING"),
        bigquery.SchemaField("campaign",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("ctr",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "pub_revenue_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("ctr",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "pub_revenue_by_country": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "pub_revenue_by_adtype": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("ad_type",             "STRING"),
        bigquery.SchemaField("size",                "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    "pub_revenue_by_platform": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("package_name",        "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("device_type",         "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("ecpm",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(v) if v not in (None,"","N/A","nan") else None
    except: return None

def safe_int(v):
    try: return int(float(v)) if v not in (None,"","N/A","nan") else None
    except: return None

def now_ts(): return datetime.utcnow().isoformat()

def date_range():
    end = datetime.utcnow().date()
    return str(end - timedelta(days=LOOKBACK_DAYS)), str(end)

def cohort_range():
    end = datetime.utcnow().date()
    return str(end - timedelta(days=44)), str(end)

def fetch_csv(url, params):
    params["api_key"] = APPLOVIN_REPORT_KEY
    params["format"]  = "csv"
    log.info(f"  GET {url} cols={params.get('columns','')[:70]}")
    try:
        resp = requests.get(url, params=params, timeout=120)
        if resp.status_code != 200:
            log.warning(f"  API error {resp.status_code}: {resp.text[:300]}")
            return []
        text = resp.text.strip()
        if not text or text.lower().startswith("error"):
            log.warning(f"  Empty/error: {text[:200]}")
            return []
        return list(csv.DictReader(io.StringIO(text)))
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

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
    table_ref  = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    BATCH_SIZE = 200
    total_errors = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            errs = client.insert_rows_json(table_ref, batch)
            if errs: total_errors.extend(errs[:2])
        except Exception as e:
            log.error(f"  Batch {i} failed: {e}")
    if total_errors: log.error(f"BQ errors [{name}]: {total_errors[:2]}")
    else: log.info(f"  ✅ {len(rows):,} rows → {name}")

# ─── MAX REVENUE ──────────────────────────────────────────────────────────────
def fetch_max_revenue_daily():
    log.info("Fetching MAX Revenue Daily...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {"start":start,"end":end,
        "columns":"day,application,package_name,platform,impressions,estimated_revenue,ecpm,requests"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package_name"),"platform":r.get("Platform"),
            "impressions":safe_int(r.get("Impressions")),
            "estimated_revenue":safe_float(r.get("Estimated_revenue")),
            "ecpm":safe_float(r.get("Ecpm")),"requests":safe_int(r.get("Requests")),
            "_ingested_at":now_ts()})
    return rows

def fetch_max_revenue_by_country():
    log.info("Fetching MAX Revenue by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {"start":start,"end":end,
        "columns":"day,application,package_name,platform,country,impressions,estimated_revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package_name"),"platform":r.get("Platform"),
            "country":r.get("Country"),"impressions":safe_int(r.get("Impressions")),
            "estimated_revenue":safe_float(r.get("Estimated_revenue")),
            "ecpm":safe_float(r.get("Ecpm")),"_ingested_at":now_ts()})
    return rows

def fetch_max_revenue_by_network():
    log.info("Fetching MAX Revenue by Network...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {"start":start,"end":end,
        "columns":"day,application,package_name,platform,network,impressions,estimated_revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package_name"),"platform":r.get("Platform"),
            "network":r.get("Network"),"impressions":safe_int(r.get("Impressions")),
            "estimated_revenue":safe_float(r.get("Estimated_revenue")),
            "ecpm":safe_float(r.get("Ecpm")),"_ingested_at":now_ts()})
    return rows

def fetch_max_revenue_by_format():
    log.info("Fetching MAX Revenue by Format...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {"start":start,"end":end,
        "columns":"day,application,package_name,platform,ad_format,impressions,estimated_revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package_name"),"platform":r.get("Platform"),
            "ad_format":r.get("Ad_format"),"impressions":safe_int(r.get("Impressions")),
            "estimated_revenue":safe_float(r.get("Estimated_revenue")),
            "ecpm":safe_float(r.get("Ecpm")),"_ingested_at":now_ts()})
    return rows

def fetch_max_revenue_by_device():
    log.info("Fetching MAX Revenue by Device...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {"start":start,"end":end,
        "columns":"day,application,package_name,platform,device_type,impressions,estimated_revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package_name"),"platform":r.get("Platform"),
            "device_type":r.get("Device_type"),"impressions":safe_int(r.get("Impressions")),
            "estimated_revenue":safe_float(r.get("Estimated_revenue")),
            "ecpm":safe_float(r.get("Ecpm")),"_ingested_at":now_ts()})
    return rows

def fetch_max_revenue_by_hour():
    log.info("Fetching MAX Revenue by Hour (last 7 days)...")
    end   = datetime.utcnow().date()
    start = str(end - timedelta(days=7))
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {"start":start,"end":str(end),
        "columns":"day,hour,application,package_name,platform,impressions,estimated_revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"hour":r.get("Hour"),
            "application":r.get("Application"),"package_name":r.get("Package_name"),
            "platform":r.get("Platform"),"impressions":safe_int(r.get("Impressions")),
            "estimated_revenue":safe_float(r.get("Estimated_revenue")),
            "ecpm":safe_float(r.get("Ecpm")),"_ingested_at":now_ts()})
    return rows

def fetch_max_user_revenue():
    log.info("Fetching MAX User Revenue (android, yesterday)...")
    yesterday = str(datetime.utcnow().date() - timedelta(days=1))
    rows = []
    # User revenue API requires fetching aggregated data - no per-user breakdown without app_id
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": yesterday, "end": yesterday,
        "columns": "day,application,package_name,platform,country,network,ad_format,impressions,estimated_revenue",
    }):
        rows.append({"date":r.get("Day") or r.get("day"),
            "application":r.get("Application") or r.get("application"),
            "package_name":r.get("Package_name") or r.get("package_name"),
            "platform":r.get("Platform") or r.get("platform"),
            "country":r.get("Country") or r.get("country"),
            "network":r.get("Network") or r.get("network"),
            "ad_format":r.get("Ad_format") or r.get("ad_format"),
            "impressions":safe_int(r.get("Impressions") or r.get("impressions")),
            "estimated_revenue":safe_float(r.get("Estimated_revenue") or r.get("estimated_revenue")),
            "_ingested_at":now_ts()})
    return rows

# ─── MAX COHORT ───────────────────────────────────────────────────────────────
def fetch_max_cohort_revenue():
    log.info("Fetching MAX Cohort Revenue...")
    start, end = cohort_range()
    rows = []
    for r in fetch_csv(MAX_COHORT_URL, {"start":start,"end":end,
        "columns":"day,application,package_name,platform,country,installs,"
                  "pub_revenue_0,pub_revenue_1,pub_revenue_3,pub_revenue_7,"
                  "pub_revenue_14,pub_revenue_30,pub_revenue_45,"
                  "rpi_0,rpi_1,rpi_3,rpi_7,rpi_14,rpi_30,rpi_45,"
                  "inter_pub_revenue_7,reward_pub_revenue_7,banner_pub_revenue_7,mrec_pub_revenue_7"}):
        rows.append({"day":r.get("day"),"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "country":r.get("country"),"installs":safe_int(r.get("installs")),
            "pub_revenue_0":safe_float(r.get("pub_revenue_0")),
            "pub_revenue_1":safe_float(r.get("pub_revenue_1")),
            "pub_revenue_3":safe_float(r.get("pub_revenue_3")),
            "pub_revenue_7":safe_float(r.get("pub_revenue_7")),
            "pub_revenue_14":safe_float(r.get("pub_revenue_14")),
            "pub_revenue_30":safe_float(r.get("pub_revenue_30")),
            "pub_revenue_45":safe_float(r.get("pub_revenue_45")),
            "rpi_0":safe_float(r.get("rpi_0")),"rpi_1":safe_float(r.get("rpi_1")),
            "rpi_3":safe_float(r.get("rpi_3")),"rpi_7":safe_float(r.get("rpi_7")),
            "rpi_14":safe_float(r.get("rpi_14")),"rpi_30":safe_float(r.get("rpi_30")),
            "rpi_45":safe_float(r.get("rpi_45")),
            "inter_pub_revenue_7":safe_float(r.get("inter_pub_revenue_7")),
            "reward_pub_revenue_7":safe_float(r.get("reward_pub_revenue_7")),
            "banner_pub_revenue_7":safe_float(r.get("banner_pub_revenue_7")),
            "mrec_pub_revenue_7":safe_float(r.get("mrec_pub_revenue_7")),
            "_ingested_at":now_ts()})
    return rows

def fetch_max_cohort_impressions():
    log.info("Fetching MAX Cohort Impressions...")
    start, end = cohort_range()
    rows = []
    for r in fetch_csv(f"{MAX_COHORT_URL}/imp", {"start":start,"end":end,
        "columns":"day,application,package_name,platform,country,installs,"
                  "imp_0,imp_1,imp_3,imp_7,imp_14,imp_30,imp_45,"
                  "imp_per_user_0,imp_per_user_1,imp_per_user_7,imp_per_user_30,"
                  "inter_imp_7,reward_imp_7,banner_imp_7,"
                  "user_count_0,user_count_1,user_count_7,user_count_30"}):
        rows.append({"day":r.get("day"),"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "country":r.get("country"),"installs":safe_int(r.get("installs")),
            "imp_0":safe_int(r.get("imp_0")),"imp_1":safe_int(r.get("imp_1")),
            "imp_3":safe_int(r.get("imp_3")),"imp_7":safe_int(r.get("imp_7")),
            "imp_14":safe_int(r.get("imp_14")),"imp_30":safe_int(r.get("imp_30")),
            "imp_45":safe_int(r.get("imp_45")),
            "imp_per_user_0":safe_float(r.get("imp_per_user_0")),
            "imp_per_user_1":safe_float(r.get("imp_per_user_1")),
            "imp_per_user_7":safe_float(r.get("imp_per_user_7")),
            "imp_per_user_30":safe_float(r.get("imp_per_user_30")),
            "inter_imp_7":safe_int(r.get("inter_imp_7")),
            "reward_imp_7":safe_int(r.get("reward_imp_7")),
            "banner_imp_7":safe_int(r.get("banner_imp_7")),
            "user_count_0":safe_int(r.get("user_count_0")),
            "user_count_1":safe_int(r.get("user_count_1")),
            "user_count_7":safe_int(r.get("user_count_7")),
            "user_count_30":safe_int(r.get("user_count_30")),
            "_ingested_at":now_ts()})
    return rows

def fetch_max_cohort_sessions():
    log.info("Fetching MAX Cohort Sessions...")
    start, end = cohort_range()
    rows = []
    for r in fetch_csv(f"{MAX_COHORT_URL}/session", {"start":start,"end":end,
        "columns":"day,application,package_name,platform,country,installs,"
                  "retention_1,retention_3,retention_7,retention_14,retention_30,"
                  "session_count_0,session_count_1,session_count_7,session_count_30,"
                  "daily_usage_0,daily_usage_1,daily_usage_7,daily_usage_30,"
                  "user_count_1,user_count_7,user_count_30"}):
        rows.append({"day":r.get("day"),"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "country":r.get("country"),"installs":safe_int(r.get("installs")),
            "retention_1":safe_float(r.get("retention_1")),
            "retention_3":safe_float(r.get("retention_3")),
            "retention_7":safe_float(r.get("retention_7")),
            "retention_14":safe_float(r.get("retention_14")),
            "retention_30":safe_float(r.get("retention_30")),
            "session_count_0":safe_int(r.get("session_count_0")),
            "session_count_1":safe_int(r.get("session_count_1")),
            "session_count_7":safe_int(r.get("session_count_7")),
            "session_count_30":safe_int(r.get("session_count_30")),
            "daily_usage_0":safe_float(r.get("daily_usage_0")),
            "daily_usage_1":safe_float(r.get("daily_usage_1")),
            "daily_usage_7":safe_float(r.get("daily_usage_7")),
            "daily_usage_30":safe_float(r.get("daily_usage_30")),
            "user_count_1":safe_int(r.get("user_count_1")),
            "user_count_7":safe_int(r.get("user_count_7")),
            "user_count_30":safe_int(r.get("user_count_30")),
            "_ingested_at":now_ts()})
    return rows

# ─── UA (ADVERTISER) ──────────────────────────────────────────────────────────
def fetch_ua_campaigns_daily():
    log.info("Fetching UA Campaigns Daily (requires AppDiscovery to be active)...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"advertiser",
        "columns":"day,campaign,campaign_id_external,campaign_package_name,campaign_type,"
                  "platform,country,impressions,clicks,conversions,cost,ctr,conversion_rate,"
                  "roas_0d,roas_1d,roas_3d,roas_7d,roas_14d,roas_30d,"
                  "iap_roas_7d,iap_roas_30d,ad_roas_7d,ad_roas_30d,"
                  "iap_rev_7d,iap_rev_30d,ad_rev_7d,ad_rev_30d,total_rev_7d,total_rev_30d,"
                  "ret_1d,ret_3d,ret_7d,ret_14d"}):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        rev   = safe_float(r.get("total_rev_7d")) or 0.0
        rows.append({"date":r.get("day"),"campaign":r.get("campaign"),
            "campaign_id_external":r.get("campaign_id_external"),
            "campaign_package_name":r.get("campaign_package_name"),
            "campaign_type":r.get("campaign_type"),
            "platform":r.get("platform"),"country":r.get("country"),
            "impressions":safe_int(r.get("impressions")),"clicks":safe_int(r.get("clicks")),
            "conversions":convs,"cost":cost,"ctr":safe_float(r.get("ctr")),
            "conversion_rate":safe_float(r.get("conversion_rate")),
            "cpi":round(cost/convs,4) if convs else None,
            "revenue":rev,"roas":round(rev/cost,4) if cost and rev else None,
            "roas_0d":safe_float(r.get("roas_0d")),"roas_1d":safe_float(r.get("roas_1d")),
            "roas_3d":safe_float(r.get("roas_3d")),"roas_7d":safe_float(r.get("roas_7d")),
            "roas_14d":safe_float(r.get("roas_14d")),"roas_30d":safe_float(r.get("roas_30d")),
            "iap_roas_7d":safe_float(r.get("iap_roas_7d")),
            "iap_roas_30d":safe_float(r.get("iap_roas_30d")),
            "ad_roas_7d":safe_float(r.get("ad_roas_7d")),
            "ad_roas_30d":safe_float(r.get("ad_roas_30d")),
            "iap_rev_7d":safe_float(r.get("iap_rev_7d")),
            "iap_rev_30d":safe_float(r.get("iap_rev_30d")),
            "ad_rev_7d":safe_float(r.get("ad_rev_7d")),
            "ad_rev_30d":safe_float(r.get("ad_rev_30d")),
            "total_rev_7d":safe_float(r.get("total_rev_7d")),
            "total_rev_30d":safe_float(r.get("total_rev_30d")),
            "ret_1d":safe_float(r.get("ret_1d")),"ret_3d":safe_float(r.get("ret_3d")),
            "ret_7d":safe_float(r.get("ret_7d")),"ret_14d":safe_float(r.get("ret_14d")),
            "_ingested_at":now_ts()})
    return rows

def fetch_ua_by_country():
    log.info("Fetching UA by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"advertiser",
        "columns":"day,campaign,campaign_package_name,country,impressions,clicks,conversions,cost,roas_7d,roas_30d"}):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        rows.append({"date":r.get("day"),"campaign":r.get("campaign"),
            "campaign_package_name":r.get("campaign_package_name"),
            "country":r.get("country"),"impressions":safe_int(r.get("impressions")),
            "clicks":safe_int(r.get("clicks")),"conversions":convs,"cost":cost,
            "cpi":round(cost/convs,4) if convs else None,
            "roas_7d":safe_float(r.get("roas_7d")),
            "roas_30d":safe_float(r.get("roas_30d")),
            "_ingested_at":now_ts()})
    return rows

def fetch_ua_by_platform():
    log.info("Fetching UA by Platform...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"advertiser",
        "columns":"day,campaign,campaign_package_name,platform,impressions,clicks,conversions,cost"}):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        rows.append({"date":r.get("day"),"campaign":r.get("campaign"),
            "campaign_package_name":r.get("campaign_package_name"),
            "platform":r.get("platform"),"impressions":safe_int(r.get("impressions")),
            "clicks":safe_int(r.get("clicks")),"conversions":convs,"cost":cost,
            "cpi":round(cost/convs,4) if convs else None,"_ingested_at":now_ts()})
    return rows

def fetch_ua_by_creative_type():
    log.info("Fetching UA by Creative Type...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"advertiser",
        "columns":"day,campaign,campaign_package_name,ad_creative_type,impressions,clicks,conversions,cost,ctr"}):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        rows.append({"date":r.get("day"),"campaign":r.get("campaign"),
            "campaign_package_name":r.get("campaign_package_name"),
            "ad_creative_type":r.get("ad_creative_type"),
            "impressions":safe_int(r.get("impressions")),"clicks":safe_int(r.get("clicks")),
            "conversions":convs,"cost":cost,"ctr":safe_float(r.get("ctr")),
            "cpi":round(cost/convs,4) if convs else None,"_ingested_at":now_ts()})
    return rows

def fetch_ua_by_app():
    log.info("Fetching UA by App...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"advertiser",
        "columns":"day,campaign_package_name,platform,impressions,clicks,conversions,cost,"
                  "roas_7d,roas_30d,total_rev_7d,total_rev_30d"}):
        cost  = safe_float(r.get("cost")) or 0.0
        rev   = safe_float(r.get("total_rev_7d")) or 0.0
        rows.append({"date":r.get("day"),
            "campaign_package_name":r.get("campaign_package_name"),
            "platform":r.get("platform"),"impressions":safe_int(r.get("impressions")),
            "clicks":safe_int(r.get("clicks")),
            "conversions":safe_int(r.get("conversions")),"cost":cost,
            "revenue":rev,"roas":round(rev/cost,4) if cost and rev else None,
            "roas_7d":safe_float(r.get("roas_7d")),
            "roas_30d":safe_float(r.get("roas_30d")),
            "total_rev_7d":safe_float(r.get("total_rev_7d")),
            "total_rev_30d":safe_float(r.get("total_rev_30d")),
            "_ingested_at":now_ts()})
    return rows

def fetch_ua_creatives_daily():
    log.info("Fetching UA Creatives (Asset Report)...")
    rows = []
    for r in fetch_csv(UA_ASSET_URL, {"range":"last_7d",
        "columns":"asset_id,asset_name,campaign,impressions,clicks,cost,ctr"}):
        rows.append({"date":str(datetime.utcnow().date() - timedelta(days=1)),
            "asset_id":r.get("asset_id"),"asset_name":r.get("asset_name"),
            "campaign":r.get("campaign"),"impressions":safe_int(r.get("impressions")),
            "clicks":safe_int(r.get("clicks")),"cost":safe_float(r.get("cost")),
            "ctr":safe_float(r.get("ctr")),"_ingested_at":now_ts()})
    return rows

# ─── PUBLISHER (AD REVENUE) ───────────────────────────────────────────────────
def fetch_pub_revenue_daily():
    log.info("Fetching Publisher Revenue Daily...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"publisher",
        "columns":"day,application,package_name,platform,impressions,clicks,revenue,ecpm,ctr"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package Name"),"platform":r.get("Platform"),
            "impressions":safe_int(r.get("Impressions")),"clicks":safe_int(r.get("Clicks")),
            "revenue":safe_float(r.get("Revenue")),"ecpm":safe_float(r.get("Ecpm")),
            "ctr":safe_float(r.get("Ctr")),"_ingested_at":now_ts()})
    return rows

def fetch_pub_revenue_by_country():
    log.info("Fetching Publisher Revenue by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"publisher",
        "columns":"day,application,package_name,platform,country,impressions,revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package Name"),"platform":r.get("Platform"),
            "country":r.get("Country"),"impressions":safe_int(r.get("Impressions")),
            "revenue":safe_float(r.get("Revenue")),"ecpm":safe_float(r.get("Ecpm")),
            "_ingested_at":now_ts()})
    return rows

def fetch_pub_revenue_by_adtype():
    log.info("Fetching Publisher Revenue by Ad Type...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"publisher",
        "columns":"day,application,package_name,platform,ad_type,size,impressions,revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package Name"),"platform":r.get("Platform"),
            "ad_type":r.get("Ad_type"),"size":r.get("Size"),
            "impressions":safe_int(r.get("Impressions")),
            "revenue":safe_float(r.get("Revenue")),"ecpm":safe_float(r.get("Ecpm")),
            "_ingested_at":now_ts()})
    return rows

def fetch_pub_revenue_by_platform():
    log.info("Fetching Publisher Revenue by Platform/Device...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {"start":start,"end":end,"report_type":"publisher",
        "columns":"day,application,package_name,platform,device_type,impressions,revenue,ecpm"}):
        rows.append({"date":r.get("Day"),"application":r.get("Application"),
            "package_name":r.get("Package Name"),"platform":r.get("Platform"),
            "device_type":r.get("Device_type"),"impressions":safe_int(r.get("Impressions")),
            "revenue":safe_float(r.get("Revenue")),"ecpm":safe_float(r.get("Ecpm")),
            "_ingested_at":now_ts()})
    return rows

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 AppLovin → BigQuery COMPLETE sync")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")
    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS: ensure_table(bq, t)

    log.info("── MAX Revenue ──")
    load_to_bq(bq, "max_revenue_daily",         fetch_max_revenue_daily())
    load_to_bq(bq, "max_revenue_by_country",     fetch_max_revenue_by_country())
    load_to_bq(bq, "max_revenue_by_network",     fetch_max_revenue_by_network())
    load_to_bq(bq, "max_revenue_by_format",      fetch_max_revenue_by_format())
    load_to_bq(bq, "max_revenue_by_device",      fetch_max_revenue_by_device())
    load_to_bq(bq, "max_revenue_by_hour",        fetch_max_revenue_by_hour())
    load_to_bq(bq, "max_user_revenue",           fetch_max_user_revenue())

    log.info("── MAX Cohort ──")
    load_to_bq(bq, "max_cohort_revenue",         fetch_max_cohort_revenue())
    load_to_bq(bq, "max_cohort_impressions",     fetch_max_cohort_impressions())
    load_to_bq(bq, "max_cohort_sessions",        fetch_max_cohort_sessions())

    log.info("── AppDiscovery (UA) ──")
    load_to_bq(bq, "ua_campaigns_daily",         fetch_ua_campaigns_daily())
    load_to_bq(bq, "ua_by_country",              fetch_ua_by_country())
    load_to_bq(bq, "ua_by_platform",             fetch_ua_by_platform())
    load_to_bq(bq, "ua_by_creative_type",        fetch_ua_by_creative_type())
    load_to_bq(bq, "ua_by_app",                  fetch_ua_by_app())
    load_to_bq(bq, "ua_creatives_daily",         fetch_ua_creatives_daily())

    log.info("── Publisher Revenue ──")
    load_to_bq(bq, "pub_revenue_daily",          fetch_pub_revenue_daily())
    load_to_bq(bq, "pub_revenue_by_country",     fetch_pub_revenue_by_country())
    load_to_bq(bq, "pub_revenue_by_adtype",      fetch_pub_revenue_by_adtype())
    load_to_bq(bq, "pub_revenue_by_platform",    fetch_pub_revenue_by_platform())

    log.info("✅ COMPLETE AppLovin sync done! 20 tables loaded.")

if __name__ == "__main__":
    main()
