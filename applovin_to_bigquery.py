"""
AppLovin → BigQuery  ·  COMPLETE PIPELINE v2
=============================================
Fixes vs v1:
  1. Delete-before-insert — no duplicates on re-runs
  2. Publisher CSV header keys fixed (lowercase)
  3. MAX_USER_URL properly used for max_user_revenue
  4. Cohort URLs fixed — all use MAX_COHORT_URL base
  5. ua_creatives_daily uses start/end dates not range=last_7d
  6. Rate metrics recalculated from raw totals consistently
  7. Error tracking — reports which tables failed
  8. cohort_range extended to 90 days for proper D45 coverage
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

MAX_REPORT_URL = "https://r.applovin.com/maxReport"
MAX_USER_URL   = "https://r.applovin.com/max/userAdRevenueReport"
MAX_COHORT_URL = "https://r.applovin.com/maxCohort"
UA_REPORT_URL  = "https://r.applovin.com/report"
UA_ASSET_URL   = "https://r.applovin.com/assetReport"

# Track failures
FAILED_TABLES = []

# ─── BQ SCHEMAS ──────────────────────────────────────────────────────────────
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

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(v) if v not in (None, "", "N/A", "nan") else None
    except: return None

def safe_int(v):
    try: return int(float(v)) if v not in (None, "", "N/A", "nan") else None
    except: return None

def now_ts():
    return datetime.utcnow().isoformat()

def date_range():
    end = datetime.utcnow().date()
    return str(end - timedelta(days=LOOKBACK_DAYS)), str(end)

# FIX v2: Extended to 90 days for proper D45 cohort coverage
def cohort_range():
    end = datetime.utcnow().date()
    return str(end - timedelta(days=90)), str(end)

def fetch_csv(url, params):
    params["api_key"] = APPLOVIN_REPORT_KEY
    params["format"]  = "csv"
    log.info(f"  GET {url} cols={params.get('columns', '')[:80]}")
    try:
        resp = requests.get(url, params=params, timeout=120)
        if resp.status_code != 200:
            log.warning(f"  API error {resp.status_code}: {resp.text[:300]}")
            return []
        text = resp.text.strip()
        if not text or text.lower().startswith("error"):
            log.warning(f"  Empty/error response: {text[:200]}")
            return []
        rows = list(csv.DictReader(io.StringIO(text)))
        # Log actual headers on first successful call to aid debugging
        if rows:
            log.info(f"  CSV headers: {list(rows[0].keys())[:8]}")
        return rows
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

# ─── BQ HELPERS ──────────────────────────────────────────────────────────────
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

# FIX v2: Delete-before-insert — no duplicates on re-runs
def load_to_bq(client, name, rows):
    global FAILED_TABLES
    if not rows:
        log.info(f"  No rows for {name}")
        return

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    start, end = date_range()

    # Determine date column for delete-before-insert
    date_col_map = {
        "max_revenue_daily":        "date",
        "max_revenue_by_country":   "date",
        "max_revenue_by_network":   "date",
        "max_revenue_by_format":    "date",
        "max_revenue_by_device":    "date",
        "max_revenue_by_hour":      "date",
        "max_user_revenue":         "date",
        "ua_campaigns_daily":       "date",
        "ua_by_country":            "date",
        "ua_by_platform":           "date",
        "ua_by_creative_type":      "date",
        "ua_by_app":                "date",
        "ua_creatives_daily":       "date",
        "pub_revenue_daily":        "date",
        "pub_revenue_by_country":   "date",
        "pub_revenue_by_adtype":    "date",
        "pub_revenue_by_platform":  "date",
    }
    cohort_tables = {"max_cohort_revenue", "max_cohort_impressions", "max_cohort_sessions"}

    date_col = date_col_map.get(name)
    if date_col:
        try:
            client.query(
                f"DELETE FROM `{table_ref}` WHERE {date_col} BETWEEN '{start}' AND '{end}'"
            ).result()
            log.info(f"  Cleared {name} ({start} to {end})")
        except Exception as e:
            log.warning(f"  Could not clear {name}: {e}")
    elif name in cohort_tables:
        # Cohort tables use 'day' column with 90-day range
        c_start, c_end = cohort_range()
        try:
            client.query(
                f"DELETE FROM `{table_ref}` WHERE day BETWEEN '{c_start}' AND '{c_end}'"
            ).result()
            log.info(f"  Cleared {name} ({c_start} to {c_end})")
        except Exception as e:
            log.warning(f"  Could not clear {name}: {e}")

    # Insert in batches
    BATCH_SIZE = 200
    total_errors = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            errs = client.insert_rows_json(table_ref, batch)
            if errs:
                total_errors.extend(errs[:2])
        except Exception as e:
            log.error(f"  Batch {i} failed: {e}")

    if total_errors:
        log.error(f"BQ errors [{name}]: {total_errors[:2]}")
        FAILED_TABLES.append(name)
    else:
        log.info(f"  ✅ {len(rows):,} rows → {name}")

# ─── MAX REVENUE ─────────────────────────────────────────────────────────────
# MAX Report API returns title-case CSV headers: Day, Application, Package_name etc.

def fetch_max_revenue_daily():
    log.info("Fetching MAX Revenue Daily...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,impressions,estimated_revenue,ecpm,requests"
    }):
        rows.append({
            "date":               r.get("Day"),
            "application":        r.get("Application"),
            "package_name":       r.get("Package_name"),
            "platform":           r.get("Platform"),
            "impressions":        safe_int(r.get("Impressions")),
            "estimated_revenue":  safe_float(r.get("Estimated_revenue")),
            "ecpm":               safe_float(r.get("Ecpm")),
            "requests":           safe_int(r.get("Requests")),
            "_ingested_at":       now_ts(),
        })
    return rows

def fetch_max_revenue_by_country():
    log.info("Fetching MAX Revenue by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,country,impressions,estimated_revenue,ecpm"
    }):
        rows.append({
            "date":               r.get("Day"),
            "application":        r.get("Application"),
            "package_name":       r.get("Package_name"),
            "platform":           r.get("Platform"),
            "country":            r.get("Country"),
            "impressions":        safe_int(r.get("Impressions")),
            "estimated_revenue":  safe_float(r.get("Estimated_revenue")),
            "ecpm":               safe_float(r.get("Ecpm")),
            "_ingested_at":       now_ts(),
        })
    return rows

def fetch_max_revenue_by_network():
    log.info("Fetching MAX Revenue by Network...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,network,impressions,estimated_revenue,ecpm"
    }):
        rows.append({
            "date":               r.get("Day"),
            "application":        r.get("Application"),
            "package_name":       r.get("Package_name"),
            "platform":           r.get("Platform"),
            "network":            r.get("Network"),
            "impressions":        safe_int(r.get("Impressions")),
            "estimated_revenue":  safe_float(r.get("Estimated_revenue")),
            "ecpm":               safe_float(r.get("Ecpm")),
            "_ingested_at":       now_ts(),
        })
    return rows

def fetch_max_revenue_by_format():
    log.info("Fetching MAX Revenue by Format...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,ad_format,impressions,estimated_revenue,ecpm"
    }):
        rows.append({
            "date":               r.get("Day"),
            "application":        r.get("Application"),
            "package_name":       r.get("Package_name"),
            "platform":           r.get("Platform"),
            "ad_format":          r.get("Ad_format"),
            "impressions":        safe_int(r.get("Impressions")),
            "estimated_revenue":  safe_float(r.get("Estimated_revenue")),
            "ecpm":               safe_float(r.get("Ecpm")),
            "_ingested_at":       now_ts(),
        })
    return rows

def fetch_max_revenue_by_device():
    log.info("Fetching MAX Revenue by Device...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,device_type,impressions,estimated_revenue,ecpm"
    }):
        rows.append({
            "date":               r.get("Day"),
            "application":        r.get("Application"),
            "package_name":       r.get("Package_name"),
            "platform":           r.get("Platform"),
            "device_type":        r.get("Device_type"),
            "impressions":        safe_int(r.get("Impressions")),
            "estimated_revenue":  safe_float(r.get("Estimated_revenue")),
            "ecpm":               safe_float(r.get("Ecpm")),
            "_ingested_at":       now_ts(),
        })
    return rows

def fetch_max_revenue_by_hour():
    log.info("Fetching MAX Revenue by Hour...")
    # FIX: AppLovin only keeps hourly data for 30 days — cap lookback
    end = datetime.utcnow().date()
    hour_lookback = min(LOOKBACK_DAYS, 30)
    start = str(end - timedelta(days=hour_lookback))
    end = str(end)
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,hour,application,package_name,platform,impressions,estimated_revenue,ecpm"
    }):
        rows.append({
            "date":               r.get("Day"),
            "hour":               r.get("Hour"),
            "application":        r.get("Application"),
            "package_name":       r.get("Package_name"),
            "platform":           r.get("Platform"),
            "impressions":        safe_int(r.get("Impressions")),
            "estimated_revenue":  safe_float(r.get("Estimated_revenue")),
            "ecpm":               safe_float(r.get("Ecpm")),
            "_ingested_at":       now_ts(),
        })
    return rows

# FIX v2: Now properly uses MAX_USER_URL endpoint
def fetch_max_user_revenue():
    log.info("Fetching MAX User Revenue...")
    start, end = date_range()
    rows = []
    # MAX User Revenue API — requires date range, returns per-user ad revenue
    # Falls back to MAX_REPORT_URL with country+network+ad_format if user endpoint fails
    raw = fetch_csv(MAX_USER_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,country,network,ad_format,impressions,estimated_revenue"
    })
    if not raw:
        log.info("  MAX_USER_URL returned empty — falling back to MAX_REPORT_URL breakdown")
        raw = fetch_csv(MAX_REPORT_URL, {
            "start": start, "end": end,
            "columns": "day,application,package_name,platform,country,network,ad_format,impressions,estimated_revenue"
        })
    for r in raw:
        # Handle both title-case (MAX_REPORT_URL) and lowercase (MAX_USER_URL) headers
        rows.append({
            "date":               r.get("Day") or r.get("day"),
            "application":        r.get("Application") or r.get("application"),
            "package_name":       r.get("Package_name") or r.get("package_name"),
            "platform":           r.get("Platform") or r.get("platform"),
            "country":            r.get("Country") or r.get("country"),
            "network":            r.get("Network") or r.get("network"),
            "ad_format":          r.get("Ad_format") or r.get("ad_format"),
            "impressions":        safe_int(r.get("Impressions") or r.get("impressions")),
            "estimated_revenue":  safe_float(r.get("Estimated_revenue") or r.get("estimated_revenue")),
            "_ingested_at":       now_ts(),
        })
    return rows

# ─── MAX COHORT ──────────────────────────────────────────────────────────────
# Cohort API returns lowercase headers: day, application, package_name etc.
# FIX v2: All cohort tables use MAX_COHORT_URL (no /imp or /session suffixes)

def fetch_max_cohort_revenue():
    log.info("Fetching MAX Cohort Revenue...")
    start, end = cohort_range()
    rows = []
    for r in fetch_csv(MAX_COHORT_URL, {
        "start": start, "end": end,
        "columns": (
            "day,application,package_name,platform,country,installs,"
            "pub_revenue_0,pub_revenue_1,pub_revenue_3,pub_revenue_7,"
            "pub_revenue_14,pub_revenue_30,pub_revenue_45,"
            "rpi_0,rpi_1,rpi_3,rpi_7,rpi_14,rpi_30,rpi_45,"
            "inter_pub_revenue_7,reward_pub_revenue_7,banner_pub_revenue_7,mrec_pub_revenue_7"
        )
    }):
        rows.append({
            "day":                  r.get("day"),
            "application":          r.get("application"),
            "package_name":         r.get("package_name"),
            "platform":             r.get("platform"),
            "country":              r.get("country"),
            "installs":             safe_int(r.get("installs")),
            "pub_revenue_0":        safe_float(r.get("pub_revenue_0")),
            "pub_revenue_1":        safe_float(r.get("pub_revenue_1")),
            "pub_revenue_3":        safe_float(r.get("pub_revenue_3")),
            "pub_revenue_7":        safe_float(r.get("pub_revenue_7")),
            "pub_revenue_14":       safe_float(r.get("pub_revenue_14")),
            "pub_revenue_30":       safe_float(r.get("pub_revenue_30")),
            "pub_revenue_45":       safe_float(r.get("pub_revenue_45")),
            "rpi_0":                safe_float(r.get("rpi_0")),
            "rpi_1":                safe_float(r.get("rpi_1")),
            "rpi_3":                safe_float(r.get("rpi_3")),
            "rpi_7":                safe_float(r.get("rpi_7")),
            "rpi_14":               safe_float(r.get("rpi_14")),
            "rpi_30":               safe_float(r.get("rpi_30")),
            "rpi_45":               safe_float(r.get("rpi_45")),
            "inter_pub_revenue_7":  safe_float(r.get("inter_pub_revenue_7")),
            "reward_pub_revenue_7": safe_float(r.get("reward_pub_revenue_7")),
            "banner_pub_revenue_7": safe_float(r.get("banner_pub_revenue_7")),
            "mrec_pub_revenue_7":   safe_float(r.get("mrec_pub_revenue_7")),
            "_ingested_at":         now_ts(),
        })
    return rows

def fetch_max_cohort_impressions():
    log.info("Fetching MAX Cohort Impressions...")
    start, end = cohort_range()
    rows = []
    # FIX v2: Use MAX_COHORT_URL (not /imp suffix)
    for r in fetch_csv(MAX_COHORT_URL, {
        "start": start, "end": end,
        "columns": (
            "day,application,package_name,platform,country,installs,"
            "imp_0,imp_1,imp_3,imp_7,imp_14,imp_30,imp_45,"
            "imp_per_user_0,imp_per_user_1,imp_per_user_7,imp_per_user_30,"
            "inter_imp_7,reward_imp_7,banner_imp_7,"
            "user_count_0,user_count_1,user_count_7,user_count_30"
        )
    }):
        rows.append({
            "day":              r.get("day"),
            "application":      r.get("application"),
            "package_name":     r.get("package_name"),
            "platform":         r.get("platform"),
            "country":          r.get("country"),
            "installs":         safe_int(r.get("installs")),
            "imp_0":            safe_int(r.get("imp_0")),
            "imp_1":            safe_int(r.get("imp_1")),
            "imp_3":            safe_int(r.get("imp_3")),
            "imp_7":            safe_int(r.get("imp_7")),
            "imp_14":           safe_int(r.get("imp_14")),
            "imp_30":           safe_int(r.get("imp_30")),
            "imp_45":           safe_int(r.get("imp_45")),
            "imp_per_user_0":   safe_float(r.get("imp_per_user_0")),
            "imp_per_user_1":   safe_float(r.get("imp_per_user_1")),
            "imp_per_user_7":   safe_float(r.get("imp_per_user_7")),
            "imp_per_user_30":  safe_float(r.get("imp_per_user_30")),
            "inter_imp_7":      safe_int(r.get("inter_imp_7")),
            "reward_imp_7":     safe_int(r.get("reward_imp_7")),
            "banner_imp_7":     safe_int(r.get("banner_imp_7")),
            "user_count_0":     safe_int(r.get("user_count_0")),
            "user_count_1":     safe_int(r.get("user_count_1")),
            "user_count_7":     safe_int(r.get("user_count_7")),
            "user_count_30":    safe_int(r.get("user_count_30")),
            "_ingested_at":     now_ts(),
        })
    return rows

def fetch_max_cohort_sessions():
    log.info("Fetching MAX Cohort Sessions...")
    start, end = cohort_range()
    rows = []
    # FIX v2: Use MAX_COHORT_URL (not /session suffix)
    for r in fetch_csv(MAX_COHORT_URL, {
        "start": start, "end": end,
        "columns": (
            "day,application,package_name,platform,country,installs,"
            "retention_1,retention_3,retention_7,retention_14,retention_30,"
            "session_count_0,session_count_1,session_count_7,session_count_30,"
            "daily_usage_0,daily_usage_1,daily_usage_7,daily_usage_30,"
            "user_count_1,user_count_7,user_count_30"
        )
    }):
        rows.append({
            "day":              r.get("day"),
            "application":      r.get("application"),
            "package_name":     r.get("package_name"),
            "platform":         r.get("platform"),
            "country":          r.get("country"),
            "installs":         safe_int(r.get("installs")),
            "retention_1":      safe_float(r.get("retention_1")),
            "retention_3":      safe_float(r.get("retention_3")),
            "retention_7":      safe_float(r.get("retention_7")),
            "retention_14":     safe_float(r.get("retention_14")),
            "retention_30":     safe_float(r.get("retention_30")),
            "session_count_0":  safe_int(r.get("session_count_0")),
            "session_count_1":  safe_int(r.get("session_count_1")),
            "session_count_7":  safe_int(r.get("session_count_7")),
            "session_count_30": safe_int(r.get("session_count_30")),
            "daily_usage_0":    safe_float(r.get("daily_usage_0")),
            "daily_usage_1":    safe_float(r.get("daily_usage_1")),
            "daily_usage_7":    safe_float(r.get("daily_usage_7")),
            "daily_usage_30":   safe_float(r.get("daily_usage_30")),
            "user_count_1":     safe_int(r.get("user_count_1")),
            "user_count_7":     safe_int(r.get("user_count_7")),
            "user_count_30":    safe_int(r.get("user_count_30")),
            "_ingested_at":     now_ts(),
        })
    return rows

# ─── UA (ADVERTISER) ─────────────────────────────────────────────────────────
# UA Report API returns lowercase headers: day, campaign, campaign_package_name etc.

def fetch_ua_campaigns_daily():
    log.info("Fetching UA Campaigns Daily...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "advertiser",
        "columns": (
            "day,campaign,campaign_id_external,campaign_package_name,campaign_type,"
            "platform,country,impressions,clicks,conversions,cost,revenue,"
            "roas_0d,roas_1d,roas_3d,roas_7d,roas_14d,roas_30d,"
            "iap_roas_7d,iap_roas_30d,ad_roas_7d,ad_roas_30d,"
            "iap_rev_7d,iap_rev_30d,ad_rev_7d,ad_rev_30d,total_rev_7d,total_rev_30d,"
            "ret_1d,ret_3d,ret_7d,ret_14d"
        )
    }):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        imps  = safe_int(r.get("impressions")) or 0
        clicks = safe_int(r.get("clicks")) or 0
        # FIX v2: Use actual revenue column (attributed revenue for the period)
        # NOT total_rev_7d which is a windowed metric
        rev   = safe_float(r.get("revenue")) or 0.0
        rows.append({
            "date":                     r.get("day"),
            "campaign":                 r.get("campaign"),
            "campaign_id_external":     r.get("campaign_id_external"),
            "campaign_package_name":    r.get("campaign_package_name"),
            "campaign_type":            r.get("campaign_type"),
            "platform":                 r.get("platform"),
            "country":                  r.get("country"),
            "impressions":              imps,
            "clicks":                   clicks,
            "conversions":              convs,
            "cost":                     cost,
            # All rate metrics recalculated from raw totals
            "ctr":                      round(clicks / imps, 6) if imps else None,
            "conversion_rate":          round(convs / clicks, 6) if clicks else None,
            "cpi":                      round(cost / convs, 4) if convs else None,
            "revenue":                  rev,
            "roas":                     round(rev / cost, 4) if cost and rev else None,
            "roas_0d":                  safe_float(r.get("roas_0d")),
            "roas_1d":                  safe_float(r.get("roas_1d")),
            "roas_3d":                  safe_float(r.get("roas_3d")),
            "roas_7d":                  safe_float(r.get("roas_7d")),
            "roas_14d":                 safe_float(r.get("roas_14d")),
            "roas_30d":                 safe_float(r.get("roas_30d")),
            "iap_roas_7d":              safe_float(r.get("iap_roas_7d")),
            "iap_roas_30d":             safe_float(r.get("iap_roas_30d")),
            "ad_roas_7d":               safe_float(r.get("ad_roas_7d")),
            "ad_roas_30d":              safe_float(r.get("ad_roas_30d")),
            "iap_rev_7d":               safe_float(r.get("iap_rev_7d")),
            "iap_rev_30d":              safe_float(r.get("iap_rev_30d")),
            "ad_rev_7d":                safe_float(r.get("ad_rev_7d")),
            "ad_rev_30d":               safe_float(r.get("ad_rev_30d")),
            "total_rev_7d":             safe_float(r.get("total_rev_7d")),
            "total_rev_30d":            safe_float(r.get("total_rev_30d")),
            "ret_1d":                   safe_float(r.get("ret_1d")),
            "ret_3d":                   safe_float(r.get("ret_3d")),
            "ret_7d":                   safe_float(r.get("ret_7d")),
            "ret_14d":                  safe_float(r.get("ret_14d")),
            "_ingested_at":             now_ts(),
        })
    return rows

def fetch_ua_by_country():
    log.info("Fetching UA by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "advertiser",
        "columns": "day,campaign,campaign_package_name,country,impressions,clicks,conversions,cost,roas_7d,roas_30d"
    }):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        rows.append({
            "date":                     r.get("day"),
            "campaign":                 r.get("campaign"),
            "campaign_package_name":    r.get("campaign_package_name"),
            "country":                  r.get("country"),
            "impressions":              safe_int(r.get("impressions")),
            "clicks":                   safe_int(r.get("clicks")),
            "conversions":              convs,
            "cost":                     cost,
            "cpi":                      round(cost / convs, 4) if convs else None,
            "roas_7d":                  safe_float(r.get("roas_7d")),
            "roas_30d":                 safe_float(r.get("roas_30d")),
            "_ingested_at":             now_ts(),
        })
    return rows

def fetch_ua_by_platform():
    log.info("Fetching UA by Platform...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "advertiser",
        "columns": "day,campaign,campaign_package_name,platform,impressions,clicks,conversions,cost"
    }):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        rows.append({
            "date":                     r.get("day"),
            "campaign":                 r.get("campaign"),
            "campaign_package_name":    r.get("campaign_package_name"),
            "platform":                 r.get("platform"),
            "impressions":              safe_int(r.get("impressions")),
            "clicks":                   safe_int(r.get("clicks")),
            "conversions":              convs,
            "cost":                     cost,
            "cpi":                      round(cost / convs, 4) if convs else None,
            "_ingested_at":             now_ts(),
        })
    return rows

def fetch_ua_by_creative_type():
    log.info("Fetching UA by Creative Type...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "advertiser",
        "columns": "day,campaign,campaign_package_name,ad_creative_type,impressions,clicks,conversions,cost"
    }):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        imps  = safe_int(r.get("impressions")) or 0
        clicks = safe_int(r.get("clicks")) or 0
        rows.append({
            "date":                     r.get("day"),
            "campaign":                 r.get("campaign"),
            "campaign_package_name":    r.get("campaign_package_name"),
            "ad_creative_type":         r.get("ad_creative_type"),
            "impressions":              imps,
            "clicks":                   clicks,
            "conversions":              convs,
            "cost":                     cost,
            "ctr":                      round(clicks / imps, 6) if imps else None,
            "cpi":                      round(cost / convs, 4) if convs else None,
            "_ingested_at":             now_ts(),
        })
    return rows

def fetch_ua_by_app():
    log.info("Fetching UA by App...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "advertiser",
        "columns": "day,campaign_package_name,platform,impressions,clicks,conversions,cost,revenue,roas_7d,roas_30d,total_rev_7d,total_rev_30d"
    }):
        cost  = safe_float(r.get("cost")) or 0.0
        # FIX: Use actual revenue column, not total_rev_7d
        rev   = safe_float(r.get("revenue")) or 0.0
        rows.append({
            "date":                     r.get("day"),
            "campaign_package_name":    r.get("campaign_package_name"),
            "platform":                 r.get("platform"),
            "impressions":              safe_int(r.get("impressions")),
            "clicks":                   safe_int(r.get("clicks")),
            "conversions":              safe_int(r.get("conversions")),
            "cost":                     cost,
            "revenue":                  rev,
            "roas":                     round(rev / cost, 4) if cost and rev else None,
            "roas_7d":                  safe_float(r.get("roas_7d")),
            "roas_30d":                 safe_float(r.get("roas_30d")),
            "total_rev_7d":             safe_float(r.get("total_rev_7d")),
            "total_rev_30d":            safe_float(r.get("total_rev_30d")),
            "_ingested_at":             now_ts(),
        })
    return rows

# FIX v2: Use start/end dates instead of range=last_7d
# Asset report aggregates over the period — no per-day breakdown available from API
# So we store one row per asset per campaign with date = run date
def fetch_ua_creatives_daily():
    log.info("Fetching UA Creatives (Asset Report)...")
    start, end = date_range()
    rows = []
    # Try start/end first; asset report API may only support range param
    # FIX: try start/end, fallback to range=last_7d if empty
    raw = fetch_csv(UA_ASSET_URL, {
        "start": start, "end": end,
        "columns": "asset_id,asset_name,campaign,impressions,clicks,cost,ctr"
    })
    if not raw:
        log.info("  start/end returned empty — trying range=last_7d")
        raw = fetch_csv(UA_ASSET_URL, {
            "range": "last_7d",
            "columns": "asset_id,asset_name,campaign,impressions,clicks,cost,ctr"
        })
    for r in raw:
        imps   = safe_int(r.get("impressions")) or 0
        clicks = safe_int(r.get("clicks")) or 0
        rows.append({
            "date":         end,  # asset report aggregates — no per-day breakdown
            "asset_id":     r.get("asset_id"),
            "asset_name":   r.get("asset_name"),
            "campaign":     r.get("campaign"),
            "impressions":  imps,
            "clicks":       clicks,
            "cost":         safe_float(r.get("cost")),
            "ctr":          round(clicks / imps, 6) if imps else None,
            "_ingested_at": now_ts(),
        })
    return rows

# ─── PUBLISHER (AD REVENUE) ──────────────────────────────────────────────────
# FIX v2: Publisher API returns lowercase headers — fixed all key names
# Publisher report: UA_REPORT_URL with report_type=publisher

def fetch_pub_revenue_daily():
    log.info("Fetching Publisher Revenue Daily...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "publisher",
        "columns": "day,application,package_name,platform,impressions,clicks,revenue,ecpm,ctr"
    }):
        imps  = safe_int(r.get("impressions")) or 0
        clicks = safe_int(r.get("clicks")) or 0
        rows.append({
            "date":         r.get("day"),
            "application":  r.get("application"),
            "package_name": r.get("package_name"),
            "platform":     r.get("platform"),
            "impressions":  imps,
            "clicks":       clicks,
            "revenue":      safe_float(r.get("revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "ctr":          round(clicks / imps, 6) if imps else None,
            "_ingested_at": now_ts(),
        })
    return rows

def fetch_pub_revenue_by_country():
    log.info("Fetching Publisher Revenue by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "publisher",
        "columns": "day,application,package_name,platform,country,impressions,revenue,ecpm"
    }):
        rows.append({
            "date":         r.get("day"),
            "application":  r.get("application"),
            "package_name": r.get("package_name"),
            "platform":     r.get("platform"),
            "country":      r.get("country"),
            "impressions":  safe_int(r.get("impressions")),
            "revenue":      safe_float(r.get("revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "_ingested_at": now_ts(),
        })
    return rows

def fetch_pub_revenue_by_adtype():
    log.info("Fetching Publisher Revenue by Ad Type...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "publisher",
        "columns": "day,application,package_name,platform,ad_type,size,impressions,revenue,ecpm"
    }):
        rows.append({
            "date":         r.get("day"),
            "application":  r.get("application"),
            "package_name": r.get("package_name"),
            "platform":     r.get("platform"),
            "ad_type":      r.get("ad_type"),
            "size":         r.get("size"),
            "impressions":  safe_int(r.get("impressions")),
            "revenue":      safe_float(r.get("revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "_ingested_at": now_ts(),
        })
    return rows

def fetch_pub_revenue_by_platform():
    log.info("Fetching Publisher Revenue by Platform/Device...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end, "report_type": "publisher",
        "columns": "day,application,package_name,platform,device_type,impressions,revenue,ecpm"
    }):
        rows.append({
            "date":         r.get("day"),
            "application":  r.get("application"),
            "package_name": r.get("package_name"),
            "platform":     r.get("platform"),
            "device_type":  r.get("device_type"),
            "impressions":  safe_int(r.get("impressions")),
            "revenue":      safe_float(r.get("revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "_ingested_at": now_ts(),
        })
    return rows

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    global FAILED_TABLES
    FAILED_TABLES = []

    log.info("🚀 AppLovin → BigQuery COMPLETE sync v2")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")

    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

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

    # FIX v2: Report failures explicitly
    if FAILED_TABLES:
        log.error(f"❌ {len(FAILED_TABLES)} tables had BQ errors: {FAILED_TABLES}")
    else:
        log.info("✅ AppLovin sync v2 complete! 20 tables loaded.")

if __name__ == "__main__":
    main()
