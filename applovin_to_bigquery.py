"""
AppLovin → BigQuery  ·  FULL DATA PIPELINE (FIXED COLUMNS)
============================================================
Uses exact column names from AppLovin API documentation.
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

SCHEMAS = {
    # ── MAX Revenue Daily ─────────────────────────────────────────────────────
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
    # ── MAX Revenue by Country ────────────────────────────────────────────────
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
    # ── MAX Revenue by Network ────────────────────────────────────────────────
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
    # ── MAX Revenue by Ad Format ──────────────────────────────────────────────
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
    # ── MAX User Revenue ──────────────────────────────────────────────────────
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
    # ── MAX Cohort Revenue ────────────────────────────────────────────────────
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
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    # ── UA Campaigns Daily ────────────────────────────────────────────────────
    "ua_campaigns_daily": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign",            "STRING"),
        bigquery.SchemaField("application",         "STRING"),
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
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    # ── UA Creatives / Assets Daily ───────────────────────────────────────────
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
    # ── UA by Country ─────────────────────────────────────────────────────────
    "ua_by_country": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("campaign",            "STRING"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("country",             "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("conversions",         "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("cpi",                 "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
    # ── UA by App ─────────────────────────────────────────────────────────────
    "ua_by_app": [
        bigquery.SchemaField("date",                "DATE"),
        bigquery.SchemaField("application",         "STRING"),
        bigquery.SchemaField("platform",            "STRING"),
        bigquery.SchemaField("impressions",         "INTEGER"),
        bigquery.SchemaField("clicks",              "INTEGER"),
        bigquery.SchemaField("conversions",         "INTEGER"),
        bigquery.SchemaField("cost",                "FLOAT"),
        bigquery.SchemaField("revenue",             "FLOAT"),
        bigquery.SchemaField("roas",                "FLOAT"),
        bigquery.SchemaField("_ingested_at",        "TIMESTAMP"),
    ],
}

def safe_float(v):
    try: return float(v) if v not in (None,"","N/A") else None
    except: return None

def safe_int(v):
    try: return int(float(v)) if v not in (None,"","N/A") else None
    except: return None

def now_ts(): return datetime.utcnow().isoformat()

def date_range():
    end = datetime.utcnow().date()
    return str(end - timedelta(days=LOOKBACK_DAYS)), str(end)

def fetch_csv(url, params):
    params["api_key"] = APPLOVIN_REPORT_KEY
    params["format"]  = "csv"
    log.info(f"  GET {url} columns={params.get('columns','')[:80]}")
    resp = requests.get(url, params=params, timeout=120)
    if resp.status_code != 200:
        log.warning(f"  API error {resp.status_code}: {resp.text[:300]}")
        return []
    text = resp.text.strip()
    if not text or text.lower().startswith("error"):
        log.warning(f"  Empty/error: {text[:200]}")
        return []
    return list(csv.DictReader(io.StringIO(text)))

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
    if errors: log.error(f"BQ errors [{name}]: {errors[:2]}")
    else: log.info(f"  ✅ {len(rows):,} rows → {name}")

# ─── 1. MAX REVENUE DAILY ─────────────────────────────────────────────────────
def fetch_max_revenue_daily():
    log.info("Fetching MAX Revenue Daily...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,impressions,estimated_revenue,ecpm,requests",
    }):
        rows.append({"date":r.get("day"),"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "impressions":safe_int(r.get("impressions")),
            "estimated_revenue":safe_float(r.get("estimated_revenue")),
            "ecpm":safe_float(r.get("ecpm")),"requests":safe_int(r.get("requests")),
            "_ingested_at":now_ts()})
    return rows

# ─── 2. MAX REVENUE BY COUNTRY ────────────────────────────────────────────────
def fetch_max_revenue_by_country():
    log.info("Fetching MAX Revenue by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,country,impressions,estimated_revenue,ecpm",
    }):
        rows.append({"date":r.get("day"),"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "country":r.get("country"),"impressions":safe_int(r.get("impressions")),
            "estimated_revenue":safe_float(r.get("estimated_revenue")),
            "ecpm":safe_float(r.get("ecpm")),"_ingested_at":now_ts()})
    return rows

# ─── 3. MAX REVENUE BY NETWORK ────────────────────────────────────────────────
def fetch_max_revenue_by_network():
    log.info("Fetching MAX Revenue by Network...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,network,impressions,estimated_revenue,ecpm",
    }):
        rows.append({"date":r.get("day"),"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "network":r.get("network"),"impressions":safe_int(r.get("impressions")),
            "estimated_revenue":safe_float(r.get("estimated_revenue")),
            "ecpm":safe_float(r.get("ecpm")),"_ingested_at":now_ts()})
    return rows

# ─── 4. MAX REVENUE BY FORMAT ─────────────────────────────────────────────────
def fetch_max_revenue_by_format():
    log.info("Fetching MAX Revenue by Format...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(MAX_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,package_name,platform,ad_format,impressions,estimated_revenue,ecpm",
    }):
        rows.append({"date":r.get("day"),"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "ad_format":r.get("ad_format"),"impressions":safe_int(r.get("impressions")),
            "estimated_revenue":safe_float(r.get("estimated_revenue")),
            "ecpm":safe_float(r.get("ecpm")),"_ingested_at":now_ts()})
    return rows

# ─── 5. MAX USER REVENUE (yesterday only — large data) ────────────────────────
def fetch_max_user_revenue():
    log.info("Fetching MAX User Revenue...")
    yesterday = str(datetime.utcnow().date() - timedelta(days=1))
    rows = []
    for r in fetch_csv(MAX_USER_URL, {
        "date": yesterday,
        "columns": "application,package_name,platform,country,network,ad_format,impressions,estimated_revenue",
    }):
        rows.append({"date":yesterday,"application":r.get("application"),
            "package_name":r.get("package_name"),"platform":r.get("platform"),
            "country":r.get("country"),"network":r.get("network"),
            "ad_format":r.get("ad_format"),"impressions":safe_int(r.get("impressions")),
            "estimated_revenue":safe_float(r.get("estimated_revenue")),
            "_ingested_at":now_ts()})
    return rows

# ─── 6. MAX COHORT REVENUE ────────────────────────────────────────────────────
def fetch_max_cohort_revenue():
    log.info("Fetching MAX Cohort Revenue...")
    end   = datetime.utcnow().date()
    start = end - timedelta(days=44)  # max 45 day window
    rows = []
    for r in fetch_csv(MAX_COHORT_URL, {
        "start": str(start), "end": str(end),
        "columns": "day,application,package_name,platform,country,installs,"
                   "pub_revenue_0,pub_revenue_1,pub_revenue_3,pub_revenue_7,"
                   "pub_revenue_14,pub_revenue_30,pub_revenue_45,"
                   "rpi_0,rpi_1,rpi_3,rpi_7,rpi_14,rpi_30,rpi_45",
    }):
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
            "rpi_45":safe_float(r.get("rpi_45")),"_ingested_at":now_ts()})
    return rows

# ─── 7. UA CAMPAIGNS DAILY ────────────────────────────────────────────────────
def fetch_ua_campaigns_daily():
    log.info("Fetching UA Campaigns Daily...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,campaign,application,platform,country,impressions,clicks,conversions,cost,ctr,conversion_rate,revenue",
        "report_type": "advertiser",
    }):
        cost    = safe_float(r.get("cost")) or 0.0
        convs   = safe_int(r.get("conversions")) or 0
        revenue = safe_float(r.get("revenue")) or 0.0
        rows.append({"date":r.get("day"),"campaign":r.get("campaign"),
            "application":r.get("application"),"platform":r.get("platform"),
            "country":r.get("country"),"impressions":safe_int(r.get("impressions")),
            "clicks":safe_int(r.get("clicks")),"conversions":convs,"cost":cost,
            "ctr":safe_float(r.get("ctr")),
            "conversion_rate":safe_float(r.get("conversion_rate")),
            "cpi":round(cost/convs,4) if convs else None,"revenue":revenue,
            "roas":round(revenue/cost,4) if cost else None,
            "_ingested_at":now_ts()})
    return rows

# ─── 8. UA CREATIVES / ASSETS ─────────────────────────────────────────────────
def fetch_ua_creatives_daily():
    log.info("Fetching UA Creatives (Asset Report)...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_ASSET_URL, {
        "start": start, "end": end,
        "columns": "asset_id,asset_name,campaign,impressions,clicks,cost,ctr",
    }):
        rows.append({"date":start,  # asset report uses range not daily breakdown
            "asset_id":r.get("asset_id"),"asset_name":r.get("asset_name"),
            "campaign":r.get("campaign"),"impressions":safe_int(r.get("impressions")),
            "clicks":safe_int(r.get("clicks")),"cost":safe_float(r.get("cost")),
            "ctr":safe_float(r.get("ctr")),"_ingested_at":now_ts()})
    return rows

# ─── 9. UA BY COUNTRY ─────────────────────────────────────────────────────────
def fetch_ua_by_country():
    log.info("Fetching UA by Country...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,campaign,application,country,impressions,clicks,conversions,cost",
        "report_type": "advertiser",
    }):
        cost  = safe_float(r.get("cost")) or 0.0
        convs = safe_int(r.get("conversions")) or 0
        rows.append({"date":r.get("day"),"campaign":r.get("campaign"),
            "application":r.get("application"),"country":r.get("country"),
            "impressions":safe_int(r.get("impressions")),"clicks":safe_int(r.get("clicks")),
            "conversions":convs,"cost":cost,
            "cpi":round(cost/convs,4) if convs else None,
            "_ingested_at":now_ts()})
    return rows

# ─── 10. UA BY APP ────────────────────────────────────────────────────────────
def fetch_ua_by_app():
    log.info("Fetching UA by App...")
    start, end = date_range()
    rows = []
    for r in fetch_csv(UA_REPORT_URL, {
        "start": start, "end": end,
        "columns": "day,application,platform,impressions,clicks,conversions,cost,revenue",
        "report_type": "advertiser",
    }):
        cost    = safe_float(r.get("cost")) or 0.0
        revenue = safe_float(r.get("revenue")) or 0.0
        rows.append({"date":r.get("day"),"application":r.get("application"),
            "platform":r.get("platform"),"impressions":safe_int(r.get("impressions")),
            "clicks":safe_int(r.get("clicks")),"conversions":safe_int(r.get("conversions")),
            "cost":cost,"revenue":revenue,
            "roas":round(revenue/cost,4) if cost else None,
            "_ingested_at":now_ts()})
    return rows

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 AppLovin → BigQuery FULL sync")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")
    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS: ensure_table(bq, t)

    log.info("── MAX (Monetization) ──")
    load_to_bq(bq, "max_revenue_daily",      fetch_max_revenue_daily())
    load_to_bq(bq, "max_revenue_by_country", fetch_max_revenue_by_country())
    load_to_bq(bq, "max_revenue_by_network", fetch_max_revenue_by_network())
    load_to_bq(bq, "max_revenue_by_format",  fetch_max_revenue_by_format())
    load_to_bq(bq, "max_user_revenue",       fetch_max_user_revenue())
    load_to_bq(bq, "max_cohort_revenue",     fetch_max_cohort_revenue())

    log.info("── AppDiscovery (User Acquisition) ──")
    load_to_bq(bq, "ua_campaigns_daily",     fetch_ua_campaigns_daily())
    load_to_bq(bq, "ua_creatives_daily",     fetch_ua_creatives_daily())
    load_to_bq(bq, "ua_by_country",          fetch_ua_by_country())
    load_to_bq(bq, "ua_by_app",              fetch_ua_by_app())

    log.info("✅ Full AppLovin sync complete!")

if __name__ == "__main__":
    main()
