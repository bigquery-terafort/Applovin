"""
Debug script - prints exact column names returned by AppLovin API
Run this BEFORE the main script to verify column names
"""
import os, csv, io, requests

APPLOVIN_REPORT_KEY = os.environ["APPLOVIN_REPORT_KEY"]
MAX_REPORT_URL  = "https://r.applovin.com/maxReport"
MAX_COHORT_URL  = "https://r.applovin.com/maxCohort"
UA_REPORT_URL   = "https://r.applovin.com/report"
UA_ASSET_URL    = "https://r.applovin.com/assetReport"
MAX_USER_URL    = "https://r.applovin.com/max/userAdRevenueReport"

from datetime import datetime, timedelta
yesterday = str(datetime.utcnow().date() - timedelta(days=1))
start     = str(datetime.utcnow().date() - timedelta(days=3))
end       = str(datetime.utcnow().date())
cohort_start = str(datetime.utcnow().date() - timedelta(days=7))

def check(name, url, params):
    params["api_key"] = APPLOVIN_REPORT_KEY
    params["format"]  = "csv"
    resp = requests.get(url, params=params, timeout=60)
    if resp.status_code != 200:
        print(f"\n❌ {name}: HTTP {resp.status_code} — {resp.text[:200]}")
        return
    text = resp.text.strip()
    if not text or text.lower().startswith("error"):
        print(f"\n❌ {name}: {text[:200]}")
        return
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        print(f"\n⚠️  {name}: 0 rows returned")
        return
    print(f"\n✅ {name} ({len(rows)} rows)")
    print(f"   Exact column names: {list(rows[0].keys())}")
    print(f"   Sample row: {dict(list(rows[0].items())[:5])}")

print("=" * 70)
print("AppLovin API Column Name Debug")
print("=" * 70)

check("MAX Revenue Daily", MAX_REPORT_URL, {
    "start": start, "end": end,
    "columns": "day,application,package_name,platform,impressions,estimated_revenue,ecpm,requests"
})

check("MAX Revenue by Country", MAX_REPORT_URL, {
    "start": start, "end": end,
    "columns": "day,application,package_name,platform,country,impressions,estimated_revenue,ecpm"
})

check("MAX Revenue by Network", MAX_REPORT_URL, {
    "start": start, "end": end,
    "columns": "day,application,package_name,platform,network,impressions,estimated_revenue,ecpm"
})

check("MAX Revenue by Format", MAX_REPORT_URL, {
    "start": start, "end": end,
    "columns": "day,application,package_name,platform,ad_format,impressions,estimated_revenue,ecpm"
})

check("MAX Revenue by Device", MAX_REPORT_URL, {
    "start": start, "end": end,
    "columns": "day,application,package_name,platform,device_type,impressions,estimated_revenue,ecpm"
})

check("MAX Cohort Revenue", MAX_COHORT_URL, {
    "start": cohort_start, "end": end,
    "columns": "day,application,package_name,platform,country,installs,pub_revenue_0,pub_revenue_7,rpi_7"
})

check("MAX Cohort Impressions", f"{MAX_COHORT_URL}/imp", {
    "start": cohort_start, "end": end,
    "columns": "day,application,package_name,platform,country,installs,imp_7,user_count_7"
})

check("MAX Cohort Sessions", f"{MAX_COHORT_URL}/session", {
    "start": cohort_start, "end": end,
    "columns": "day,application,package_name,platform,country,installs,retention_7,session_count_7,daily_usage_7"
})

check("UA Campaigns", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "advertiser",
    "columns": "day,campaign,campaign_id_external,campaign_package_name,campaign_type,platform,country,impressions,clicks,conversions,cost,ctr,roas_7d,roas_30d,total_rev_7d,ret_7d"
})

check("UA by Country", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "advertiser",
    "columns": "day,campaign,campaign_package_name,country,impressions,clicks,conversions,cost,roas_7d"
})

check("UA by Platform", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "advertiser",
    "columns": "day,campaign,campaign_package_name,platform,impressions,clicks,conversions,cost"
})

check("UA by Creative Type", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "advertiser",
    "columns": "day,campaign,campaign_package_name,ad_creative_type,impressions,clicks,conversions,cost,ctr"
})

check("UA by App", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "advertiser",
    "columns": "day,campaign_package_name,platform,impressions,clicks,conversions,cost,roas_7d,total_rev_7d"
})

check("UA Asset Report", UA_ASSET_URL, {
    "range": "last_7d",
    "columns": "asset_id,asset_name,campaign,impressions,clicks,cost,ctr"
})

check("Publisher Revenue Daily", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "publisher",
    "columns": "day,application,package_name,platform,impressions,clicks,revenue,ecpm,ctr"
})

check("Publisher Revenue by Country", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "publisher",
    "columns": "day,application,package_name,platform,country,impressions,revenue,ecpm"
})

check("Publisher Revenue by Ad Type", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "publisher",
    "columns": "day,application,package_name,platform,ad_type,size,impressions,revenue,ecpm"
})

check("Publisher Revenue by Platform", UA_REPORT_URL, {
    "start": start, "end": end, "report_type": "publisher",
    "columns": "day,application,package_name,platform,device_type,impressions,revenue,ecpm"
})

check("MAX User Revenue (android)", MAX_USER_URL, {
    "date": yesterday, "platform": "android",
    "columns": "application,package_name,country,network,ad_format,impressions,estimated_revenue"
})

print("\n" + "=" * 70)
print("Done! Check exact column names above and compare to script.")
print("=" * 70)
