#!/usr/bin/env python3
"""
compute_breadth.py — Generate breadth_baseline.json for Trend M.A.P.

Fetches historical Nasdaq-100 advance/decline data via Alpaca Markets API,
computes McClellan Oscillator and Summation Index, and writes a JSON baseline
file used by the standalone Trend MAP page.

Environment variables required:
    ALPACA_KEY_ID       — Alpaca API Key ID
    ALPACA_SECRET_KEY   — Alpaca Secret Key

Usage:
    python compute_breadth.py

Output:
    breadth_baseline.json (written to current directory)

Designed to run nightly via GitHub Actions (see .github/workflows/breadth-update.yml).
"""

import os
import sys
import json
import math
import time
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode


# =============================================================================
# CONFIGURATION
# =============================================================================

ALPACA_BASE = "https://data.alpaca.markets/v2"
LOOKBACK_YEARS = 3          # ~700+ trading days
BATCH_SIZE = 50             # Alpaca max symbols per request
AD_HISTORY_DAYS = 60        # Recent A/D entries to include for incremental updates
OUTPUT_FILE = "breadth_baseline.json"

# Fallback Nasdaq-100 components (used if SlickCharts scraping fails)
FALLBACK_COMPONENTS = [
    'AAPL','ABNB','ADBE','ADI','ADP','ADSK','AEP','AMAT','AMGN','AMZN',
    'ANSS','APP','ARM','ASML','AVGO','AZN','BIIB','BKNG','BKR','CCEP',
    'CDNS','CDW','CEG','CHTR','CMCSA','COST','CPRT','CRWD','CSCO','CSGP',
    'CSX','CTAS','CTSH','DASH','DDOG','DLTR','DXCM','EA','EXC','FANG',
    'FAST','FTNT','GEHC','GFS','GILD','GOOG','GOOGL','HON','IDXX','INTC',
    'INTU','ISRG','KDP','KHC','KLAC','LIN','LRCX','LULU','MAR','MCHP',
    'MDB','MDLZ','MELI','META','MNST','MRVL','MSFT','MU','NFLX','NVDA',
    'NXPI','ODFL','ON','ORLY','PCAR','PAYX','PANW','PDD','PEP','PLTR',
    'PYPL','QCOM','REGN','ROP','ROST','SBUX','SHOP','SMCI','SNPS','SPX',
    'TEAM','TMUS','TRI','TSLA','TTD','TTWO','TXN','VRSK','VRTX','WBD','XEL'
]


# =============================================================================
# HELPERS
# =============================================================================

def alpaca_get(path, params=None, api_key=None, api_secret=None):
    """Make an authenticated GET request to Alpaca."""
    url = f"{ALPACA_BASE}{path}"
    if params:
        url += "?" + urlencode(params)
    req = Request(url, headers={
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Accept": "application/json"
    })
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        if e.code == 403:
            # Retry with IEX feed
            if "feed=" not in url:
                sep = "&" if "?" in url else "?"
                url_iex = url + sep + "feed=iex"
                req_iex = Request(url_iex, headers={
                    "APCA-API-KEY-ID": api_key,
                    "APCA-API-SECRET-KEY": api_secret,
                    "Accept": "application/json"
                })
                with urlopen(req_iex) as resp:
                    return json.loads(resp.read().decode())
        raise


def alpaca_get_with_retry(path, params=None, api_key=None, api_secret=None,
                          max_attempts=4, base_delay=5):
    """alpaca_get with exponential backoff retry for transient failures.

    Retries on network errors and 5xx / 429 HTTP errors.
    Raises immediately on 4xx errors (except 403, handled inside alpaca_get).
    Delays: 5s, 10s, 20s (doubles each attempt).
    """
    delay = base_delay
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return alpaca_get(path, params=params, api_key=api_key, api_secret=api_secret)
        except HTTPError as e:
            last_exc = e
            if e.code in (429, 500, 502, 503, 504):
                print(f"    HTTP {e.code} on attempt {attempt}/{max_attempts} — retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
            else:
                raise  # Non-retryable HTTP error — propagate immediately
        except (URLError, OSError) as e:
            last_exc = e
            print(f"    Network error on attempt {attempt}/{max_attempts}: {e} — retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2
    print(f"    All {max_attempts} attempts failed. Last error: {last_exc}")
    raise last_exc


def is_weekend(date_str):
    """Check if a YYYY-MM-DD date string falls on a weekend."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return d.weekday() >= 5


def fetch_nasdaq100_components():
    """Try to scrape current Nasdaq-100 from SlickCharts, fall back to hardcoded list."""
    try:
        import re
        req = Request(
            "https://www.slickcharts.com/nasdaq100",
            headers={"User-Agent": "Mozilla/5.0 (compatible; TrendMAP/1.0)"}
        )
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode()
        # Extract ticker symbols from table
        symbols = re.findall(r'/symbol/([A-Z]+)"', html)
        symbols = list(dict.fromkeys(symbols))  # Deduplicate, preserve order
        if len(symbols) >= 80:
            print(f"  Scraped {len(symbols)} components from SlickCharts")
            return symbols
    except Exception as e:
        print(f"  SlickCharts scrape failed: {e}")

    print(f"  Using fallback component list ({len(FALLBACK_COMPONENTS)} symbols)")
    return FALLBACK_COMPONENTS[:]


# =============================================================================
# MAIN
# =============================================================================

def main():
    api_key = os.environ.get("ALPACA_KEY_ID", "").strip()
    api_secret = os.environ.get("ALPACA_SECRET_KEY", "").strip()

    if not api_key or not api_secret:
        print("ERROR: ALPACA_KEY_ID and ALPACA_SECRET_KEY environment variables required.")
        sys.exit(1)

    print("=" * 60)
    print("Trend M.A.P. — Breadth Baseline Generator")
    print("=" * 60)

    # --- Step 1: Get Nasdaq-100 components ---
    print("\n[1/4] Fetching Nasdaq-100 components...")
    components = fetch_nasdaq100_components()
    component_source = "slickcharts" if len(components) != len(FALLBACK_COMPONENTS) else "fallback"

    # Add benchmark ETFs for reference
    benchmarks = ["SPY", "QQQ", "DIA"]
    all_symbols = components + [b for b in benchmarks if b not in components]

    # --- Step 2: Fetch historical daily bars for all symbols ---
    print(f"\n[2/4] Fetching {LOOKBACK_YEARS}y daily bars for {len(all_symbols)} symbols...")

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=LOOKBACK_YEARS * 365)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # Collect all bars per symbol
    all_bars = {}
    for i in range(0, len(all_symbols), BATCH_SIZE):
        batch = all_symbols[i:i + BATCH_SIZE]
        print(f"  Batch {i // BATCH_SIZE + 1}: {batch[0]}...{batch[-1]} ({len(batch)} symbols)")

        page_token = None
        pages = 0
        while True:
            params = {
                "symbols": ",".join(batch),
                "timeframe": "1Day",
                "start": start_str,
                "end": end_str,
                "limit": "10000",
                "adjustment": "split",
                "sort": "asc"
            }
            if page_token:
                params["page_token"] = page_token

            try:
                data = alpaca_get_with_retry("/stocks/bars", params, api_key, api_secret)
            except Exception as e:
                print(f"    ERROR fetching batch (all retries exhausted): {e}")
                break

            if data.get("bars"):
                for sym, bars in data["bars"].items():
                    if sym not in all_bars:
                        all_bars[sym] = []
                    all_bars[sym].extend(bars)

            page_token = data.get("next_page_token")
            pages += 1
            if not page_token or pages >= 20:
                break

        time.sleep(0.3)  # Rate limiting

    print(f"  Retrieved bars for {len(all_bars)} symbols")

    # --- Step 3: Build daily A/D series ---
    print("\n[3/4] Computing daily advance/decline series...")

    # Collect all unique trading dates across all symbols
    all_dates = set()
    symbol_closes = {}
    for sym, bars in all_bars.items():
        closes = {}
        for bar in bars:
            date_str = bar["t"][:10]  # Extract YYYY-MM-DD from timestamp
            if not is_weekend(date_str):
                closes[date_str] = bar["c"]
        symbol_closes[sym] = closes
        all_dates.update(closes.keys())

    # Sort dates
    sorted_dates = sorted(all_dates)
    print(f"  Found {len(sorted_dates)} trading days from {sorted_dates[0]} to {sorted_dates[-1]}")

    # For each trading day, count advancers and decliners among components
    daily_series = []
    k19 = 2.0 / 20.0   # 19-day EMA smoothing factor
    k39 = 2.0 / 40.0   # 39-day EMA smoothing factor
    ema19 = None
    ema39 = None
    summation_index = 0.0
    oscillator = 0.0

    for idx, date in enumerate(sorted_dates):
        advances = 0
        declines = 0

        for sym in components:
            closes = symbol_closes.get(sym, {})
            today_close = closes.get(date)
            if today_close is None:
                continue

            # Find previous trading day's close for this symbol
            prev_close = None
            for prev_idx in range(idx - 1, max(idx - 5, -1), -1):
                prev_date = sorted_dates[prev_idx]
                if prev_date in closes:
                    prev_close = closes[prev_date]
                    break

            if prev_close is None or prev_close <= 0:
                continue

            if today_close > prev_close:
                advances += 1
            elif today_close < prev_close:
                declines += 1

        net_ad = advances - declines

        # McClellan Oscillator calculation
        if ema19 is None:
            ema19 = float(net_ad)
            ema39 = float(net_ad)
        else:
            ema19 = (net_ad * k19) + (ema19 * (1.0 - k19))
            ema39 = (net_ad * k39) + (ema39 * (1.0 - k39))

        oscillator = ema19 - ema39
        summation_index += oscillator

        daily_series.append({
            "date": date,
            "advances": advances,
            "declines": declines,
            "netAD": net_ad,
            "ema19": round(ema19, 4),
            "ema39": round(ema39, 4),
            "oscillator": round(oscillator, 4),
            "summationIndex": round(summation_index, 4)
        })

    print(f"  Computed {len(daily_series)} daily entries")
    if daily_series:
        last = daily_series[-1]
        print(f"  Latest: {last['date']} — MCO: {last['oscillator']:.4f}, MCSI: {last['summationIndex']:.4f}")

    # --- Step 4: Build output JSON ---
    print(f"\n[4/4] Writing {OUTPUT_FILE}...")

    last_entry = daily_series[-1] if daily_series else None

    # Recent A/D history (last N trading days) for incremental updates
    ad_history = []
    for entry in daily_series[-AD_HISTORY_DAYS:]:
        ad_history.append({
            "date": entry["date"],
            "advances": entry["advances"],
            "declines": entry["declines"],
            "netAD": entry["netAD"]
        })

    output = {
        "version": 2,
        "generated": datetime.now(timezone.utc).isoformat(),
        "startDate": daily_series[0]["date"] if daily_series else None,
        "endDate": last_entry["date"] if last_entry else None,
        "tradingDays": len(daily_series),
        "components": all_symbols,
        "componentCount": len(all_symbols),
        "componentSource": component_source,
        "lastState": {
            "date": last_entry["date"],
            "ema19": round(last_entry["ema19"], 6),
            "ema39": round(last_entry["ema39"], 6),
            "oscillator": round(last_entry["oscillator"], 4),
            "summationIndex": round(last_entry["summationIndex"], 4)
        } if last_entry else None,
        "dailySeries": daily_series,
        "adHistory": ad_history
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"  Written: {OUTPUT_FILE} ({file_size:,} bytes)")
    print(f"\nDone! {len(daily_series)} trading days processed.")


if __name__ == "__main__":
    main()
