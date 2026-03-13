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
HL_HISTORY_DAYS = 60        # Recent 52-week H/L entries to include in baseline
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
                data = alpaca_get("/stocks/bars", params, api_key, api_secret)
            except Exception as e:
                print(f"    ERROR fetching batch: {e}")
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

    # --- Step 3: Build daily A/D and 52-week H/L series ---
    print("\n[3/4] Computing daily advance/decline and 52-week H/L series...")

    # Collect all unique trading dates across all symbols
    # Store closes, highs, and lows per symbol per date
    all_dates = set()
    symbol_closes = {}
    symbol_highs = {}
    symbol_lows = {}
    for sym, bars in all_bars.items():
        closes = {}
        highs = {}
        lows = {}
        for bar in bars:
            date_str = bar["t"][:10]  # Extract YYYY-MM-DD from timestamp
            if not is_weekend(date_str):
                closes[date_str] = bar["c"]
                highs[date_str] = bar["h"]
                lows[date_str] = bar["l"]
        symbol_closes[sym] = closes
        symbol_highs[sym] = highs
        symbol_lows[sym] = lows
        all_dates.update(closes.keys())

    # Sort dates
    sorted_dates = sorted(all_dates)
    print(f"  Found {len(sorted_dates)} trading days from {sorted_dates[0]} to {sorted_dates[-1]}")

    # For each trading day, count advancers, decliners, new 52-week highs/lows
    daily_series = []
    k19 = 2.0 / 20.0   # 19-day EMA smoothing factor
    k39 = 2.0 / 40.0   # 39-day EMA smoothing factor
    ema19 = None
    ema39 = None
    summation_index = 0.0
    oscillator = 0.0

    # ~252 trading days in a year; we look back up to this many days for 52-week H/L
    LOOKBACK_52W = 252

    for idx, date in enumerate(sorted_dates):
        advances = 0
        declines = 0
        new_highs = 0
        new_lows = 0

        # The 52-week lookback window: all trading dates BEFORE today within ~252 days
        lookback_start = max(0, idx - LOOKBACK_52W)
        prior_dates = sorted_dates[lookback_start:idx]  # excludes today

        for sym in components:
            closes = symbol_closes.get(sym, {})
            sym_highs = symbol_highs.get(sym, {})
            sym_lows = symbol_lows.get(sym, {})

            today_close = closes.get(date)
            if today_close is None:
                continue

            # A/D: compare to previous trading day
            prev_close = None
            for prev_idx in range(idx - 1, max(idx - 5, -1), -1):
                prev_date = sorted_dates[prev_idx]
                if prev_date in closes:
                    prev_close = closes[prev_date]
                    break

            if prev_close is not None and prev_close > 0:
                if today_close > prev_close:
                    advances += 1
                elif today_close < prev_close:
                    declines += 1

            # 52-week H/L: compare today's high/low to prior 252 trading days
            today_high = sym_highs.get(date)
            today_low = sym_lows.get(date)
            if today_high is not None and today_low is not None and prior_dates:
                prior_high = max(
                    (sym_highs[d] for d in prior_dates if d in sym_highs),
                    default=None
                )
                prior_low = min(
                    (sym_lows[d] for d in prior_dates if d in sym_lows),
                    default=None
                )
                if prior_high is not None and today_high >= prior_high:
                    new_highs += 1
                if prior_low is not None and today_low <= prior_low:
                    new_lows += 1

        net_ad = advances - declines
        net_hl = new_highs - new_lows

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
            "newHighs": new_highs,
            "newLows": new_lows,
            "netHL": net_hl,
            "ema19": round(ema19, 4),
            "ema39": round(ema39, 4),
            "oscillator": round(oscillator, 4),
            "summationIndex": round(summation_index, 4)
        })

    print(f"  Computed {len(daily_series)} daily entries")
    if daily_series:
        last = daily_series[-1]
        print(f"  Latest: {last['date']} — MCO: {last['oscillator']:.4f}, MCSI: {last['summationIndex']:.4f}, Net H/L: {last['netHL']:+d} ({last['newHighs']}H/{last['newLows']}L)")

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

    # Recent 52-week H/L history (last N trading days) — full history for the chart
    hl_history = []
    for entry in daily_series[-HL_HISTORY_DAYS:]:
        hl_history.append({
            "date": entry["date"],
            "highs": entry["newHighs"],
            "lows": entry["newLows"],
            "net": entry["netHL"]
        })

    output = {
        "version": 3,
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
        "adHistory": ad_history,
        "hlHistory": hl_history
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"  Written: {OUTPUT_FILE} ({file_size:,} bytes)")
    print(f"\nDone! {len(daily_series)} trading days processed.")


if __name__ == "__main__":
    main()
