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
FINNHUB_BASE = "https://finnhub.io/api/v1"
LOOKBACK_YEARS = 3          # ~700+ trading days
BATCH_SIZE = 50             # Alpaca max symbols per request
AD_HISTORY_DAYS = 60        # Recent A/D entries to include for incremental updates
OUTPUT_FILE = "breadth_baseline.json"
SCREENER_LOOKBACK_DAYS = 90  # Cal days of bars needed for SMA20/SMA50 + 20-day avg vol
FINNHUB_RATE_LIMIT = 55     # Conservative req/min (free tier = 60)

# Russell 1000 + NDX-100 combined screener universe (~1,100 symbols)
# Mirrors _SCREENER_UNIVERSE in index.html — keep in sync
# Fallback screener universe — used if live scraping fails.
# Covers Russell 1000 + Russell 2000 + NDX-100 (~1,200 symbols after dedup).
# Updated manually ~annually; the live scraper below keeps this current automatically.
FALLBACK_SCREENER_UNIVERSE = [
    'A','AAPL','ABBV','ABT','ACGL','ACN','ADBE','ADI','ADM','ADP','ADSK','AEE','AEP',
    'AES','AFL','AIG','AIZ','AJG','AKAM','ALB','ALGN','ALK','ALL','ALLE','AMAT','AMCR',
    'AMD','AME','AMGN','AMP','AMT','AMZN','ANET','ANSS','AON','AOS','APA','APD','APH',
    'APP','APTV','ARE','ARM','ASML','ATO','AVB','AVGO','AVY','AWK','AXON','AXP','AZN',
    'AZO','BA','BAC','BALL','BAX','BBWI','BBY','BDX','BEN','BG','BIIB','BK','BKNG',
    'BKR','BLDR','BLK','BMY','BR','BRO','BSX','BWA','C','CAG','CAH','CARR','CAT','CB',
    'CBOE','CBRE','CCI','CCEP','CCL','CDNS','CDW','CEG','CF','CFG','CHD','CHRW','CHTR',
    'CI','CINF','CL','CLX','CMA','CMCSA','CME','CMG','CMI','CMS','CNC','CNP','COF',
    'COO','COP','COST','CPAY','CPB','CPRT','CPT','CRL','CRM','CRWD','CSCO','CSGP',
    'CSX','CTAS','CTLT','CTRA','CTSH','CTVA','CVS','CVX','CZR','D','DAL','DAY','DASH',
    'DD','DE','DECK','DDOG','DG','DGX','DHI','DHR','DIS','DLR','DLTR','DOC','DOV',
    'DOW','DPZ','DRI','DTE','DUK','DVA','DVN','DXCM','EA','EBAY','ECL','ED','EFX',
    'EG','EIX','EL','ELV','EMN','EMR','ENPH','EOG','EPAM','EQIX','EQR','EQT','ES',
    'ESS','ETN','ETR','EVRG','EW','EXC','EXPD','EXPE','EXR','F','FANG','FAST','FCX',
    'FDS','FDX','FE','FFIV','FI','FICO','FIS','FITB','FMC','FOX','FOXA','FRT','FSLR',
    'FTNT','FTV','GD','GDDY','GE','GEHC','GEN','GEV','GFS','GILD','GIS','GL','GLW',
    'GM','GNRC','GOOG','GOOGL','GPC','GPN','GRMN','GS','GWW','HAL','HAS','HBAN','HCA',
    'HD','HES','HIG','HII','HLT','HOLX','HON','HPE','HPQ','HRL','HSIC','HST','HSY',
    'HUBB','HUM','HWM','IBM','ICE','IDXX','IEX','IFF','ILMN','INCY','INTC','INTU',
    'INVH','IP','IPG','IQV','IR','IRM','ISRG','IT','ITW','IVZ','J','JBHT','JBL','JCI',
    'JKHY','JNJ','JNPR','JPM','K','KDP','KEY','KEYS','KHC','KIM','KLAC','KMB','KMI',
    'KMX','KO','KR','L','LDOS','LEN','LH','LHX','LIN','LKQ','LLY','LMT','LNT','LOW',
    'LRCX','LULU','LUV','LVS','LW','LYB','LYV','MA','MAA','MAR','MAS','MCD','MCHP',
    'MCK','MCO','MDLZ','MDB','MDT','MELI','MET','META','MGM','MHK','MKC','MKTX','MLM',
    'MMC','MMM','MNST','MO','MOH','MOS','MPC','MPWR','MRK','MRNA','MRO','MRVL','MS',
    'MSCI','MSFT','MSI','MTB','MTCH','MTD','MU','NCLH','NDAQ','NEE','NEM','NFLX','NI',
    'NKE','NOC','NOW','NRG','NSC','NTAP','NTRS','NUE','NVDA','NVR','NWS','NWSA','NXPI',
    'O','ODFL','OKE','OMC','ON','ORCL','ORLY','OXY','PANW','PARA','PAYC','PAYX','PCAR',
    'PCG','PDD','PEG','PEP','PFE','PFG','PG','PGR','PH','PHM','PKG','PLD','PLTR','PM',
    'PNC','PNR','PNW','PODD','POOL','PPG','PPL','PRU','PSA','PSX','PTC','PWR','PYPL',
    'QCOM','QRVO','RCL','REG','REGN','RF','RJF','RL','RMD','ROK','ROL','ROP','ROST',
    'RSG','RTX','RVTY','SBAC','SBUX','SCHW','SHW','SHOP','SJM','SLB','SMCI','SNA',
    'SNPS','SO','SPG','SPGI','SRE','STT','STX','STZ','SWK','SWKS','SYF','SYK','SYY',
    'T','TAP','TDG','TDY','TEAM','TECH','TEL','TER','TFC','TFX','TGT','TJX','TMO',
    'TMUS','TPR','TRGP','TRMB','TROW','TRV','TSCO','TSLA','TSN','TT','TTD','TTWO',
    'TXN','TXT','TYL','UAL','UBER','UDR','UHS','ULTA','UNH','UNP','UPS','URI','USB',
    'V','VFC','VICI','VLO','VMC','VRSK','VRSN','VRTX','VST','VTR','VTRS','WAB','WAT',
    'WBA','WBD','WDC','WEC','WELL','WFC','WHR','WM','WMB','WMT','WRB','WST','WTW',
    'WY','WYNN','XEL','XOM','XYL','YUM','ZBH','ZBRA','ZTS',
    # Russell mid/small-cap extension
    'AA','AAON','ACHC','ACLS','ACM','AGCO','AGO','AIRC','AL','ALKS','ALLY','AMKR',
    'AMN','AN','ANF','APAM','ARCC','ARMK','ARW','ASH','ASGN','ASO','ASTE','ATMU',
    'ATSG','AUB','AVNT','AVTR','AWI','AWR','AX','AYI','AZZ','B','BAH','BANF','BANR',
    'BCC','BCO','BDN','BECN','BFH','BGS','BJ','BKH','BLD','BLMN','BMI','BOOT','BOX',
    'BRC','BRX','BSY','BURL','BWB','BWXT','BYD','CACI','CALM','CALX','CARG','CARS',
    'CASH','CASY','CATO','CBRL','CBT','CBU','CC','CCK','CCOI','CCS','CFR','CGNX','CHE',
    'CHRD','CIR','CKH','CLB','CLF','CLH','CMC','CMCO','CNA','CNH','CNM','CNO','CNX',
    'COLB','COLD','COLM','COOP','CORE','CPF','CR','CROX','CRS','CRUS','CRSR','CSL',
    'CSWI','CTBI','CTG','CUBE','CUZ','CVLT','CW','CWST','CWT','DAN','DAR','DBRG','DCO',
    'DCOM','DDS','DEI','DELL','DEN','DFS','DHX','DIN','DKS','DLX','DNB','DNOW','DNUT',
    'DOCS','DOMO','DRH','DRVN','DSP','DT','DUOL','DXC','EAT','EBS','EFC','EHC','EIG',
    'ELF','EME','ENS','ENVA','EPRT','EQH','ESAB','ESNT','ESRT','EVR','EWBC','EXEL',
    'EXLS','EXP','FAF','FARO','FBP','FCNCA','FHN','FIBK','FIVN','FIVE','FIZZ','FL',
    'FLNC','FLUT','FNB','FND','FNF','FOUR','FR','FUL','G','GATX','GBCI','GBX','GCI',
    'GCO','GEF','GFF','GHM','GIL','GLDD','GLOB','GLP','GME','GMED','GMS','GO','GOLF',
    'GPRE','GRC','GVA','GWB','GXO','H','HAE','HAYW','HBI','HCC','HCSG','HE','HEES',
    'HGV','HI','HLIT','HLMN','HLX','HNI','HOPE','HP','HPK','HRB','HRI','HTLD','HTLF',
    'HUN','IAC','IART','IBP','ICF','ICFI','IDCC','IDA','IIPR','IMAX','INGR','INT',
    'INTL','IOSP','IPAR','IPGP','IRT','ITIC','IVT','JELD','JHG','JLL','JOBY','JOE',
    'JOUT','JWN','KALU','KAR','KFY','KLIC','KNF','KNX','KRC','KSS','KVYO','LAD',
    'LADR','LAZ','LBRT','LC','LCII','LECO','LEG','LGIH','LNC','LNW','LPX','LSTR',
    'LUMN','LXP','LZ','M','MAC','MATX','MBI','MBWM','MCS','MCY','MDP','MEDP','MGY',
    'MHO','MIDD','MMSI','MMS','MOD','MORN','MPW','MSA','MTG','MTN','MTZ','MUSA','NBR',
    'NCNO','NEP','NFG','NJR','NNN','NOV','NOVT','NRC','NSA','NSP','NTGR','NTR','NVT',
    'NX','NYT','OC','OFC','OGE','OGN','OI','OIS','OLLI','OLN','OMCL','OMF','ORA',
    'OSCR','OUT','OWL','PAG','PAGP','PBF','PCH','PEB','PENN','PII','PJT','PLAY','PLMR',
    'PLNT','PLUS','PNFP','PNM','POST','PPC','PR','PRG','PRIM','PRK','PRVA','PSMT','PVH',
    'R','RBC','RDN','RES','REXR','RGA','RGP','RIG','RLJ','RMBS','ROCK','RPM','RRC',
    'RS','RXO','SAH','SAIA','SANM','SBCF','SBGI','SCI','SEIC','SEM','SFBS','SFM',
    'SFNC','SGH','SIGI','SITE','SJW','SKX','SLG','SM','SMPL','SNV','SONO','SPB','SPSC',
    'SPXC','SRC','SRCL','SSD','STAG','STLD','STR','SUM','SUPN','SVC','SWN','SWX','SXT',
    'SYBT','SYM','TALO','TBI','TCBI','TCBK','TDC','TDS','TDW','TENB','TFIN','THC',
    'THS','TKR','TNDM','TOL','TOWN','TPH','TPX','TRMK','TTEK','TTMI','TXRH','TZOO',
    'UFPI','UGI','UMBF','UNF','UNFI','UNM','UNUM','USFD','USPH','VICR','VIRT','VMEO',
    'VPG','VSH','VVV','W','WAFD','WAL','WBS','WCC','WD','WDFC','WEN','WERN','WEX',
    'WH','WHD','WK','WOR','WPC','WRE','WS','WSFS','WU','WWD','XPO','XRAY','XRX',
    'YETI','YELP','ZWS',
    # R2000 small-cap extension
    'AAOI','AAWW','ABG','ACEL','ACMR','ACVA','ADEA','ADN','ADNT','AEHR','AEIS','AESI',
    'AFRI','AFYA','AGM','AGS','AGX','AHH','AHT','AIXI','AJX','AKBA','AKRO','ALCO',
    'ALEC','ALG','ALGT','ALIM','ALJJ','ALKT','ALNT','ALRS','ALTG','ALTI','ALUR','ALVR',
    'AMBC','AMCX','AMED','AMEH','ANGI','ANGO','ANI','AOSL','APEI','APOG','APPF','APVO',
    'AQB','ARCB','ARCO','ARCT','ARDX','AROW','ARQT','ARRY','ARTW','ATEC','ATEX','ATLO',
    'ATRA','ATXS','AUBN','AUID','AULT','AUPH','AVAH','AVAV','AVDL','AVNS','AVNW','AVPT',
    'AVRO','AXDX','AXGN','AXSM','AY','BBCP','BBIO','BBSI','BCEI','BCML','BCPC','BCRX',
    'BDTX','BDVG','BELFB','BFAM','BFLY','BFST','BHB','BHLB','BHRB','BJRI','BKKT','BL',
    'BLBD','BLCM','BLFS','BLKB','BLX','BMTC','BNFT','BNRG','BOC','BOMN','BRSP','BRT',
    'BSIG','BSVN','BTU','BUR','BW','BWB','BYFC','CABO','CADE','CAKE','CALA','CAMP',
    'CBAN','CBFV','CCNE','CDXC','CECO','CENT','CENTA','CENX','CEVA','CFB','CFFI','CFFN',
    'CFSB','CGBD','CHEF','CHGG','CHUY','CKPT','CLBK','CLBT','CLIN','CLPT','CLSK','CMPS',
    'CNOB','CNSL','COMM','COSA','COUR','CRGY','CRK','CULP','CVLY','CXDO','CXSE','CXW',
    'CYH','CYTO','CZWI','DAKT','DCPH','DFIN','DIOD','DJCO','DPSI','DY','DYNT','DZSI',
    'ECPG','ECTX','EGP','EMMS','ENOV','ENR','ENSG','EQUB','ETD','ETWO','EZPW','FCPT',
    'FELE','FFBC','FFIN','FISI','FLNT','FLR','FRAF','FRBA','FRME','FROG','FRPH','FRST',
    'FRWT','FSB','FSBW','FSCO','FUNC','FWRD','GHC','GNE','GRPN','GRVY','GTX','HRZN',
    'HTBI','JACK','KALU','LOCO','LQDA','LQDT','MARA','MTN','NABL','NATL',
]



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


def fetch_full_exchange_universe(api_key, api_secret):
    """Fetch all active common-stock equities from NYSE, NASDAQ, AMEX, and ARCA
    via the Alpaca /v2/assets endpoint.

    Filters out ETFs, warrants, rights, preferred shares, and other non-common-
    stock instruments by name pattern and asset attributes.

    Falls back to FALLBACK_SCREENER_UNIVERSE if the API call fails or returns
    too few results.

    Returns a deduplicated list of ticker strings, dot-tickers excluded.
    """
    BROKER_BASE = "https://paper-api.alpaca.markets/v2"
    TARGET_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "ARCA"}
    MIN_EXPECTED = 3000  # Sanity floor — fewer than this suggests a bad response

    # Name-pattern exclusions: warrants, rights, preferred, units, notes, ETFs
    EXCLUDE_PATTERNS = [
        " warrant", " wt", " right", " unit", " note", " preferred",
        " pfd", " trust", " fund", " etf", " etp", " reit",
    ]

    all_assets = []
    page_token = None
    page = 0

    print("  Fetching asset list from Alpaca /v2/assets ...")
    try:
        while True:
            params = {
                "status": "active",
                "asset_class": "us_equity",
                "tradable": "true",
            }
            if page_token:
                params["page_token"] = page_token

            url = f"{BROKER_BASE}/assets?" + urlencode(params)
            req = Request(url, headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
                "Accept": "application/json"
            })
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())

            if not isinstance(data, list):
                raise ValueError(f"Unexpected response type: {type(data)}")

            all_assets.extend(data)
            page += 1

            # Alpaca returns up to 1000 per page; if fewer, we're done
            if len(data) < 1000:
                break

            # Some Alpaca versions paginate via next_page_token in headers
            page_token = resp.headers.get("X-Next-Page-Token")
            if not page_token:
                break

            if page > 20:  # Safety cap
                break

        print(f"  Retrieved {len(all_assets)} total assets ({page} page(s))")

    except Exception as e:
        print(f"  Alpaca assets fetch failed: {e} — using fallback")
        return _fallback_screener_universe()

    # Filter to target exchanges + common stocks only
    result = []
    seen = set()
    skipped_exchange = skipped_name = skipped_dot = 0

    for asset in all_assets:
        sym = asset.get("symbol", "")
        exchange = asset.get("exchange", "")
        name = (asset.get("name") or "").lower()

        # Exchange filter
        if exchange not in TARGET_EXCHANGES:
            skipped_exchange += 1
            continue

        # Skip dot-tickers (Alpaca uses BRK/B style anyway but be safe)
        if "." in sym or "/" in sym:
            skipped_dot += 1
            continue

        # Skip non-common-stock instruments by name pattern
        if any(pat in name for pat in EXCLUDE_PATTERNS):
            skipped_name += 1
            continue

        # Skip obvious non-equity suffixes in ticker (W=warrant, R=right, U=unit)
        if len(sym) > 1 and sym[-1] in ("W", "R", "U") and sym[:-1].isalpha():
            skipped_name += 1
            continue

        if sym and sym not in seen:
            seen.add(sym)
            result.append(sym)

    print(f"  Filtered: {skipped_exchange} wrong exchange, "
          f"{skipped_name} non-common, {skipped_dot} dot/slash tickers")
    print(f"  Final universe: {len(result)} common stocks")

    if len(result) < MIN_EXPECTED:
        print(f"  Universe too small ({len(result)} < {MIN_EXPECTED}) — using fallback")
        return _fallback_screener_universe()

    return sorted(result)  # Alphabetical for deterministic batching


def _fallback_screener_universe():
    """Return deduplicated fallback screener universe, dot-tickers excluded."""
    seen = set()
    result = []
    for s in FALLBACK_SCREENER_UNIVERSE:
        if s not in seen and "." not in s:
            seen.add(s)
            result.append(s)
    print(f"  Screener universe: {len(result)} symbols (fallback hardcoded list)")
    return result



def fetch_sector_map(finnhub_key, symbols):
    """Fetch sector/industry for each symbol via Finnhub /stock/profile2.

    Respects the free-tier rate limit (FINNHUB_RATE_LIMIT req/min) by
    throttling with a small sleep between requests.  Symbols that return
    no profile or an error are silently skipped — the browser falls back
    to showing no tag for those tickers.

    Returns a dict: { "AAPL": "Technology", "JPM": "Financial Services", ... }
    """
    if not finnhub_key:
        print("  FINNHUB_KEY not set — skipping sector map")
        return {}

    sector_map = {}
    delay = 60.0 / FINNHUB_RATE_LIMIT  # seconds between requests

    print(f"  Fetching sector data for {len(symbols)} symbols "
          f"(~{len(symbols) * delay / 60:.1f} min at {FINNHUB_RATE_LIMIT} req/min)…")

    for i, sym in enumerate(symbols, 1):
        try:
            url = f"{FINNHUB_BASE}/stock/profile2?symbol={sym}&token={finnhub_key}"
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            sector = (data.get("finnhubIndustry") or "").strip()
            if sector:
                sector_map[sym] = sector
        except Exception as e:
            # Non-fatal — just skip this symbol
            if i <= 5 or i % 200 == 0:
                print(f"    [{i}] {sym}: {e}")
        time.sleep(delay)
        if i % 100 == 0:
            print(f"    {i} / {len(symbols)} done ({len(sector_map)} sectors found)")

    print(f"  Sector map complete: {len(sector_map)} / {len(symbols)} symbols mapped")
    return sector_map


# =============================================================================
# MAIN
# =============================================================================

def main():
    api_key = os.environ.get("ALPACA_KEY_ID", "").strip()
    api_secret = os.environ.get("ALPACA_SECRET_KEY", "").strip()
    finnhub_key = os.environ.get("FINNHUB_KEY", "").strip()

    if not api_key or not api_secret:
        print("ERROR: ALPACA_KEY_ID and ALPACA_SECRET_KEY environment variables required.")
        sys.exit(1)

    print("=" * 60)
    print("Trend M.A.P. — Breadth Baseline Generator")
    print("=" * 60)

    # --- Step 1: Get Nasdaq-100 components ---
    print("\n[1/5] Fetching Nasdaq-100 components...")
    components = fetch_nasdaq100_components()
    component_source = "slickcharts" if len(components) != len(FALLBACK_COMPONENTS) else "fallback"

    # Add benchmark ETFs for reference
    benchmarks = ["SPY", "QQQ", "DIA"]
    all_symbols = components + [b for b in benchmarks if b not in components]

    # --- Step 2: Fetch historical daily bars for all symbols ---
    print(f"\n[2/5] Fetching {LOOKBACK_YEARS}y daily bars for {len(all_symbols)} symbols...")

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
    print("\n[3/5] Computing daily advance/decline series...")

    # Collect all unique trading dates across all symbols
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
                highs[date_str]  = bar["h"]
                lows[date_str]   = bar["l"]
        symbol_closes[sym] = closes
        symbol_highs[sym]  = highs
        symbol_lows[sym]   = lows
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
        new_highs = 0
        new_lows = 0

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

            # 52-week H/L: compare today's high/low against prior 52w range
            # (all trading days in the prior year, excluding today itself)
            today_high = symbol_highs.get(sym, {}).get(date)
            today_low  = symbol_lows.get(sym, {}).get(date)
            if today_high is not None and today_low is not None and idx >= 1:
                # Find dates in the prior 252 trading days (approx 1 year)
                prior_start = max(0, idx - 252)
                prior_dates = sorted_dates[prior_start:idx]
                sym_highs_map = symbol_highs.get(sym, {})
                sym_lows_map  = symbol_lows.get(sym, {})
                prior_high = max((sym_highs_map[d] for d in prior_dates if d in sym_highs_map), default=None)
                prior_low  = min((sym_lows_map[d]  for d in prior_dates if d in sym_lows_map),  default=None)
                if prior_high is not None and today_high >= prior_high:
                    new_highs += 1
                if prior_low is not None and today_low <= prior_low:
                    new_lows += 1

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
            "summationIndex": round(summation_index, 4),
            "newHighs": new_highs,
            "newLows": new_lows,
            "netHL": new_highs - new_lows
        })

    print(f"  Computed {len(daily_series)} daily entries")
    if daily_series:
        last = daily_series[-1]
        print(f"  Latest: {last['date']} — MCO: {last['oscillator']:.4f}, MCSI: {last['summationIndex']:.4f}")

    # --- Step 4: Build output JSON ---
    print(f"\n[4/5] Building McClellan output...")

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

    # Recent 52-week H/L history (last N trading days) — seeds browser chart on load
    hl_history = []
    for entry in daily_series[-AD_HISTORY_DAYS:]:
        hl_history.append({
            "date": entry["date"],
            "highs": entry["newHighs"],
            "lows": entry["newLows"],
            "net": entry["netHL"]
        })

    # --- Step 5: Screener data — SMA20, SMA50, 20-day avg vol & dollar vol ---
    print(f"\n[5/5] Fetching screener universe and computing SMA data...")

    # Fetch full exchange universe via Alpaca assets API; falls back to hardcoded list if call fails
    scr_symbols = fetch_full_exchange_universe(api_key, api_secret)

    end_date_screener = datetime.now(timezone.utc)
    start_date_screener = end_date_screener - timedelta(days=SCREENER_LOOKBACK_DAYS)
    scr_start = start_date_screener.strftime("%Y-%m-%d")
    scr_end   = end_date_screener.strftime("%Y-%m-%d")

    print(f"  Computing SMA data for {len(scr_symbols)} symbols...")

    screener_data = {}  # sym -> { sma20, sma50, avgVol20, avgDolVol20 }

    for i in range(0, len(scr_symbols), BATCH_SIZE):
        batch = scr_symbols[i:i + BATCH_SIZE]
        print(f"  Screener batch {i // BATCH_SIZE + 1}/{(len(scr_symbols) + BATCH_SIZE - 1) // BATCH_SIZE}: "
              f"{batch[0]}…{batch[-1]}")
        params = {
            "symbols": ",".join(batch),
            "timeframe": "1Day",
            "start": scr_start,
            "end": scr_end,
            "limit": "10000",
            "adjustment": "split",
            "sort": "asc"
        }
        try:
            data = alpaca_get_with_retry("/stocks/bars", params, api_key, api_secret)
        except Exception as e:
            print(f"    ERROR: {e} — skipping batch")
            time.sleep(0.5)
            continue

        if data.get("bars"):
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            for sym, bars in data["bars"].items():
                if not bars:
                    continue
                # Strip today's bar if present — it may be incomplete if the job
                # runs during market hours or close to open
                if bars[-1]["t"][:10] == today_str:
                    bars = bars[:-1]
                if len(bars) < 20:
                    continue
                closes = [b["c"] for b in bars]
                vols   = [b["v"] for b in bars]

                sma20 = round(sum(closes[-20:]) / 20, 4) if len(closes) >= 20 else None
                sma50 = round(sum(closes[-50:]) / 50, 4) if len(closes) >= 50 else None

                rv = vols[-20:]
                rp = closes[-20:]
                avg_vol20     = round(sum(rv) / len(rv))
                avg_dolvol20  = round(sum(v * p for v, p in zip(rv, rp)) / len(rv))

                screener_data[sym] = {
                    "sma20":       sma20,
                    "sma50":       sma50,
                    "avgVol20":    avg_vol20,
                    "avgDolVol20": avg_dolvol20
                }

        time.sleep(0.3)

    print(f"  Screener data built for {len(screener_data)} symbols")

    # --- Step 6: Sector map via Finnhub -----------------------------------
    print(f"\n[6/6] Fetching sector/industry data from Finnhub…")
    sector_map = fetch_sector_map(finnhub_key, scr_symbols)

    # --- Write output JSON ---
    print(f"\nWriting {OUTPUT_FILE}...")

    output = {
        "version": 5,
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
        "hlHistory": hl_history,
        "screenerData": screener_data,
        "screenerUniverse": scr_symbols,
        "sectorMap": sector_map,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"  Written: {OUTPUT_FILE} ({file_size:,} bytes)")
    print(f"  screenerData: {len(screener_data)} symbols")
    print(f"  sectorMap:    {len(sector_map)} symbols")
    print(f"\nDone! {len(daily_series)} trading days processed.")


if __name__ == "__main__":
    main()
