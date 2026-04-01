#!/usr/bin/env python3
"""
build_industry_map.py — Build industry_map.json for Trend M.A.P. Screener

Fetches industry classifications from Yahoo Finance for the full screener
universe and writes a compact JSON file you commit to your GitHub repo.

Usage:
    pip install yfinance
    python build_industry_map.py

Output:
    industry_map.json  (commit this to your repo root alongside screener.html)

Runtime: ~10–20 minutes for ~1,100 symbols at a polite request rate.
You only need to re-run this occasionally (industries rarely change).
"""

import json
import time
import sys
from datetime import datetime

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed.")
    print("Run:  pip install yfinance")
    sys.exit(1)


# ── Full screener universe (mirrors FALLBACK_SCREENER_UNIVERSE in compute_breadth.py)
SYMBOLS = [
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

# Deduplicate while preserving order
seen = set()
SYMBOLS = [s for s in SYMBOLS if s not in seen and not seen.add(s)]

OUTPUT_FILE = 'industry_map.json'
BATCH_SIZE  = 20    # yfinance handles batches internally; smaller = more resilient
SLEEP_SEC   = 1.0   # pause between batches — keeps Yahoo happy
RESUME_FILE = 'industry_map_partial.json'  # checkpoint so you can resume if interrupted


def fetch_batch(syms):
    """Fetch industry for a batch of symbols via yfinance. Returns dict sym->industry."""
    result = {}
    try:
        tickers = yf.Tickers(' '.join(syms))
        for sym in syms:
            try:
                info = tickers.tickers[sym].info
                industry = info.get('industry', '') or ''
                result[sym] = industry
            except Exception:
                result[sym] = ''
    except Exception as e:
        print(f"  Batch error: {e} — marking all as empty")
        for sym in syms:
            result[sym] = ''
    return result


def main():
    print("=" * 60)
    print("Trend M.A.P. — Industry Map Builder")
    print(f"Universe: {len(SYMBOLS)} symbols")
    print("=" * 60)

    # Resume from partial file if it exists
    industry_map = {}
    try:
        with open(RESUME_FILE) as f:
            industry_map = json.load(f)
        print(f"Resuming from checkpoint: {len(industry_map)} symbols already done")
    except FileNotFoundError:
        pass

    remaining = [s for s in SYMBOLS if s not in industry_map]
    total     = len(SYMBOLS)
    done      = len(industry_map)

    print(f"Remaining: {len(remaining)} symbols\n")

    batches = [remaining[i:i+BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
    start   = time.time()

    for i, batch in enumerate(batches):
        batch_result = fetch_batch(batch)
        industry_map.update(batch_result)
        done += len(batch)

        filled  = sum(1 for v in batch_result.values() if v)
        elapsed = time.time() - start
        rate    = done / elapsed if elapsed > 0 else 0
        eta     = (total - done) / rate if rate > 0 else 0

        print(
            f"  Batch {i+1}/{len(batches)} — "
            f"{batch[0]}…{batch[-1]}  "
            f"({filled}/{len(batch)} with industry)  "
            f"[{done}/{total} total | ETA {eta/60:.1f} min]"
        )

        # Checkpoint every 5 batches
        if (i + 1) % 5 == 0:
            with open(RESUME_FILE, 'w') as f:
                json.dump(industry_map, f)

        if i < len(batches) - 1:
            time.sleep(SLEEP_SEC)

    # Final stats
    filled_count = sum(1 for v in industry_map.values() if v)
    print(f"\nDone: {filled_count}/{len(industry_map)} symbols have industry data "
          f"({filled_count/len(industry_map)*100:.0f}%)")

    # Write final output — compact, sorted by symbol
    output = {
        "_generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_count": filled_count,
        **{k: industry_map[k] for k in sorted(industry_map) if industry_map[k]}
    }
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    size_kb = len(json.dumps(output)) / 1024
    print(f"Written: {OUTPUT_FILE}  ({size_kb:.0f} KB)")
    print(f"\nNext step: commit {OUTPUT_FILE} to your GitHub repo root.")

    # Clean up checkpoint
    import os
    try:
        os.remove(RESUME_FILE)
    except FileNotFoundError:
        pass


if __name__ == '__main__':
    main()
