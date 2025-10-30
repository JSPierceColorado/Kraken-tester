import os
import time
import json
import logging
from typing import Dict, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------------------- Config ----------------------
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "Crypto Papertrader")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "tab")
QUOTE_CCY = os.getenv("QUOTE_CCY", "USD").upper()
POLL_INTERVAL_SECS = int(os.getenv("POLL_INTERVAL_SECS", "60"))  # how often to refresh prices
ASSET_PAIRS_REFRESH_SECS = int(os.getenv("ASSET_PAIRS_REFRESH_SECS", "600"))  # refresh Kraken pairs mapping
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
KRAKEN_BASE = "https://api.kraken.com/0/public"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("crypto-tracker")

# Kraken base-asset aliases (helpful when users type common names/symbols)
BASE_ALIASES = {
    "BTC": "XBT",
    "XBT": "XBT",
    "DOGE": "XDG",
    "XDG": "XDG",
    "BITCOIN": "XBT",
    "ETHEREUM": "ETH",
    "ETH": "ETH",
    "ADA": "ADA",
    "CARDANO": "ADA",
    "XRP": "XRP",
    "RIPPLE": "XRP",
    "SOL": "SOL",
    "SOLANA": "SOL",
    "LTC": "LTC",
    "LITECOIN": "LTC",
    "BCH": "BCH",
    "BITCOIN CASH": "BCH",
    # Add any house favorites here as needed
}

# ----------------- Google Sheets auth ----------------

def get_gspread_client() -> gspread.Client:
    """Authenticate to Google using a JSON string in GOOGLE_CREDS_JSON."""
    google_creds_json = os.getenv("GOOGLE_CREDS_JSON")
    if not google_creds_json:
        raise RuntimeError("GOOGLE_CREDS_JSON env var is required (service account JSON string)")
    info = json.loads(google_creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

# --------------- Kraken helpers ----------------------

class KrakenPairsCache:
    def __init__(self):
        self._pairs_by_base_quote: Dict[Tuple[str, str], str] = {}
        self._last_refresh = 0.0

    def refresh_if_needed(self):
        now = time.time()
        if now - self._last_refresh < ASSET_PAIRS_REFRESH_SECS and self._pairs_by_base_quote:
            return
        log.info("Refreshing Kraken AssetPairs mapping...")
        url = f"{KRAKEN_BASE}/AssetPairs"
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise RuntimeError(f"Kraken AssetPairs error: {data['error']}")
        pairs = data["result"]
        mapping: Dict[Tuple[str, str], str] = {}
        for pair_name, meta in pairs.items():
            base = meta.get("base", "").replace("X", "").replace("Z", "")  # e.g., XXBT -> XBT
            quote = meta.get("quote", "").replace("X", "").replace("Z", "")  # e.g., ZUSD -> USD
            if base and quote:
                mapping[(base, quote)] = pair_name
        self._pairs_by_base_quote = mapping
        self._last_refresh = now
        log.info("Loaded %d Kraken pairs", len(mapping))

    def find_pair(self, base_input: str, quote: str) -> Optional[str]:
        base_norm = BASE_ALIASES.get(base_input.upper().strip(), base_input.upper().strip())
        quote_norm = quote.upper().strip()
        # try exact
        if (base_norm, quote_norm) in self._pairs_by_base_quote:
            return self._pairs_by_base_quote[(base_norm, quote_norm)]
        # Some assets are stored in Kraken meta as e.g. XBT while our alias already matches; also try without aliasing
        for key in [
            (base_norm, quote_norm),
            (base_norm.replace("X", ""), quote_norm.replace("Z", "")),
            (base_input.upper().strip(), quote_norm),
        ]:
            if key in self._pairs_by_base_quote:
                return self._pairs_by_base_quote[key]
        return None

pairs_cache = KrakenPairsCache()


def get_kraken_last_price(pair_name: str) -> float:
    url = f"{KRAKEN_BASE}/Ticker"
    r = requests.get(url, params={"pair": pair_name}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken Ticker error: {data['error']}")
    result = data["result"]
    # result is a dict keyed by the canonical pair name
    first_key = next(iter(result))
    last_trade = result[first_key]["c"][0]  # last trade price as string
    return float(last_trade)

# --------------- Core loop ---------------------------

def update_sheet_once(sh):
    ws = sh.worksheet(WORKSHEET_NAME)
    # Read column A (tickers/names), skipping header at A1
    col = ws.col_values(1)
    if len(col) <= 1:
        log.info("No tickers found (only header or empty).")
        return

    inputs = col[1:]  # from A2 downward
    pairs_cache.refresh_if_needed()

    cells_to_update = []
    start_row = 2

    for idx, raw in enumerate(inputs):
        row = start_row + idx
        symbol_str = (raw or "").strip()
        if not symbol_str:
            continue
        try:
            pair_name = pairs_cache.find_pair(symbol_str, QUOTE_CCY)
            if not pair_name:
                log.warning("No Kraken pair for %s-%s (row %d)", symbol_str, QUOTE_CCY, row)
                value = "N/A"
            else:
                price = get_kraken_last_price(pair_name)
                value = price
            cells_to_update.append(gspread.Cell(row=row, col=2, value=value))  # B column
        except Exception as e:
            log.exception("Failed to fetch price for %s (row %d)", symbol_str, row)
            cells_to_update.append(gspread.Cell(row=row, col=2, value="ERR"))

    if cells_to_update:
        ws.update_cells(cells_to_update, value_input_option="USER_ENTERED")
        log.info("Updated %d price cells.", len(cells_to_update))


def main():
    log.info("Starting Kraken Crypto Price Tracker...")
    gc = get_gspread_client()
    sh = gc.open(SPREADSHEET_NAME)

    while True:
        try:
            update_sheet_once(sh)
        except Exception:
            log.exception("Run iteration failed")
        time.sleep(POLL_INTERVAL_SECS)


if __name__ == "__main__":
    main()
