#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

# Loaded from metalprice.api
API_KEY = ""
# If you move the key file, update this path.
KEY_FILENAME = "metalprice.api"
# MetalpriceAPI base URL (US region)
BASE_URL = "https://api.metalpriceapi.com/v1"
DEFAULT_DATE = "2026-01-30"
DEFAULT_BASE = "USD"
DEFAULT_SYMBOL = "XAU"
DEFAULT_VERBOSE = True
DEFAULT_CACHE = True


def build_url(date_str: str, base: str, symbol: str) -> str:
    params = {
        "api_key": API_KEY,
        "base": base,
        "currencies": symbol,
    }
    query = urllib.parse.urlencode(params)
    # Historical endpoint is /v1/{date}
    return f"{BASE_URL}/{date_str}?{query}"


def mask_key(url: str) -> str:
    if API_KEY:
        return url.replace(API_KEY, "****")
    return url


def fetch_price(date_str: str, base: str, symbol: str, verbose: bool) -> dict:
    url = build_url(date_str, base, symbol)
    if verbose:
        print(f"Fetching: {mask_key(url)}", file=sys.stderr)
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            # Cloudflare blocks urllib's default UA; use a real browser UA.
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            # MetalpriceAPI supports API key via header; keep query param too for now.
            "X-API-Key": API_KEY,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = ""
        if body:
            try:
                return json.loads(body)
            except Exception:
                return {"success": False, "error": {"code": e.code, "info": body}}
        return {"success": False, "error": {"code": e.code, "info": e.reason}}


def extract_rate(data: dict, symbol: str):
    rates = data.get("rates") or data.get("rate") or {}
    if symbol in rates:
        return rates[symbol]
    # Some APIs return USDXAU-style keys
    alt_key = f"USD{symbol}"
    return rates.get(alt_key)


def ensure_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metal_prices (
            date TEXT NOT NULL,
            base TEXT NOT NULL,
            symbol TEXT NOT NULL,
            rate REAL NOT NULL,
            xauusd REAL,
            source TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            PRIMARY KEY (date, base, symbol, source)
        )
        """
    )

def ensure_column(conn: sqlite3.Connection, table: str, column: str, coltype: str):
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")


def fill_missing_xauusd(conn: sqlite3.Connection, verbose: bool):
    # Fill xauusd for rows where we can derive it from rate.
    cur = conn.execute(
        """
        UPDATE metal_prices
        SET xauusd = CASE
            WHEN base = 'USD' AND symbol = 'XAU' AND rate IS NOT NULL THEN (1.0 / rate)
            WHEN base = 'XAU' AND symbol = 'USD' AND rate IS NOT NULL THEN rate
            ELSE xauusd
        END
        WHERE xauusd IS NULL
          AND ((base = 'USD' AND symbol = 'XAU') OR (base = 'XAU' AND symbol = 'USD'))
        """
    )
    if verbose:
        print(f"Backfilled xauusd rows: {cur.rowcount}", file=sys.stderr)


def get_cached_price(conn: sqlite3.Connection, date_str: str, base: str, symbol: str, source: str):
    cur = conn.execute(
        """
        SELECT rate, raw_json, xauusd FROM metal_prices
        WHERE date = ? AND base = ? AND symbol = ? AND source = ?
        """,
        (date_str, base, symbol, source),
    )
    row = cur.fetchone()
    if not row:
        return None
    rate, raw_json, xauusd = row
    return rate, json.loads(raw_json), xauusd


def insert_price(db_path: str, date_str: str, base: str, symbol: str, rate: float, xauusd: float, source: str, raw: dict, verbose: bool):
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn)
        ensure_column(conn, "metal_prices", "xauusd", "REAL")
        conn.execute(
            """
            INSERT OR REPLACE INTO metal_prices (date, base, symbol, rate, xauusd, source, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (date_str, base, symbol, rate, xauusd, source, json.dumps(raw, separators=(",", ":"))),
        )
        conn.commit()
        if verbose:
            print(f"Saved to SQLite: {db_path}", file=sys.stderr)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch historical gold price from MetalpriceAPI")
    parser.add_argument("--date", default=DEFAULT_DATE, help="Date in YYYY-MM-DD (default: 2026-01-29)")
    parser.add_argument("--start", default="", help="Start date YYYY-MM-DD (optional)")
    parser.add_argument("--end", default="", help="End date YYYY-MM-DD (optional)")
    parser.add_argument("--base", default=DEFAULT_BASE, help="Base currency (default: USD)")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Metal symbol (default: XAU)")
    parser.add_argument("--sqlite", default="goldprice.db", help="Optional path to SQLite DB to store result")
    parser.add_argument("--quiet", action="store_true", help="Disable verbose output")
    parser.add_argument("--no-cache", action="store_true", help="Disable SQLite cache and force API call")
    args = parser.parse_args()
    verbose = DEFAULT_VERBOSE and not args.quiet
    use_cache = DEFAULT_CACHE and not args.no_cache

    key = os.getenv("METALPRICE_API_KEY", "").strip()
    if not key:
        key_path = os.path.join(os.path.dirname(__file__), KEY_FILENAME)
        try:
            with open(key_path, "r", encoding="utf-8") as f:
                key = f.read().strip()
        except FileNotFoundError:
            print(f"Error: missing API key file: {key_path}", file=sys.stderr)
            sys.exit(2)

    if not key or key == "PASTE_API_KEY_HERE":
        print("Error: API key missing. Set METALPRICE_API_KEY or fill metalprice.api.", file=sys.stderr)
        sys.exit(2)

    global API_KEY
    API_KEY = key

    def parse_date(s: str) -> datetime:
        return datetime.strptime(s, "%Y-%m-%d")

    # Validate date(s)
    try:
        if args.start and args.end:
            start_dt = parse_date(args.start)
            end_dt = parse_date(args.end)
        else:
            parse_date(args.date)
            start_dt = None
            end_dt = None
    except ValueError:
        print("Error: date must be in YYYY-MM-DD format.", file=sys.stderr)
        sys.exit(2)

    source = "metalpriceapi"

    def fetch_one(date_str: str):
        data = None
        rate = None
        xauusd = None
        if args.sqlite and use_cache:
            conn = sqlite3.connect(args.sqlite)
            try:
                ensure_table(conn)
                ensure_column(conn, "metal_prices", "xauusd", "REAL")
                fill_missing_xauusd(conn, verbose)
                conn.commit()
                cached = get_cached_price(conn, date_str, args.base, args.symbol, source)
            finally:
                conn.close()
            if cached:
                rate, data, xauusd = cached
                if verbose:
                    print("Using cached value from SQLite.", file=sys.stderr)

        if data is None:
            data = fetch_price(date_str, args.base, args.symbol, verbose)
        if not data.get("success", True):
            print("API error:", json.dumps(data, ensure_ascii=False), file=sys.stderr)
            return False

        if rate is None:
            rate = extract_rate(data, args.symbol)
        if rate is None:
            print("Error: could not find rate in response.", file=sys.stderr)
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return False

        # Compute USD per XAU (troy oz) regardless of request direction
        if xauusd is None:
            if args.base.upper() == "USD" and args.symbol.upper() == "XAU" and rate:
                xauusd = 1.0 / float(rate)
            elif args.base.upper() == "XAU" and args.symbol.upper() == "USD":
                xauusd = float(rate)

        # Print a simple line for now
        print(f"{date_str} {args.symbol}/{args.base} = {rate}")

        if args.sqlite:
            insert_price(args.sqlite, date_str, args.base, args.symbol, float(rate), xauusd, source, data, verbose)
        return True

    if start_dt and end_dt:
        if end_dt < start_dt:
            print("Error: --end must be >= --start.", file=sys.stderr)
            sys.exit(2)
        cur = start_dt
        while cur <= end_dt:
            fetch_one(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
    else:
        fetch_one(args.date)


if __name__ == "__main__":
    main()
