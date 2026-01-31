#!/usr/bin/env python3
import os
import re
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta

MIN_DATE = date(2026, 1, 2)


def get_latest_date(db_path: str):
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT MAX(date) FROM metal_prices")
        row = cur.fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    try:
        return datetime.strptime(row[0], "%Y-%m-%d").date()
    except ValueError:
        return None


def run_range(script_path: str, start_dt: date, end_dt: date):
    if end_dt < start_dt:
        return
    cmd = [sys.executable, script_path, "--start", start_dt.strftime("%Y-%m-%d"), "--end", end_dt.strftime("%Y-%m-%d")]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)

def update_index_html(repo_dir: str, gspln_db: str):
    if not os.path.exists(gspln_db):
        print("GSPLN.db not found; skipping index update.")
        return
    conn = sqlite3.connect(gspln_db)
    try:
        cur = conn.execute(
            """
            SELECT date, xauusd, xagusd, xaupln, xagpln
            FROM gsp
            WHERE xauusd IS NOT NULL AND xagusd IS NOT NULL
            ORDER BY date DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        print("No data in GSPLN.db; skipping index update.")
        return
    last_date, xauusd, xagusd, xaupln, xagpln = row
    index_path = os.path.join(repo_dir, "index.html")
    if not os.path.exists(index_path):
        print("index.html not found; skipping index update.")
        return
    with open(index_path, "r", encoding="utf-8") as f:
        html = f.read()
    replacements = {
        "{{XAUUSD}}": f"{xauusd:.2f}",
        "{{XAGUSD}}": f"{xagusd:.2f}",
        "{{XAUPLN}}": f"{xaupln:.2f}" if xaupln is not None else "—",
        "{{XAGPLN}}": f"{xagpln:.2f}" if xagpln is not None else "—",
        "{{DATE}}": last_date,
    }
    for key, val in replacements.items():
        html = html.replace(key, val)
    # Fallback: replace inside strong tags if tokens are missing
    def rep(id_, value, src):
        return re.sub(
            rf'(<strong id="{id_}">)([^<]*)(</strong>)',
            lambda m: f"{m.group(1)}{value}{m.group(3)}",
            src,
        )
    html = rep("rate-xauusd", replacements["{{XAUUSD}}"], html)
    html = rep("rate-xagusd", replacements["{{XAGUSD}}"], html)
    html = rep("rate-xaupln", replacements["{{XAUPLN}}"], html)
    html = rep("rate-xagpln", replacements["{{XAGPLN}}"], html)
    html = rep("rate-date", replacements["{{DATE}}"], html)
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)

def git_sync(repo_dir: str):
    if not os.path.isdir(os.path.join(repo_dir, ".git")):
        print("Git repo not initialized; skipping push.")
        return
    def run(cmd):
        subprocess.run(cmd, check=True, cwd=repo_dir)

    # Stage only DBs, plots, and index page
    run(["git", "add", "goldprice.db", "silverprice.db", "GSPLN.db", "plots/", "index.html"])
    # Commit only if there are staged changes
    status = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo_dir)
    if status.returncode == 0:
        print("Git: no changes to push.")
        return
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    run(["git", "commit", "-m", f"Update data and plots ({stamp})"])
    run(["git", "push"])

def main():
    here = os.path.dirname(__file__)
    gold_script = os.path.join(here, "goldprice.py")
    silver_script = os.path.join(here, "silverprice.py")
    gold_db = os.path.join(here, "goldprice.db")
    silver_db = os.path.join(here, "silverprice.db")

    yesterday = date.today() - timedelta(days=1)
    if yesterday < MIN_DATE:
        print("Yesterday is before MIN_DATE; nothing to do.")
        return

    # Gold
    gold_latest = get_latest_date(gold_db)
    gold_start = MIN_DATE if not gold_latest else gold_latest + timedelta(days=1)
    if gold_start <= yesterday:
        run_range(gold_script, gold_start, yesterday)
    else:
        print("Gold: already up to date.")

    # Silver
    silver_latest = get_latest_date(silver_db)
    silver_start = MIN_DATE if not silver_latest else silver_latest + timedelta(days=1)
    if silver_start <= yesterday:
        run_range(silver_script, silver_start, yesterday)
    else:
        print("Silver: already up to date.")

    # Refresh plots
    plot_script = os.path.join(here, "plot.py")
    cmd = [sys.executable, plot_script]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    update_index_html(here, os.path.join(here, "GSPLN.db"))

    # Push DBs + plots to GitHub
    git_sync(here)


if __name__ == "__main__":
    main()
