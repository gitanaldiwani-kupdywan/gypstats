#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Rectangle

NBP_USDPLN_URL = "https://api.nbp.pl/api/exchangerates/rates/a/usd/{date}/?format=json"


def fetch_usdpln(date_str: str, cache: dict):
    if date_str in cache:
        return cache[date_str]
    url = NBP_USDPLN_URL.format(date=date_str)
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "GypStats/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        rate = data["rates"][0]["mid"]
        cache[date_str] = rate
        return rate
    except urllib.error.HTTPError:
        cache[date_str] = None
        return None


def load_series(db_path: str, column: str):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            """
            SELECT date, {col}
            FROM metal_prices
            WHERE {col} IS NOT NULL
            ORDER BY date ASC
            """.format(col=column)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return [], []
    dates, values = zip(*rows)
    return list(dates), list(values)


def load_joined_series(gold_db: str, silver_db: str):
    # Join by date where both series exist.
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("ATTACH DATABASE ? AS gold", (gold_db,))
        conn.execute("ATTACH DATABASE ? AS silver", (silver_db,))
        cur = conn.execute(
            """
            SELECT g.date, g.xauusd, s.xagusd
            FROM gold.metal_prices g
            JOIN silver.metal_prices s ON s.date = g.date
            WHERE g.xauusd IS NOT NULL AND s.xagusd IS NOT NULL
            ORDER BY g.date ASC
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return [], [], []
    dates, xauusd, xagusd = zip(*rows)
    return list(dates), list(xauusd), list(xagusd)


def ensure_gsp_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gsp (
            date TEXT PRIMARY KEY,
            xauusd REAL NOT NULL,
            xagusd REAL NOT NULL,
            gsr REAL NOT NULL,
            usdpln REAL,
            xaupln REAL,
            xagpln REAL
        )
        """
    )
    cur = conn.execute("PRAGMA table_info(gsp)")
    cols = {row[1] for row in cur.fetchall()}
    for col, coltype in (("usdpln", "REAL"), ("xaupln", "REAL"), ("xagpln", "REAL")):
        if col not in cols:
            conn.execute(f"ALTER TABLE gsp ADD COLUMN {col} {coltype}")


def write_gspln_db(db_path: str, dates, xauusd, xagusd):
    conn = sqlite3.connect(db_path)
    try:
        ensure_gsp_table(conn)
        cur = conn.execute("SELECT date, usdpln, xaupln, xagpln FROM gsp")
        existing = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}
        rows = []
        usd_cache = {}
        usdpln_list = []
        xaupln_list = []
        xagpln_list = []
        for d, g, s in zip(dates, xauusd, xagusd):
            if s:
                usdpln, xaupln, xagpln = existing.get(d, (None, None, None))
                if usdpln is None or xaupln is None or xagpln is None:
                    usdpln = fetch_usdpln(d, usd_cache)
                    if usdpln is None:
                        # Try up to 7 days back for last available NBP fixing.
                        back = datetime.strptime(d, "%Y-%m-%d")
                        for _ in range(7):
                            back = back.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
                            ds = back.strftime("%Y-%m-%d")
                            usdpln = fetch_usdpln(ds, usd_cache)
                            if usdpln is not None:
                                break
                    if usdpln is not None:
                        xaupln = float(g) * float(usdpln)
                        xagpln = float(s) * float(usdpln)
                    else:
                        xaupln = None
                        xagpln = None
                rows.append((d, float(g), float(s), float(g) / float(s), usdpln, xaupln, xagpln))
                usdpln_list.append(usdpln)
                xaupln_list.append(xaupln)
                xagpln_list.append(xagpln)
        conn.executemany(
            "INSERT OR REPLACE INTO gsp (date, xauusd, xagusd, gsr, usdpln, xaupln, xagpln) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()
    return usdpln_list, xaupln_list, xagpln_list


def main():
    parser = argparse.ArgumentParser(description="Plot XAUUSD/XAGUSD/GSR over time from SQLite")
    parser.add_argument("--gold-db", default="goldprice.db", help="Path to gold DB (default: goldprice.db)")
    parser.add_argument("--silver-db", default="silverprice.db", help="Path to silver DB (default: silverprice.db)")
    parser.add_argument("--out-gold", default="", help="Optional output image for gold (e.g. xauusd.png)")
    parser.add_argument("--out-silver", default="", help="Optional output image for silver (e.g. xagusd.png)")
    parser.add_argument("--out-all", default="", help="Optional output image for combined plot (e.g. all.png)")
    parser.add_argument("--gspln-db", default="GSPLN.db", help="Output DB for joined series (default: GSPLN.db)")
    parser.add_argument("--show", action="store_true", help="Show interactive window")
    args = parser.parse_args()

    gold_db = args.gold_db
    silver_db = args.silver_db
    if not os.path.isabs(gold_db):
        gold_db = os.path.join(os.path.dirname(__file__), gold_db)
    if not os.path.isabs(silver_db):
        silver_db = os.path.join(os.path.dirname(__file__), silver_db)
    gspln_db = args.gspln_db
    if not os.path.isabs(gspln_db):
        gspln_db = os.path.join(os.path.dirname(__file__), gspln_db)
    plots_dir = os.path.join(os.path.dirname(__file__), "plots")
    os.makedirs(plots_dir, exist_ok=True)

    if not os.path.exists(gold_db):
        print(f"Error: gold DB not found: {gold_db}", file=sys.stderr)
        sys.exit(2)
    if not os.path.exists(silver_db):
        print(f"Error: silver DB not found: {silver_db}", file=sys.stderr)
        sys.exit(2)

    gold_dates, gold_values = load_series(gold_db, "xauusd")
    if not gold_dates:
        print("No xauusd data to plot.", file=sys.stderr)
        sys.exit(1)
    silver_dates, silver_values = load_series(silver_db, "xagusd")
    if not silver_dates:
        print("No xagusd data to plot.", file=sys.stderr)
        sys.exit(1)

    def plot_one(dates, values, title, ylabel, ax, color=None, linewidth=1.5, linestyle="-", zero_min=False):
        ax.plot(dates, values, linewidth=linewidth, color=color, linestyle=linestyle)
        ax.set_title(title)
        ax.set_xlabel("Date")
        ax.set_ylabel(ylabel)
        if zero_min:
            ax.set_ylim(bottom=0)
        if values:
            vmin = min(values)
            vmax = max(values)
            ax.text(
                0.99,
                0.04,
                f"min {vmin:.2f} · max {vmax:.2f}",
                transform=ax.transAxes,
                ha="right",
                va="bottom",
                fontsize=8,
                color="#9aa3ad",
            )
        for label in ax.get_xticklabels():
            label.set_rotation(45)

    plot_one(
        gold_dates,
        gold_values,
        "XAUUSD (USD per troy oz) over time",
        "USD / XAU",
        plt.gca(),
        color="#d4af37",
        linewidth=2.6,
        zero_min=True,
    )
    gold_out = args.out_gold
    if not gold_out and not args.show:
        gold_out = os.path.join("plots", "xauusd.png")
    if gold_out:
        if not os.path.isabs(gold_out):
            gold_out = os.path.join(os.path.dirname(__file__), gold_out)
        plt.savefig(gold_out, dpi=150)
        print(f"Saved plot to {gold_out}")
    if args.show:
        plt.show()

    plot_one(
        silver_dates,
        silver_values,
        "XAGUSD (USD per troy oz) over time",
        "USD / XAG",
        plt.gca(),
        color="#c0c0c0",
        linewidth=2.6,
        zero_min=True,
    )
    silver_out = args.out_silver
    if not silver_out and not args.show:
        silver_out = os.path.join("plots", "xagusd.png")
    if silver_out:
        if not os.path.isabs(silver_out):
            silver_out = os.path.join(os.path.dirname(__file__), silver_out)
        plt.savefig(silver_out, dpi=150)
        print(f"Saved plot to {silver_out}")
    if args.show:
        plt.show()

    joined_dates, joined_xauusd, joined_xagusd = load_joined_series(gold_db, silver_db)
    if joined_dates:
        usdpln_list, xaupln_list, xagpln_list = write_gspln_db(gspln_db, joined_dates, joined_xauusd, joined_xagusd)
        gsr_values = [g / s for g, s in zip(joined_xauusd, joined_xagusd)]

        fig, axes = plt.subplots(7, 1, figsize=(10, 18), sharex=True)
        # Banner-style header with a carpet-like pattern
        banner = Rectangle(
            (0.0, 0.92),
            1.0,
            0.08,
            transform=fig.transFigure,
            facecolor="#3a0f16",
            edgecolor="#ffd36b",
            linewidth=2.5,
            hatch="xx..",
            alpha=0.98,
            zorder=2,
        )
        fig.add_artist(banner)
        font_path = os.path.join(os.path.dirname(__file__), "fonts", "CinzelDecorative-Bold.ttf")
        if not os.path.exists(font_path):
            font_path = os.path.join(os.path.dirname(__file__), "fonts", "CinzelDecorative-Regular.ttf")
        fp = font_manager.FontProperties(fname=font_path) if os.path.exists(font_path) else None
        fig.suptitle(
            "GypStats",
            fontproperties=fp,
            fontsize=32,
            fontweight="bold",
            color="#ffe7a6",
            y=0.98,
            bbox=dict(
                boxstyle="round,pad=0.35",
                facecolor="#2b0a12",
                edgecolor="#ffd36b",
                linewidth=3.0,
            ),
        )
        # Secondary banner with live timestamps
        sub_banner = Rectangle(
            (0.0, 0.865),
            1.0,
            0.045,
            transform=fig.transFigure,
            facecolor="#0f1a24",
            edgecolor="#6cc1ff",
            linewidth=2.0,
            hatch="..//",
            alpha=0.95,
            zorder=2,
        )
        fig.add_artist(sub_banner)
        now_valencia = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%Y-%m-%d %H:%M")
        now_przewalsk = datetime.now(ZoneInfo("Asia/Bishkek")).strftime("%Y-%m-%d %H:%M")
        now_fakfak = datetime.now(ZoneInfo("Asia/Jayapura")).strftime("%Y-%m-%d %H:%M")
        sub_text = (
            f"LAST UPDATE  {now_valencia}  Valencia  •  "
            f"{now_przewalsk}  Przewalsk  •  "
            f"{now_fakfak}  Fak-fak"
        )
        fig.text(
            0.5,
            0.887,
            sub_text,
            ha="center",
            va="center",
            fontsize=10,
            color="#cfe8ff",
            fontweight="bold",
            family="DejaVu Sans",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="#0b121a", edgecolor="#6cc1ff", linewidth=1.0, alpha=0.95),
        )
        plot_one(
            joined_dates,
            joined_xauusd,
            "XAUUSD (USD per troy oz)",
            "USD / XAU",
            axes[0],
            color="#d4af37",
            linewidth=2.6,
            zero_min=True,
        )
        plot_one(
            joined_dates,
            joined_xagusd,
            "XAGUSD (USD per troy oz)",
            "USD / XAG",
            axes[1],
            color="#c0c0c0",
            linewidth=2.6,
            zero_min=True,
        )
        plot_one(joined_dates, gsr_values, "GSR (XAUUSD / XAGUSD)", "Ratio", axes[2], color="#d64545", linewidth=2.0, linestyle=":")
        # First derivative (day-to-day change)
        def derivative(series):
            if not series:
                return series
            diffs = [0.0]
            for i in range(1, len(series)):
                diffs.append(float(series[i]) - float(series[i - 1]))
            return diffs

        d_xau = derivative(joined_xauusd)
        d_xag = derivative(joined_xagusd)
        d_gsr = derivative(gsr_values)
        plot_one(joined_dates, d_xau, "dXAUUSD (day-to-day change)", "Δ XAUUSD", axes[3], color="#d4af37", linewidth=2.0)
        plot_one(joined_dates, d_xag, "dXAGUSD (day-to-day change)", "Δ XAGUSD", axes[4], color="#c0c0c0", linewidth=2.0, linestyle="--")
        plot_one(joined_dates, d_gsr, "dGSR (day-to-day change)", "Δ GSR", axes[5], color="#d64545", linewidth=2.0, linestyle=":")
        # Trend overlay (normalized to first value) for visual comparison
        def normalize(series):
            if not series or series[0] == 0:
                return series
            base = float(series[0])
            return [float(v) / base for v in series]

        axes[6].plot(
            joined_dates,
            normalize(joined_xauusd),
            linestyle="-",
            linewidth=2.6,
            color="#d4af37",
            label="XAUUSD",
        )
        axes[6].plot(
            joined_dates,
            normalize(joined_xagusd),
            linestyle="--",
            linewidth=2.6,
            color="#c0c0c0",
            label="XAGUSD",
        )
        axes[6].plot(
            joined_dates,
            normalize(gsr_values),
            linestyle=":",
            linewidth=2.0,
            color="#d64545",
            label="GSR",
        )
        axes[6].set_title("Trends overlay (normalized)")
        axes[6].set_ylabel("Index")
        axes[6].legend(loc="upper left")
        for label in axes[6].get_xticklabels():
            label.set_rotation(45)
        fig.tight_layout(rect=(0, 0, 1, 0.865))

        all_out = args.out_all
        if not all_out and not args.show:
            all_out = os.path.join("plots", "all.png")
        if all_out:
            if not os.path.isabs(all_out):
                all_out = os.path.join(os.path.dirname(__file__), all_out)
            fig.savefig(all_out, dpi=150)
            print(f"Saved plot to {all_out}")
        if args.show:
            plt.show()

        # PLN-based plots (filter out missing PLN rates)
        pln_dates = []
        pln_xau = []
        pln_xag = []
        pln_gsr = []
        for d, xp, sp, gsr in zip(joined_dates, xaupln_list, xagpln_list, gsr_values):
            if xp is None or sp is None:
                continue
            pln_dates.append(d)
            pln_xau.append(xp)
            pln_xag.append(sp)
            pln_gsr.append(gsr)

        if pln_dates:
            fig_pln, axes_pln = plt.subplots(4, 1, figsize=(10, 12), sharex=True)
            banner_pln = Rectangle(
                (0.0, 0.92),
                1.0,
                0.08,
                transform=fig_pln.transFigure,
                facecolor="#ffffff",
                edgecolor="#d32f2f",
                linewidth=2.8,
                hatch="..//",
                alpha=0.98,
                zorder=2,
            )
            fig_pln.add_artist(banner_pln)
            sub_banner_pln = Rectangle(
                (0.0, 0.865),
                1.0,
                0.045,
                transform=fig_pln.transFigure,
                facecolor="#d32f2f",
                edgecolor="#ffffff",
                linewidth=2.2,
                hatch="..//",
                alpha=0.95,
                zorder=2,
            )
            fig_pln.add_artist(sub_banner_pln)
            fig_pln.suptitle(
                "GypStats PLN",
                fontproperties=fp,
                fontsize=30,
                fontweight="bold",
                color="#d32f2f",
                y=0.98,
                bbox=dict(
                    boxstyle="round,pad=0.35",
                    facecolor="#ffffff",
                    edgecolor="#d32f2f",
                    linewidth=3.0,
                ),
            )
            fig_pln.text(
                0.5,
                0.887,
                sub_text,
                ha="center",
                va="center",
                fontsize=10,
                color="#ffffff",
                fontweight="bold",
                family="DejaVu Sans",
                bbox=dict(
                    boxstyle="round,pad=0.25",
                    facecolor="#b71c1c",
                    edgecolor="#ffffff",
                    linewidth=1.0,
                    alpha=0.95,
                ),
            )
            plot_one(pln_dates, pln_xau, "XAUPLN (PLN per troy oz)", "PLN / XAU", axes_pln[0], color="#d4af37", linewidth=2.6, zero_min=True)
            plot_one(pln_dates, pln_xag, "XAGPLN (PLN per troy oz)", "PLN / XAG", axes_pln[1], color="#c0c0c0", linewidth=2.6, zero_min=True)
            plot_one(pln_dates, pln_gsr, "GSR (XAUUSD / XAGUSD)", "Ratio", axes_pln[2], color="#d64545", linewidth=2.0, linestyle=":")

            def normalize(series):
                if not series or series[0] == 0:
                    return series
                base = float(series[0])
                return [float(v) / base for v in series]

            axes_pln[3].plot(pln_dates, normalize(pln_xau), linestyle="-", linewidth=2.6, color="#d4af37", label="XAUPLN")
            axes_pln[3].plot(pln_dates, normalize(pln_xag), linestyle="--", linewidth=2.6, color="#c0c0c0", label="XAGPLN")
            axes_pln[3].plot(pln_dates, normalize(pln_gsr), linestyle=":", linewidth=2.0, color="#d64545", label="GSR")
            axes_pln[3].set_title("Trends overlay (normalized)")
            axes_pln[3].set_ylabel("Index")
            axes_pln[3].legend(loc="upper left")
            for label in axes_pln[3].get_xticklabels():
                label.set_rotation(45)
            fig_pln.tight_layout(rect=(0, 0, 1, 0.865))

            allpl_out = os.path.join("plots", "allpl.png")
            if not os.path.isabs(allpl_out):
                allpl_out = os.path.join(os.path.dirname(__file__), allpl_out)
            fig_pln.savefig(allpl_out, dpi=150)
            print(f"Saved plot to {allpl_out}")
            if args.show:
                plt.show()
        else:
            print("No PLN data available to plot allpl.png.", file=sys.stderr)
    else:
        print("No joined xauusd/xagusd data to compute GSR.", file=sys.stderr)


if __name__ == "__main__":
    main()
