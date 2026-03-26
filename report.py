"""
NEPSE Daily Report Generator
==============================
Reads today's live_feed.csv, transforms the data,
plots a closing-price line graph per company,
builds a single PDF with all companies, and
emails the PDF to a specified address.

Called automatically by Task Scheduler at 3:50 PM daily.

Usage:
    python report.py
"""

import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from zoneinfo import ZoneInfo

# ── Email Configuration ───────────────────────────────────────────────────────
# 1. Go to myaccount.google.com → Security → 2-Step Verification (enable it)
# 2. Then → App Passwords → create one → paste the 16-char password below
EMAIL_SENDER   = "080bct046@ioepc.edu.np"        # Gmail address you send FROM
EMAIL_PASSWORD = "owzt ccjz kwqk rgwx"         # 16-char Gmail App Password
EMAIL_RECEIVER = "080bct035@ioepc.edu.np"   # address to send the PDF to
SMTP_HOST      = "smtp.gmail.com"
SMTP_PORT      = 587

import pandas as pd
import matplotlib
matplotlib.use("Agg")           # headless — no display needed
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    Image as RLImage, HRFlowable, PageBreak, Table, TableStyle
)

# ── Paths ─────────────────────────────────────────────────────────────────────
NPT      = ZoneInfo("Asia/Kathmandu")
BASE_DIR = r"C:\Codes\final_etl"
DATA_DIR = os.path.join(BASE_DIR, "nepse_data")
CSV_PATH = os.path.join(DATA_DIR, "live_feed.csv")
OUT_DIR  = os.path.join(BASE_DIR, "nepse_data", "reports")

# ── Colours ───────────────────────────────────────────────────────────────────
BRAND_DARK  = colors.HexColor("#0d1b2a")
BRAND_BLUE  = colors.HexColor("#1565c0")
BRAND_LIGHT = colors.HexColor("#e3f2fd")
GREEN       = colors.HexColor("#2e7d32")
RED         = colors.HexColor("#c62828")
GREY        = colors.HexColor("#607d8b")

CIRCUIT_BREAKERS = [
    (10.0,  0,   True),
    ( 6.0, 40,  False),
    ( 4.0, 20,  False),
]


def log(msg: str):
    ts = datetime.now(NPT).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]  {msg}", flush=True)
    log_path = os.path.join(DATA_DIR, "scheduler.log")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}]  {msg}\n")


# ─────────────────────────────────────────────────────────────────────────────
# 1. EXTRACT & TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def load_today(csv_path: str) -> pd.DataFrame | None:
    """Load and clean today's rows from live_feed.csv."""
    if not os.path.exists(csv_path):
        log(f"CSV not found: {csv_path}")
        return None

    df = pd.read_csv(csv_path)

    if df.empty:
        log("CSV is empty.")
        return None

    # Parse timestamps safely; skip malformed rows instead of crashing report generation
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    bad_ts = int(df["fetched_at"].isna().sum())
    if bad_ts:
        log(f"Dropped {bad_ts} row(s) with invalid fetched_at timestamps.")
        df = df[df["fetched_at"].notna()].copy()

    # Filter to today (NPT)
    today_str = datetime.now(NPT).strftime("%Y-%m-%d")
    df = df[df["fetched_at"].dt.strftime("%Y-%m-%d") == today_str].copy()

    if df.empty:
        log(f"No rows for today ({today_str}) in CSV.")
        return None

    # Cast numerics, drop rows with no close
    for col in ["open", "high", "low", "close", "volume", "prev_close", "pct_change"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.dropna(subset=["close"], inplace=True)
    df.sort_values(["symbol", "fetched_at"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    log(f"Loaded {len(df)} rows for {df['symbol'].nunique()} symbol(s) — {today_str}")
    return df


def compute_summary(sym_df: pd.DataFrame) -> dict:
    """Compute per-symbol stats."""
    first_close  = sym_df.iloc[0]["close"]
    last_close   = sym_df.iloc[-1]["close"]
    change       = round(last_close - first_close, 2)
    change_pct   = round((change / first_close) * 100, 2) if first_close else 0
    high         = sym_df["high"].max()
    low          = sym_df["low"].min()
    total_vol    = sym_df["volume"].iloc[-1]
    first_vol    = sym_df["volume"].iloc[0]

    # Find time of day high and low close
    high_time = sym_df.loc[sym_df["close"].idxmax(), "fetched_at"].strftime("%H:%M")
    low_time  = sym_df.loc[sym_df["close"].idxmin(), "fetched_at"].strftime("%H:%M")

    # Price movement description
    closes = sym_df["close"].values
    if len(closes) >= 3:
        first_half = closes[:len(closes)//2]
        second_half = closes[len(closes)//2:]
        if first_half[-1] > first_half[0] and second_half[-1] < second_half[0]:
            trend = "rose in the first half of the session then pulled back toward close"
        elif first_half[-1] < first_half[0] and second_half[-1] > second_half[0]:
            trend = "fell early in the session then recovered toward close"
        elif closes[-1] > closes[0]:
            trend = "moved gradually higher through the session"
        elif closes[-1] < closes[0]:
            trend = "drifted lower through the session"
        else:
            trend = "traded flat through the session"
    else:
        trend = "had limited trading data today"

    # Volume analysis
    vol_added = total_vol - first_vol
    if total_vol > 100000:
        vol_comment = "Volume was high today, indicating strong trader interest."
    elif total_vol > 50000:
        vol_comment = "Volume was moderate today."
    else:
        vol_comment = "Volume was light today, suggesting limited activity."

    # Circuit breaker check
    pct_change = sym_df.iloc[-1]["pct_change"] if "pct_change" in sym_df.columns else None
    cb_status  = "No circuit breaker was triggered."
    if pct_change and not pd.isna(pct_change):
        pct = float(pct_change)
        if abs(pct) >= 10:
            cb_status = f"The 10% circuit breaker was triggered. Market closed early."
        elif abs(pct) >= 6:
            cb_status = f"A 6% circuit breaker halt was triggered (40-minute halt)."
        elif abs(pct) >= 4:
            cb_status = f"A 4% circuit breaker halt was triggered (20-minute halt)."

    return {
        "first_close":  first_close,
        "last_close":   last_close,
        "change":       change,
        "change_pct":   change_pct,
        "high":         high,
        "low":          low,
        "volume":       total_vol,
        "vol_added":    vol_added,
        "polls":        len(sym_df),
        "high_time":    high_time,
        "low_time":     low_time,
        "trend":        trend,
        "vol_comment":  vol_comment,
        "cb_status":    cb_status,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. PLOT
# ─────────────────────────────────────────────────────────────────────────────

def plot_symbol(sym: str, sym_df: pd.DataFrame, summary: dict,
                out_path: str):
    """Clean, simple line chart — white background, no fancy styling."""
    fig, ax = plt.subplots(figsize=(9, 3.2))

    x = sym_df["fetched_at"].values
    y = sym_df["close"].values

    color = "#2e7d32" if summary["change"] >= 0 else "#c62828"

    ax.plot(x, y, color=color, linewidth=1.5, marker="o",
            markersize=4, markerfacecolor=color)

    # Light shading under line
    ax.fill_between(x, y, min(y) * 0.999, alpha=0.08, color=color)

    # Label first and last value
    ax.annotate(f"{y[0]:.1f}", xy=(x[0], y[0]),
                xytext=(6, 6), textcoords="offset points",
                fontsize=8, color="#555555")
    ax.annotate(f"{y[-1]:.1f}", xy=(x[-1], y[-1]),
                xytext=(-38, 6), textcoords="offset points",
                fontsize=8, fontweight="bold", color=color)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
    ax.tick_params(labelsize=8)
    ax.grid(True, color="#eeeeee", linewidth=0.6)

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    for spine in ["bottom", "left"]:
        ax.spines[spine].set_color("#cccccc")

    ax.set_xlabel("Time (NPT)", fontsize=8, color="#555555")
    ax.set_ylabel("Close Price (NPR)", fontsize=8, color="#555555")

    plt.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    log(f"  Chart saved -> {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. BUILD PDF
# ─────────────────────────────────────────────────────────────────────────────

def build_pdf(df: pd.DataFrame, chart_dir: str, pdf_path: str):
    """Assemble the full multi-company PDF report."""
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=1.8*cm, rightMargin=1.8*cm,
        topMargin=1.5*cm,  bottomMargin=1.5*cm,
    )

    styles = getSampleStyleSheet()
    story  = []

    # ── Shared styles ─────────────────────────────────────────────────────────
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=22, textColor=BRAND_DARK,
        spaceAfter=4, alignment=TA_CENTER,
    )
    sub_style = ParagraphStyle(
        "SubTitle",
        parent=styles["Normal"],
        fontSize=10, textColor=GREY,
        spaceAfter=2, alignment=TA_CENTER,
    )
    company_style = ParagraphStyle(
        "CompanyName",
        parent=styles["Heading1"],
        fontSize=16, textColor=BRAND_BLUE,
        spaceBefore=10, spaceAfter=4,
    )
    label_style = ParagraphStyle(
        "Label",
        parent=styles["Normal"],
        fontSize=9, textColor=GREY,
    )
    value_style = ParagraphStyle(
        "Value",
        parent=styles["Normal"],
        fontSize=11, textColor=BRAND_DARK,
        fontName="Helvetica-Bold",
    )

    today_str    = datetime.now(NPT).strftime("%A, %d %B %Y")
    generated_at = datetime.now(NPT).strftime("%H:%M NPT")

    # ── Cover header ──────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.4*cm))
    story.append(Paragraph("NEPSE Daily Market Report", title_style))
    story.append(Paragraph(f"{today_str}  ·  Generated {generated_at}", sub_style))
    story.append(HRFlowable(width="100%", thickness=1.5,
                            color=BRAND_BLUE, spaceAfter=12))

    symbols = df["symbol"].unique()

    for i, sym in enumerate(symbols):
        sym_df  = df[df["symbol"] == sym].copy()
        summary = compute_summary(sym_df)

        # Company heading
        story.append(Paragraph(sym, company_style))
        story.append(HRFlowable(width="100%", thickness=0.5,
                                color=BRAND_LIGHT, spaceAfter=6))

        # Stats table
        change_color = GREEN if summary["change"] >= 0 else RED
        sign         = "▲" if summary["change"] >= 0 else "▼"

        data = [
            ["Open", "High", "Low", "Last Close", "Day Change", "Volume", "Polls"],
            [
                f"NPR {summary['first_close']:.2f}",
                f"NPR {summary['high']:.2f}",
                f"NPR {summary['low']:.2f}",
                f"NPR {summary['last_close']:.2f}",
                f"{sign} {summary['change']:+.2f} ({summary['change_pct']:+.2f}%)",
                f"{summary['volume']:,}",
                str(summary['polls']),
            ],
        ]

        col_w = [(A4[0] - 3.6*cm) / 7] * 7
        tbl   = Table(data, colWidths=col_w)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, 0), BRAND_DARK),
            ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
            ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1, 0), 8),
            ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("BACKGROUND",   (0, 1), (-1, 1), BRAND_LIGHT),
            ("FONTNAME",     (0, 1), (-1, 1), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 1), (-1, 1), 9),
            ("TEXTCOLOR",    (4, 1), (4, 1),  change_color),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BRAND_LIGHT]),
            ("GRID",         (0, 0), (-1, -1), 0.4, colors.white),
            ("TOPPADDING",   (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 0.3*cm))

        # Chart
        chart_path = os.path.join(chart_dir, f"{sym}_chart.png")
        if os.path.exists(chart_path):
            page_w = A4[0] - 3.6*cm
            story.append(RLImage(chart_path, width=page_w, height=page_w * 0.38))

        story.append(Spacer(1, 0.3*cm))

        # ── Day summary ───────────────────────────────────────────────────────
        direction  = "gained" if summary["change"] >= 0 else "lost"
        day_summary = (
            f"{sym} opened at NPR {summary['first_close']:.2f} and closed at "
            f"NPR {summary['last_close']:.2f}, {direction} NPR {abs(summary['change']):.2f} "
            f"({abs(summary['change_pct']):.2f}%) on the day. "
            f"The stock {summary['trend']}. "
            f"It reached its intraday high close of NPR {summary['high']:.2f} around {summary['high_time']} "
            f"and its low of NPR {summary['low']:.2f} around {summary['low_time']}."
        )
        story.append(Paragraph("<b>Day Summary</b>", styles["Heading2"]))
        story.append(Paragraph(day_summary, styles["Normal"]))
        story.append(Spacer(1, 0.2*cm))

        # ── Volume analysis ───────────────────────────────────────────────────
        vol_text = (
            f"Total volume recorded was {summary['volume']:,} shares. "
            f"{summary['vol_comment']}"
        )
        story.append(Paragraph("<b>Volume</b>", styles["Heading2"]))
        story.append(Paragraph(vol_text, styles["Normal"]))
        story.append(Spacer(1, 0.2*cm))

        # ── Circuit breaker status ────────────────────────────────────────────
        story.append(Paragraph("<b>Circuit Breaker</b>", styles["Heading2"]))
        story.append(Paragraph(summary["cb_status"], styles["Normal"]))

        # Page break between companies (not after the last one)
        if i < len(symbols) - 1:
            story.append(PageBreak())

    # ── Footer note ───────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.8*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=GREY))
    story.append(Spacer(1, 0.2*cm))
    footer = ParagraphStyle("footer", parent=styles["Normal"],
                            fontSize=7, textColor=GREY, alignment=TA_CENTER)
    story.append(Paragraph(
        "Data sourced from merolagani.com  ·  For personal use only  ·  "
        f"Report generated {today_str} at {generated_at}",
        footer
    ))

    doc.build(story)
    log(f"PDF saved → {pdf_path}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def send_email(pdf_path: str):
    """
    Send the PDF report as an email attachment via Gmail SMTP.
    All steps and errors are written to scheduler.log.
    """
    today_str    = datetime.now(NPT).strftime("%A, %d %B %Y")
    generated_at = datetime.now(NPT).strftime("%H:%M NPT")
    subject      = f"NEPSE Daily Report — {today_str}"

    # ── Check placeholders ────────────────────────────────────────────────────
    if "your_email" in EMAIL_SENDER or "xxxx" in EMAIL_PASSWORD:
        log("EMAIL SKIPPED — placeholders not filled in.")
        log("  Edit EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER in report.py")
        return

    # ── Build email ───────────────────────────────────────────────────────────
    log(f"  Preparing email to {EMAIL_RECEIVER} ...")
    msg = MIMEMultipart()
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_RECEIVER
    msg["Subject"] = subject

    body = (
        f"Hi,\n\n"
        f"Please find attached the NEPSE daily market report for {today_str}.\n"
        f"Generated at {generated_at}.\n\n"
        f"Report includes: price charts, OHLCV summary, and day change "
        f"for all tracked symbols.\n\n"
        f"— NEPSE ETL"
    )
    msg.attach(MIMEText(body, "plain"))

    # ── Attach PDF ────────────────────────────────────────────────────────────
    log(f"  Attaching PDF: {os.path.basename(pdf_path)} ...")
    try:
        with open(pdf_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={os.path.basename(pdf_path)}"
        )
        msg.attach(part)
        log("  PDF attached successfully.")
    except Exception as e:
        log(f"  FAILED to attach PDF: {e}")
        return

    # ── Send via Gmail SMTP ───────────────────────────────────────────────────
    log(f"  Connecting to {SMTP_HOST}:{SMTP_PORT} ...")
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            log("  EHLO OK")
            server.starttls()
            log("  STARTTLS OK")
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            log(f"  Login OK as {EMAIL_SENDER}")
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
            log(f"  Email sent successfully to {EMAIL_RECEIVER}")
    except smtplib.SMTPAuthenticationError as e:
        log(f"  SMTP AUTH FAILED: {e}")
        log("  → Check EMAIL_SENDER and EMAIL_PASSWORD in report.py")
        log("  → Make sure you used an App Password, not your Gmail password")
    except smtplib.SMTPException as e:
        log(f"  SMTP ERROR: {e}")
    except Exception as e:
        log(f"  EMAIL FAILED (unexpected): {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run():
    log("=" * 60)
    log("Report generator started")
    log(f"  BASE_DIR : {BASE_DIR}")
    log(f"  CSV_PATH : {CSV_PATH}")
    log(f"  OUT_DIR  : {OUT_DIR}")
    log("=" * 60)

    # ── Step 0: Create output dirs ────────────────────────────────────────────
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
        log("STEP 0 OK — output dirs created")
    except Exception as e:
        log(f"STEP 0 FAILED — could not create output dirs: {e}")
        sys.exit(1)

    # ── Step 1: Load & transform CSV ─────────────────────────────────────────
    log("STEP 1 — loading and transforming CSV ...")
    try:
        df = load_today(CSV_PATH)
    except Exception as e:
        log(f"STEP 1 FAILED — unexpected error in load_today: {e}")
        import traceback
        log(traceback.format_exc())
        sys.exit(1)

    if df is None:
        log("STEP 1 — no data for today. Nothing to report. Exiting.")
        log(f"  Check that fetcher.py ran today and {CSV_PATH} exists.")
        sys.exit(0)

    log(f"STEP 1 OK — {len(df)} rows, {df['symbol'].nunique()} symbol(s): "
        f"{list(df['symbol'].unique())}")

    # ── Step 2: Generate charts ───────────────────────────────────────────────
    log("STEP 2 — generating charts ...")
    chart_dir = os.path.join(OUT_DIR, "charts")
    try:
        os.makedirs(chart_dir, exist_ok=True)
    except Exception as e:
        log(f"STEP 2 FAILED — could not create chart dir: {e}")
        sys.exit(1)

    for sym in df["symbol"].unique():
        log(f"  Plotting {sym} ...")
        try:
            sym_df  = df[df["symbol"] == sym].copy()
            summary = compute_summary(sym_df)
            log(f"    {sym} summary: open={summary['first_close']} "
                f"close={summary['last_close']} change={summary['change']:+.2f} "
                f"polls={summary['polls']}")
            chart_p = os.path.join(chart_dir, f"{sym}_chart.png")
            plot_symbol(sym, sym_df, summary, chart_p)
            log(f"  STEP 2 OK — {sym} chart saved")
        except Exception as e:
            log(f"  STEP 2 FAILED for {sym}: {e}")
            import traceback
            log(traceback.format_exc())
            # continue with other symbols instead of crashing

    # ── Step 3: Build PDF ─────────────────────────────────────────────────────
    log("STEP 3 — building PDF ...")
    today_str = datetime.now(NPT).strftime("%Y-%m-%d")
    pdf_name  = f"NEPSE_Report_{today_str}.pdf"
    pdf_path  = os.path.join(OUT_DIR, pdf_name)
    log(f"  PDF path: {pdf_path}")
    try:
        build_pdf(df, chart_dir, pdf_path)
        log(f"STEP 3 OK — PDF built: {pdf_path}")
    except Exception as e:
        log(f"STEP 3 FAILED — PDF build error: {e}")
        import traceback
        log(traceback.format_exc())
        sys.exit(1)

    # ── Step 4: Send email ────────────────────────────────────────────────────
    log("STEP 4 — sending email ...")
    try:
        send_email(pdf_path)
        log("STEP 4 OK — email step complete")
    except Exception as e:
        log(f"STEP 4 FAILED — email error: {e}")
        import traceback
        log(traceback.format_exc())

    log("=" * 60)
    log("Report generation complete.")
    log("=" * 60)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        import traceback
        # Last-resort catch — write to log before dying
        NPT_      = ZoneInfo("Asia/Kathmandu")
        ts        = datetime.now(NPT_).strftime("%Y-%m-%d %H:%M:%S")
        log_path  = os.path.join(BASE_DIR, "nepse_data", "scheduler.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}]  FATAL CRASH in report.py:\n")
            f.write(traceback.format_exc())
        raise