"""
Monmouth County Golf Weather — Prefect Flow
Runs daily at 5am ET. Fetches 16-day forecast, scores each day for golf,
sends SMS via email-to-SMS gateway (no Twilio needed).

Setup:
  1. pip install prefect httpx
  2. Set environment variables (see below)
  3. python golf_weather_flow.py
"""

import os
import smtplib
from email.mime.text import MIMEText
import httpx
from datetime import date
from prefect import flow, task, get_run_logger
from prefect.variables import Variable

# ──────────────────────────────────────────────
#  CONFIG — set these as environment variables
# ──────────────────────────────────────────────

GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "kbronander@gmail.com")

# Load from Prefect Variable instead of raw env var
GMAIL_APP_PASSWORD = "nwplgoucamcmdkrm"

# Recipients: comma-separated email-to-SMS addresses
# Carrier gateways:
#   Verizon:  number@vtext.com
#   AT&T:     number@txt.att.net
#   T-Mobile: number@tmomail.net
#   Sprint:   number@messaging.sprintpcs.com
RECIPIENTS = os.environ.get(
    "GOLF_RECIPIENTS",
    "9085778614@vtext.com"
).split(",")

SITE_URL = os.environ.get("GOLF_SITE_URL", "https://kbronander.github.io/kbronander/")

# Monmouth County, NJ
LAT, LON = 40.25, -74.15

# WMO weather code to emoji + label
WMO = {
    0: ("☀️", "Clear"),        1: ("🌤️", "Mostly clear"),
    2: ("⛅", "Partly cloudy"), 3: ("☁️", "Overcast"),
    45: ("🌫️", "Fog"),         48: ("🌫️", "Rime fog"),
    51: ("🌦️", "Lt drizzle"),  53: ("🌦️", "Drizzle"),
    55: ("🌧️", "Hvy drizzle"), 61: ("🌧️", "Lt rain"),
    63: ("🌧️", "Rain"),        65: ("🌧️", "Heavy rain"),
    66: ("🌨️", "Frzg rain"),   67: ("🌨️", "Hvy frzg rain"),
    71: ("🌨️", "Lt snow"),     73: ("❄️", "Snow"),
    75: ("❄️", "Heavy snow"),   77: ("❄️", "Snow grains"),
    80: ("🌦️", "Lt showers"),  81: ("🌧️", "Showers"),
    82: ("🌧️", "Hvy showers"), 85: ("🌨️", "Snow showers"),
    86: ("❄️", "Hvy snow shrs"),95: ("⛈️", "T-storm"),
    96: ("⛈️", "T-storm/hail"),99: ("⛈️", "Severe t-storm"),
}

SEVERE_CODES = {63, 65, 66, 67, 71, 73, 75, 77, 81, 82, 85, 86, 95, 96, 99}


# ──────────────────────────────────────────────
#  GOLF SCORING
# ──────────────────────────────────────────────
def golf_score(high, low, rain_prob, humidity, code):
    """Score a day for golf: 1 (skip) to 5 (perfect)."""
    if high < 40 or rain_prob > 50 or code in SEVERE_CODES:
        return 1
    if high < 50 or high > 85:
        return 2
    if 65 <= high <= 75 and rain_prob <= 15 and humidity <= 55 and code in (0, 1):
        return 5
    if 60 <= high <= 80 and rain_prob <= 25 and humidity <= 65 and code in (0, 1, 2):
        return 4
    if rain_prob <= 40 and 50 <= high <= 85:
        return 3
    return 2


SCORE_LABELS = {1: "Skip it", 2: "Tough", 3: "Playable", 4: "Great", 5: "Perfect"}


# ──────────────────────────────────────────────
#  TASKS
# ──────────────────────────────────────────────
@task(retries=2, retry_delay_seconds=30)
def fetch_weather() -> dict:
    """Fetch 16-day forecast from Open-Meteo (free, no API key)."""
    logger = get_run_logger()
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={LAT}&longitude={LON}"
        f"&daily=temperature_2m_max,temperature_2m_min,"
        f"precipitation_probability_max,weathercode,relative_humidity_2m_max"
        f"&temperature_unit=fahrenheit&timezone=America/New_York&forecast_days=16"
    )
    resp = httpx.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    logger.info(f"Fetched {len(data['daily']['time'])} days of forecast")
    return data


@task
def process_forecast(data: dict) -> list[dict]:
    """Turn raw API data into scored day objects."""
    d = data["daily"]
    days = []
    for i in range(len(d["time"])):
        dt = date.fromisoformat(d["time"][i])
        high = d["temperature_2m_max"][i]
        low = d["temperature_2m_min"][i]
        rain = d["precipitation_probability_max"][i]
        hum = d["relative_humidity_2m_max"][i]
        code = d["weathercode"][i]
        emoji, label = WMO.get(code, ("❓", "Unknown"))
        score = golf_score(high, low, rain, hum, code)

        days.append({
            "date": dt,
            "date_str": dt.strftime("%a %b %-d"),
            "dow": dt.weekday(),          # 0=Mon … 6=Sun
            "high": round(high),
            "low": round(low),
            "rain": rain,
            "humidity": hum,
            "code": code,
            "emoji": emoji,
            "label": label,
            "score": score,
            "score_label": SCORE_LABELS[score],
        })
    return days


@task
def pick_highlights(days: list[dict]) -> dict:
    """Select best weekend day, best weekday, and day 16."""
    weekends = [d for d in days if d["dow"] in (5, 6)]    # Sat, Sun
    weekdays = [d for d in days if d["dow"] in (0, 1, 2, 3, 4)]

    def best(pool):
        return max(pool, key=lambda d: (d["score"], -d["rain"]))

    return {
        "best_weekend": best(weekends) if weekends else None,
        "best_weekday": best(weekdays) if weekdays else None,
        "day_16": days[-1] if days else None,
    }


@task
def format_message(highlights: dict) -> str:
    """Build a concise SMS-friendly message."""
    lines = ["Golf Weather - Monmouth Co."]

    bw = highlights["best_weekend"]
    if bw:
        tag = "Rough weekend" if bw["score"] == 1 else f"Score: {bw['score']}/5"
        lines.append(
            f"\nBest Weekend: {bw['date_str']}\n"
            f"{bw['high']}F/{bw['low']}F {bw['emoji']} "
            f"Rain: {bw['rain']}% | {tag}"
        )

    bd = highlights["best_weekday"]
    if bd:
        lines.append(
            f"\nBest Weekday: {bd['date_str']}\n"
            f"{bd['high']}F/{bd['low']}F {bd['emoji']} "
            f"Rain: {bd['rain']}% | Score: {bd['score']}/5"
        )

    d16 = highlights["day_16"]
    if d16:
        lines.append(
            f"\n16-Day Out: {d16['date_str']}\n"
            f"{d16['high']}F/{d16['low']}F {d16['emoji']} "
            f"Rain: {d16['rain']}% | Score: {d16['score']}/5"
        )

    lines.append(f"\nFull forecast: {SITE_URL}")
    return "\n".join(lines)


@task(retries=2, retry_delay_seconds=15)
def send_sms_via_email(message: str):
    """Send SMS to all recipients via email-to-SMS carrier gateways."""
    logger = get_run_logger()

    for recipient in RECIPIENTS:
        recipient = recipient.strip()
        if not recipient:
            continue

        msg = MIMEText(message)
        msg["From"] = GMAIL_ADDRESS
        msg["To"] = recipient
        msg["Subject"] = ""  # keep empty — subject shows as separate line on some carriers

        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
                server.send_message(msg)
            logger.info(f"SMS sent to {recipient}")
        except Exception as e:
            logger.error(f"Failed to send to {recipient}: {e}")
            raise


# ──────────────────────────────────────────────
#  MAIN FLOW
# ──────────────────────────────────────────────
@flow(name="golf-weather-sms", log_prints=True)
def golf_weather_sms():
    """Daily golf weather report for Monmouth County, NJ."""
    raw = fetch_weather()
    days = process_forecast(raw)
    highlights = pick_highlights(days)
    message = format_message(highlights)

    print(f"\n{'='*40}")
    print(message)
    print(f"{'='*40}\n")

    send_sms_via_email(message)
    print(f"Sent to {len([r for r in RECIPIENTS if r.strip()])} recipient(s)")


# ──────────────────────────────────────────────
#  DEPLOYMENT
# ──────────────────────────────────────────────
if __name__ == "__main__":
    # To test once: uncomment the next line, comment out .serve()
    # golf_weather_sms()

    # To deploy with daily 5am ET schedule:
    golf_weather_sms.serve(
        name="golf-weather-daily",
        cron="0 5 * * *",
        timezone="America/New_York",
        tags=["golf", "weather", "sms"],
    )
