"""
Monmouth County Golf Weather — Prefect Flow
Runs daily at 5am ET. Fetches 16-day forecast, scores each day for golf,
sends SMS summary via Twilio, and optionally generates AI commentary via Anthropic.

Setup:
  1. pip install prefect twilio httpx anthropic
  2. Set environment variables (see below)
  3. prefect deploy --name golf-weather-sms
"""

import os
import json
import httpx
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from prefect import flow, task, get_run_logger
from twilio.rest import Client as TwilioClient

# ──────────────────────────────────────────────
#  CONFIG — set these as environment variables
#  or Prefect Variables / Blocks
# ──────────────────────────────────────────────
TWILIO_SID = os.environ["TWILIO_ACCOUNT_SID"]       # ACxxxxxxxx
TWILIO_TOKEN = os.environ["TWILIO_AUTH_TOKEN"]       # your auth token
TWILIO_FROM = os.environ["TWILIO_FROM_NUMBER"]       # +18776405061
RECIPIENTS = os.environ.get("GOLF_RECIPIENTS", "").split(",")  # comma-separated +1XXXXXXXXXX

# Optional: set ANTHROPIC_API_KEY to enable AI-generated commentary
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SITE_URL = os.environ.get("GOLF_SITE_URL", "https://kbronander.github.io/kbronander/")

# Monmouth County, NJ coordinates
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
    if high >= 65 and high <= 75 and rain_prob <= 15 and humidity <= 55 and code in (0, 1):
        return 5
    if high >= 60 and high <= 80 and rain_prob <= 25 and humidity <= 65 and code in (0, 1, 2):
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
    """Fetch 16-day forecast from Open-Meteo (free, no key)."""
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
def format_sms(highlights: dict) -> str:
    """Build a concise SMS message (< 320 chars target)."""
    lines = ["⛳ Golf Weather - Monmouth Co."]

    bw = highlights["best_weekend"]
    if bw:
        tag = "⚠️ Rough weekend" if bw["score"] == 1 else f"Score: {bw['score']}/5"
        lines.append(
            f"\n🏆 Best Weekend: {bw['date_str']}\n"
            f"{bw['high']}°/{bw['low']}° {bw['emoji']} "
            f"Rain: {bw['rain']}% | {tag}"
        )

    bd = highlights["best_weekday"]
    if bd:
        lines.append(
            f"\n📅 Best Weekday: {bd['date_str']}\n"
            f"{bd['high']}°/{bd['low']}° {bd['emoji']} "
            f"Rain: {bd['rain']}% | Score: {bd['score']}/5"
        )

    d16 = highlights["day_16"]
    if d16:
        lines.append(
            f"\n🔭 16-Day Out: {d16['date_str']}\n"
            f"{d16['high']}°/{d16['low']}° {d16['emoji']} "
            f"Rain: {d16['rain']}% | Score: {d16['score']}/5"
        )

    lines.append(f"\nFull forecast: {SITE_URL}")
    return "\n".join(lines)


@task(retries=1, retry_delay_seconds=10)
def maybe_add_ai_commentary(highlights: dict, base_msg: str) -> str:
    """Optionally call Anthropic to add a one-liner golf tip. Skip if no key."""
    if not ANTHROPIC_API_KEY:
        return base_msg

    logger = get_run_logger()
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        bw = highlights.get("best_weekend", {})
        bd = highlights.get("best_weekday", {})
        prompt = (
            f"You're a golf buddy sending a group text. Based on this weather, "
            f"write ONE short sentence (under 60 chars) with a fun recommendation. "
            f"Best weekend: {bw.get('date_str','N/A')} {bw.get('high','?')}° {bw.get('label','?')} score {bw.get('score','?')}/5. "
            f"Best weekday: {bd.get('date_str','N/A')} {bd.get('high','?')}° {bd.get('label','?')} score {bd.get('score','?')}/5."
        )
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        tip = resp.content[0].text.strip()
        logger.info(f"AI tip: {tip}")
        return base_msg + f"\n\n💬 {tip}"
    except Exception as e:
        logger.warning(f"AI commentary skipped: {e}")
        return base_msg


@task(retries=2, retry_delay_seconds=15)
def send_sms(message: str):
    """Send the SMS to all recipients via Twilio."""
    logger = get_run_logger()
    client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)

    for number in RECIPIENTS:
        number = number.strip()
        if not number:
            continue
        msg = client.messages.create(
            body=message,
            from_=TWILIO_FROM,
            to=number,
        )
        logger.info(f"SMS sent to {number} — SID: {msg.sid}")


# ──────────────────────────────────────────────
#  MAIN FLOW
# ──────────────────────────────────────────────
@flow(name="golf-weather-sms", log_prints=True)
def golf_weather_sms():
    """Daily golf weather report for Monmouth County, NJ."""
    raw = fetch_weather()
    days = process_forecast(raw)
    highlights = pick_highlights(days)
    message = format_sms(highlights)
    message = maybe_add_ai_commentary(highlights, message)

    print(f"\n{'='*40}")
    print(message)
    print(f"{'='*40}\n")

    send_sms(message)
    print(f"✅ Sent to {len([r for r in RECIPIENTS if r.strip()])} recipient(s)")


# ──────────────────────────────────────────────
#  DEPLOYMENT — run once to register with Prefect Cloud
# ──────────────────────────────────────────────
if __name__ == "__main__":
    # Option A: Just run the flow locally right now
    # golf_weather_sms()

    # Option B: Deploy to Prefect Cloud with a daily 5am ET schedule
    golf_weather_sms.serve(
        name="golf-weather-daily",
        cron="0 5 * * *",
        timezone="America/New_York",
        tags=["golf", "weather", "sms"],
    )
