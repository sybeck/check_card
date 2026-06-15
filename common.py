import os
import json
import time
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

# =========================
# Load .env
# =========================
load_dotenv()

# =========================
# Config
# =========================
KST = timezone(timedelta(hours=9))

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "").strip()
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "").strip()

if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
    raise RuntimeError("환경변수 SLACK_BOT_TOKEN(xoxb-...), SLACK_APP_TOKEN(xapp-...)가 필요합니다. (.env 확인)")

# 새 예약 채널 ID — 실제 채널 ID로 교체하세요. (.env 아님, 코드에 직접 기입)
RESERVE_CHANNEL_ID = "C0B6V5SDHQV"

# 생일 알림 — 실제 값으로 교체하세요. (.env 아님, 코드에 직접 기입)
BIRTHDAY_CHANNEL_ID = "C09DUU8SHDW"      # 오늘 생일 축하 메시지를 보낼 채널 ID (예: C0XXXXXXX)
MY_SLACK_USER_ID = "U046NV3LGH0"         # 3일 뒤 생일 DM을 받을 내 Slack member ID (예: U0XXXXXXX)

# 메타 광고소재 시트 업데이트 결과 알림 채널 ID
META_CREATIVE_CHANNEL_ID = "C0AJ2M6KDFE"

# 주간 Meta 광고 성과 개인별 집계(자동) 결과 알림 채널 ID
PERF_WEEKLY_CHANNEL_ID = "C0A24HBHNFN"

# 주간 성과 완료 메시지에서 멘션할 강다은 팀장님 Slack member ID
KANG_DAEUN_USER_ID = "U05TADGG8Q6"


# =========================
# Time helpers
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def month_key(dt: datetime) -> str:
    return dt.strftime("%Y%m")


def script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


# =========================
# Slack: dedupe (중복 이벤트 방지)
# =========================
DEDUP_FILE = os.path.join(script_dir(), ".slack_dedup.json")


def load_dedup():
    if not os.path.exists(DEDUP_FILE):
        return {}
    try:
        with open(DEDUP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_dedup(d):
    try:
        with open(DEDUP_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def dedup_seen(key: str, ttl_sec: int = 3600) -> bool:
    d = load_dedup()
    now = int(time.time())

    for k, ts in list(d.items()):
        if now - int(ts) > ttl_sec:
            d.pop(k, None)

    if key in d:
        return True

    d[key] = now
    save_dedup(d)
    return False
