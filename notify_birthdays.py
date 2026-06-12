"""
매일 아침 Windows 작업 스케줄러로 실행되는 독립 스크립트.
birthdays.txt를 읽어
  1) 3일 뒤 생일인 사람이 있으면 → 나에게 DM (MY_SLACK_USER_ID)
  2) 오늘 생일인 사람이 있으면 → 생일 전용 채널 (BIRTHDAY_CHANNEL_ID)
을 보낸 뒤 종료한다. (Socket Mode 아님 — WebClient로 1회 전송)

해당자가 없으면 아무것도 보내지 않고 조용히 종료한다.

실행: python notify_birthdays.py
"""
import os
from datetime import timedelta

from slack_sdk import WebClient

from common import (
    now_kst,
    script_dir,
    SLACK_BOT_TOKEN,
    BIRTHDAY_CHANNEL_ID,
    MY_SLACK_USER_ID,
)

BIRTHDAYS_FILE = os.path.join(script_dir(), "birthdays.txt")


def load_birthdays():
    """birthdays.txt -> list[dict]  (name, month, day)"""
    items = []
    if not os.path.exists(BIRTHDAYS_FILE):
        return items

    with open(BIRTHDAYS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            md = parts[1].strip()  # "MM-DD"
            try:
                month_str, day_str = md.split("-")
                month = int(month_str)
                day = int(day_str)
            except Exception:
                continue
            if not name:
                continue
            items.append({"name": name, "month": month, "day": day})
    return items


def main():
    today = now_kst().date()
    target = today + timedelta(days=3)  # 3일 뒤

    todays = []
    soon = []
    for b in load_birthdays():
        if (b["month"], b["day"]) == (today.month, today.day):
            todays.append(b)
        if (b["month"], b["day"]) == (target.month, target.day):
            soon.append(b)

    client = WebClient(token=SLACK_BOT_TOKEN)

    # 1) 오늘 생일 -> 전용 채널
    if todays:
        if BIRTHDAY_CHANNEL_ID:
            names = ", ".join(b["name"] for b in todays)
            text = f"🎂 오늘은 {names}님의 생일입니다! 다 같이 축하해주세요 🎉"
            client.chat_postMessage(channel=BIRTHDAY_CHANNEL_ID, text=text)
            print(f"[INFO] sent today's birthdays ({names}) to {BIRTHDAY_CHANNEL_ID}")
        else:
            print("[WARN] BIRTHDAY_CHANNEL_ID가 비어 있어 오늘 생일 메시지를 건너뜁니다. (common.py 확인)")

    # 2) 3일 뒤 생일 -> 나에게 DM
    if soon:
        if MY_SLACK_USER_ID:
            md = f"{target.month:02d}-{target.day:02d}"
            names = ", ".join(b["name"] for b in soon)
            text = f"📅 3일 뒤({md}) {names}님 생일이에요. 미리 준비하세요!"
            client.chat_postMessage(channel=MY_SLACK_USER_ID, text=text)
            print(f"[INFO] sent 3-day-ahead DM ({names}) to {MY_SLACK_USER_ID}")
        else:
            print("[WARN] MY_SLACK_USER_ID가 비어 있어 3일 뒤 생일 DM을 건너뜁니다. (common.py 확인)")

    if not todays and not soon:
        print("[INFO] 오늘/3일 뒤 생일자가 없습니다. 전송 없이 종료합니다.")


if __name__ == "__main__":
    main()
