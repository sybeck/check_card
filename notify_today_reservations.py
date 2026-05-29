"""
매일 아침 Windows 작업 스케줄러로 실행되는 독립 스크립트.
오늘의 회의실 예약을 RESERVE_CHANNEL_ID 채널로 전송한 뒤 종료한다.
(Socket Mode 아님 — WebClient로 1회 전송)

실행: python notify_today_reservations.py
"""
from slack_sdk import WebClient

from common import now_kst, SLACK_BOT_TOKEN, RESERVE_CHANNEL_ID
from reservations import load_reservations, resv_start_end, format_reservation_line


def build_today_message() -> str:
    today = now_kst().date()
    todays = []
    for r in load_reservations():
        start_dt, _end_dt = resv_start_end(r)
        if start_dt.date() == today:
            todays.append((start_dt, r))

    if not todays:
        return "오늘 예약된 회의실이 없습니다."

    todays.sort(key=lambda x: x[0])
    lines = ["오늘의 회의실 예약"]
    lines += [format_reservation_line(r) for _, r in todays]
    return "\n".join(lines)


def main():
    client = WebClient(token=SLACK_BOT_TOKEN)
    text = build_today_message()
    client.chat_postMessage(channel=RESERVE_CHANNEL_ID, text=text)
    print(f"[INFO] sent today's reservations to {RESERVE_CHANNEL_ID}")


if __name__ == "__main__":
    main()
