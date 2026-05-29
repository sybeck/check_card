import os
from datetime import datetime, timedelta

from common import now_kst, script_dir, KST, RESERVE_CHANNEL_ID

# =========================
# Config / Storage
# =========================
RESV_FILE = os.path.join(script_dir(), "reservations.txt")

# 미팅 소요시간 옵션 (라벨, 분)
DURATION_OPTIONS = [
    ("30분", 30),
    ("1시간", 60),
    ("1시간 30분", 90),
    ("2시간", 120),
    ("3시간", 180),
    ("4시간", 240),
]

# 시작 시간 옵션 (08:00 ~ 23:30, 30분 단위)
START_TIME_OPTIONS = [
    f"{h:02d}:{m:02d}" for h in range(8, 24) for m in (0, 30)
]

CALLBACK_ID = "reservation_submit"
CANCEL_CALLBACK_ID = "reservation_cancel"

# 모달 block_id
B_NAME = "b_name"
B_RESERVER = "b_reserver"
B_DATE = "b_date"
B_START = "b_start"
B_DURATION = "b_duration"
B_CANCEL_SELECT = "b_cancel_select"


# =========================
# Storage helpers (txt = "메모장")
# =========================
def load_reservations():
    """reservations.txt -> list[dict]"""
    items = []
    if not os.path.exists(RESV_FILE):
        return items

    with open(RESV_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 8:
                continue
            try:
                duration_min = int(parts[4])
            except Exception:
                duration_min = 0
            items.append({
                "id": parts[0],
                "date": parts[1],
                "start": parts[2],
                "end": parts[3],
                "duration_min": duration_min,
                "meeting_name": parts[5],
                "reserver": parts[6],
                "created_at": parts[7],
            })
    return items


def resv_start_end(resv):
    """예약 dict -> (start_dt, end_dt) (KST). duration_min 기준으로 end 계산."""
    start_dt = datetime.strptime(f"{resv['date']} {resv['start']}", "%Y-%m-%d %H:%M").replace(tzinfo=KST)
    end_dt = start_dt + timedelta(minutes=resv["duration_min"])
    return start_dt, end_dt


def next_id() -> str:
    """YYYYMMDD-순번 (당일 카운트 기준)."""
    today = now_kst().strftime("%Y%m%d")
    count = 0
    for r in load_reservations():
        if r["id"].startswith(today + "-"):
            count += 1
    return f"{today}-{count + 1}"


def _row_line(resv) -> str:
    return (
        f"{resv['id']}\t{resv['date']}\t{resv['start']}\t{resv['end']}\t"
        f"{resv['duration_min']}\t{resv['meeting_name']}\t{resv['reserver']}\t{resv['created_at']}\n"
    )


def _write_header(f):
    f.write("# Meeting room reservations\n")
    f.write("# id\tdate\tstart\tend\tduration_min\tmeeting_name\treserver\tcreated_at\n")


def append_reservation(resv):
    is_new = not os.path.exists(RESV_FILE)
    with open(RESV_FILE, "a", encoding="utf-8") as f:
        if is_new:
            _write_header(f)
        f.write(_row_line(resv))


def remove_reservation(resv_id) -> dict:
    """resv_id에 해당하는 예약을 파일에서 제거. 제거한 예약 dict 리턴, 없으면 None."""
    items = load_reservations()
    removed = None
    keep = []
    for r in items:
        if r["id"] == resv_id and removed is None:
            removed = r
        else:
            keep.append(r)

    if removed is None:
        return None

    with open(RESV_FILE, "w", encoding="utf-8") as f:
        _write_header(f)
        for r in keep:
            f.write(_row_line(r))
    return removed


def find_conflict(new_start, new_end):
    """기존 예약과 시간이 겹치면 해당 예약 dict를 리턴, 없으면 None. (회의실 1개)"""
    for r in load_reservations():
        ex_start, ex_end = resv_start_end(r)
        # 같은 날 비교만 해도 충분하지만, datetime 비교로 일반화
        if new_start < ex_end and ex_start < new_end:
            return r
    return None


# =========================
# Modal
# =========================
def build_reservation_modal():
    return {
        "type": "modal",
        "callback_id": CALLBACK_ID,
        "title": {"type": "plain_text", "text": "회의실 예약"},
        "submit": {"type": "plain_text", "text": "예약"},
        "close": {"type": "plain_text", "text": "취소"},
        "blocks": [
            {
                "type": "input",
                "block_id": B_NAME,
                "label": {"type": "plain_text", "text": "미팅명"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "value",
                    "placeholder": {"type": "plain_text", "text": "예: 주간 기획 회의"},
                },
            },
            {
                "type": "input",
                "block_id": B_RESERVER,
                "label": {"type": "plain_text", "text": "예약자"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "value",
                    "placeholder": {"type": "plain_text", "text": "예: 홍길동"},
                },
            },
            {
                "type": "input",
                "block_id": B_DATE,
                "label": {"type": "plain_text", "text": "미팅 일자"},
                "element": {
                    "type": "datepicker",
                    "action_id": "value",
                    "placeholder": {"type": "plain_text", "text": "날짜 선택"},
                },
            },
            {
                "type": "input",
                "block_id": B_START,
                "label": {"type": "plain_text", "text": "시작 시간"},
                "element": {
                    "type": "static_select",
                    "action_id": "value",
                    "placeholder": {"type": "plain_text", "text": "시간 선택 (30분 단위)"},
                    "options": [
                        {
                            "text": {"type": "plain_text", "text": t},
                            "value": t,
                        }
                        for t in START_TIME_OPTIONS
                    ],
                },
            },
            {
                "type": "input",
                "block_id": B_DURATION,
                "label": {"type": "plain_text", "text": "미팅 시간 (소요)"},
                "element": {
                    "type": "static_select",
                    "action_id": "value",
                    "placeholder": {"type": "plain_text", "text": "소요 시간 선택"},
                    "options": [
                        {
                            "text": {"type": "plain_text", "text": label},
                            "value": str(minutes),
                        }
                        for label, minutes in DURATION_OPTIONS
                    ],
                },
            },
        ],
    }


# =========================
# Formatting
# =========================
def format_date_display(date_str: str) -> str:
    """'YYYY-MM-DD' -> 'MM월 DD일'."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%m월 %d일")


def format_reservation_line(r) -> str:
    return f"- {format_date_display(r['date'])} {r['start']}~{r['end']} {r['meeting_name']} (예약자 {r['reserver']})"


def upcoming_reservations():
    """아직 끝나지 않은 예약을 시작시간 순으로 리턴."""
    now = now_kst()
    items = [r for r in load_reservations() if resv_start_end(r)[1] >= now]
    items.sort(key=lambda r: resv_start_end(r)[0])
    return items


def build_cancel_modal(upcoming):
    """취소할 예약을 고르는 드롭다운 모달. upcoming: 남은 예약 list."""
    options = []
    for r in upcoming:
        label = f"{format_date_display(r['date'])} {r['start']}~{r['end']} {r['meeting_name']} (예약자 {r['reserver']})"
        options.append({
            "text": {"type": "plain_text", "text": label[:75]},  # Slack 옵션 텍스트 75자 제한
            "value": r["id"],
        })

    return {
        "type": "modal",
        "callback_id": CANCEL_CALLBACK_ID,
        "title": {"type": "plain_text", "text": "회의실 예약 취소"},
        "submit": {"type": "plain_text", "text": "취소하기"},
        "close": {"type": "plain_text", "text": "닫기"},
        "blocks": [
            {
                "type": "input",
                "block_id": B_CANCEL_SELECT,
                "label": {"type": "plain_text", "text": "취소할 예약"},
                "element": {
                    "type": "static_select",
                    "action_id": "value",
                    "placeholder": {"type": "plain_text", "text": "예약 선택"},
                    "options": options,
                },
            },
        ],
    }


# =========================
# Slack handler registration
# =========================
def register_reservation_handlers(app):
    @app.command("/예약")
    def open_reservation_modal(ack, body, client):
        ack()
        client.views_open(
            trigger_id=body["trigger_id"],
            view=build_reservation_modal(),
        )

    @app.view(CALLBACK_ID)
    def handle_reservation_submit(ack, body, client, logger):
        state = body["view"]["state"]["values"]
        meeting_name = state[B_NAME]["value"]["value"].strip()
        reserver = state[B_RESERVER]["value"]["value"].strip()
        date_str = state[B_DATE]["value"]["selected_date"]
        start_str = state[B_START]["value"]["selected_option"]["value"]
        duration_min = int(state[B_DURATION]["value"]["selected_option"]["value"])

        start_dt = datetime.strptime(f"{date_str} {start_str}", "%Y-%m-%d %H:%M").replace(tzinfo=KST)
        end_dt = start_dt + timedelta(minutes=duration_min)
        end_str = end_dt.strftime("%H:%M")

        # 1) 과거 시간 거부
        if start_dt < now_kst():
            ack(
                response_action="errors",
                errors={B_DATE: "이미 지난 시간으로는 예약할 수 없습니다."},
            )
            return

        # 2) 충돌 검사 (회의실 1개)
        conflict = find_conflict(start_dt, end_dt)
        if conflict:
            ack(
                response_action="errors",
                errors={
                    B_START: f"기존 예약과 겹칩니다: {conflict['meeting_name']} "
                             f"({format_date_display(conflict['date'])} {conflict['start']}~{conflict['end']})"
                },
            )
            return

        # 3) 저장
        ack()
        resv = {
            "id": next_id(),
            "date": date_str,
            "start": start_str,
            "end": end_str,
            "duration_min": duration_min,
            "meeting_name": meeting_name,
            "reserver": reserver,
            "created_at": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            append_reservation(resv)
        except Exception:
            logger.exception("Failed to save reservation")
            return

        # 4) 완료 메시지
        try:
            client.chat_postMessage(
                channel=RESERVE_CHANNEL_ID,
                text=(
                    f"📅 새 예약: {meeting_name} | "
                    f"{format_date_display(date_str)} {start_str}~{end_str} | 예약자 {reserver}"
                ),
            )
        except Exception:
            logger.exception("Failed to post reservation confirmation")

    @app.command("/예약목록")
    def list_reservations(ack, respond):
        ack()
        upcoming = upcoming_reservations()

        if not upcoming:
            respond(response_type="in_channel", text="남아 있는 예약이 없습니다.")
            return

        lines = ["🗓️ 회의실 예약 현황입니다\n"]
        lines += [format_reservation_line(r) for r in upcoming]
        respond(response_type="in_channel", text="\n".join(lines))

    @app.command("/예약취소")
    def open_cancel_modal(ack, body, client, respond):
        ack()
        upcoming = upcoming_reservations()
        if not upcoming:
            respond(response_type="ephemeral", text="취소할 예약이 없습니다.")
            return
        client.views_open(
            trigger_id=body["trigger_id"],
            view=build_cancel_modal(upcoming),
        )

    @app.view(CANCEL_CALLBACK_ID)
    def handle_cancel_submit(ack, body, client, logger):
        ack()
        state = body["view"]["state"]["values"]
        resv_id = state[B_CANCEL_SELECT]["value"]["selected_option"]["value"]

        try:
            removed = remove_reservation(resv_id)
        except Exception:
            logger.exception("Failed to cancel reservation")
            return

        if removed is None:
            # 이미 취소되었거나 사라진 예약
            try:
                client.chat_postMessage(
                    channel=RESERVE_CHANNEL_ID,
                    text="⚠️ 선택한 예약을 찾을 수 없습니다 (이미 취소되었을 수 있어요).",
                )
            except Exception:
                logger.exception("Failed to post cancel-notfound message")
            return

        # 취소 완료 메시지
        try:
            client.chat_postMessage(
                channel=RESERVE_CHANNEL_ID,
                text=(
                    f"🗑️ 예약 취소: {removed['meeting_name']} | "
                    f"{format_date_display(removed['date'])} {removed['start']}~{removed['end']} | 예약자 {removed['reserver']}"
                ),
            )
        except Exception:
            logger.exception("Failed to post cancel confirmation")
