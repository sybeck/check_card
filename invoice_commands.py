"""
슬랙 모달로 현금영수증 / 전자세금계산서 발행·취소.

슬래시 커맨드(api.slack.com 에 수동 등록 필요):
  /현금영수증      → 현금영수증 발행 모달
  /현금영수증취소  → 현금영수증 발행취소 모달
  /세금계산서      → 전자세금계산서 발행 모달
  /세금계산서취소  → 전자세금계산서 발행취소 모달

발행/취소 API(팝빌)는 3초를 넘길 수 있으므로, 모달 제출은 검증 후 즉시 ack()로 닫고
실제 호출은 백그라운드 스레드에서 수행한 뒤 결과를 전용 채널로 보고한다.
"""
import threading

from common import dedup_seen, INVOICE_RESULT_CHANNEL_ID
from connectors.popbill.cash_receipt import issue_cash_receipt, cancel_cash_receipt
from connectors.popbill.tax_invoice import issue_tax_invoice, cancel_tax_invoice

# =========================
# callback_id / block_id
# =========================
CB_SUBMIT = "cashbill_submit"
CB_CANCEL = "cashbill_cancel"
TI_SUBMIT = "taxinvoice_submit"
TI_CANCEL = "taxinvoice_cancel"

# 현금영수증 발행 (합계금액만 입력받고 공급가액/세액은 자동계산)
B_CB_USAGE = "cb_usage"
B_CB_IDTYPE = "cb_idtype"
B_CB_IDNUM = "cb_idnum"
B_CB_TOTAL = "cb_total"
# 현금영수증 취소
B_CBX_MGTKEY = "cbx_mgtkey"
B_CBX_MEMO = "cbx_memo"

# 세금계산서 발행 (합계금액만 입력받고 공급가액/세액은 과세형태에 따라 자동계산)
B_TI_WRITEDATE = "ti_writedate"
B_TI_TAXTYPE = "ti_taxtype"
B_TI_PURPOSE = "ti_purpose"
B_TI_CORPNUM = "ti_corpnum"
B_TI_CORPNAME = "ti_corpname"
B_TI_CEO = "ti_ceo"
B_TI_EMAIL = "ti_email"
B_TI_ITEM = "ti_item"
B_TI_TOTAL = "ti_total"
# 세금계산서 취소
B_TIX_MGTKEY = "tix_mgtkey"
B_TIX_MEMO = "tix_memo"


# =========================
# 입력 파싱/검증 헬퍼
# =========================
def _txt(state, block) -> str:
    v = state.get(block, {}).get("value", {}).get("value")
    return (v or "").strip()


def _sel(state, block) -> str:
    opt = state.get(block, {}).get("value", {}).get("selected_option") or {}
    return (opt.get("value") or "").strip()


def _date(state, block) -> str:
    return (state.get(block, {}).get("value", {}).get("selected_date") or "").strip()


def _amount(s: str) -> int:
    """'1,100' / '1100' -> 1100. 숫자가 아니면 ValueError."""
    s = (s or "").replace(",", "").replace(" ", "").strip()
    if not s or not s.lstrip("-").isdigit():
        raise ValueError("amount")
    return int(s)


def _digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def _valid_brn(num: str) -> bool:
    """한국 사업자등록번호 10자리 체크섬 검증. 자릿수 + 검증식 통과 시 True."""
    if len(num) != 10 or not num.isdigit():
        return False
    weights = [1, 3, 7, 1, 3, 7, 1, 3, 5]
    s = sum(int(num[i]) * weights[i] for i in range(9))
    s += (int(num[8]) * 5) // 10
    check = (10 - (s % 10)) % 10
    return check == int(num[9])


def _run_async(target) -> None:
    threading.Thread(target=target, daemon=True).start()


def _notify(client, text: str, fallback_user: str = None) -> None:
    """결과를 전용 채널로 전송. 채널 미설정 시 요청자 DM으로 폴백."""
    channel = INVOICE_RESULT_CHANNEL_ID or fallback_user
    if not channel:
        print("[WARN] INVOICE_RESULT_CHANNEL_ID 미설정 — 결과 알림 생략")
        return
    try:
        client.chat_postMessage(channel=channel, text=text)
    except Exception as e:
        print(f"[WARN] 발행 결과 알림 전송 실패: {e}")


def _won(n) -> str:
    try:
        return f"{int(n):,}원"
    except Exception:
        return f"{n}원"


# =========================
# 모달 빌더
# =========================
def _input(block_id, label, action="value", placeholder=None, optional=False, multiline=False):
    el = {"type": "plain_text_input", "action_id": action}
    if placeholder:
        el["placeholder"] = {"type": "plain_text", "text": placeholder}
    if multiline:
        el["multiline"] = True
    return {
        "type": "input",
        "block_id": block_id,
        "optional": optional,
        "label": {"type": "plain_text", "text": label},
        "element": el,
    }


def _select(block_id, label, options, placeholder="선택"):
    return {
        "type": "input",
        "block_id": block_id,
        "label": {"type": "plain_text", "text": label},
        "element": {
            "type": "static_select",
            "action_id": "value",
            "placeholder": {"type": "plain_text", "text": placeholder},
            "options": [
                {"text": {"type": "plain_text", "text": o}, "value": o} for o in options
            ],
        },
    }


def build_cashbill_modal():
    return {
        "type": "modal",
        "callback_id": CB_SUBMIT,
        "title": {"type": "plain_text", "text": "현금영수증 발행"},
        "submit": {"type": "plain_text", "text": "발행"},
        "close": {"type": "plain_text", "text": "닫기"},
        "blocks": [
            _select(B_CB_USAGE, "거래구분", ["소득공제용", "지출증빙용"]),
            _select(B_CB_IDTYPE, "식별번호 종류", ["휴대폰번호", "사업자번호", "카드번호"]),
            _input(B_CB_IDNUM, "식별번호", placeholder="예: 01012345678 / 1234567890"),
            _input(B_CB_TOTAL, "합계금액(VAT 포함금액)", placeholder="예: 11000"),
        ],
    }


def build_taxinvoice_modal():
    return {
        "type": "modal",
        "callback_id": TI_SUBMIT,
        "title": {"type": "plain_text", "text": "전자세금계산서 발행"},
        "submit": {"type": "plain_text", "text": "발행"},
        "close": {"type": "plain_text", "text": "닫기"},
        "blocks": [
            {
                "type": "input",
                "block_id": B_TI_WRITEDATE,
                "label": {"type": "plain_text", "text": "작성일자(발행일자)"},
                "element": {"type": "datepicker", "action_id": "value",
                            "placeholder": {"type": "plain_text", "text": "날짜 선택"}},
            },
            _select(B_TI_TAXTYPE, "과세형태", ["과세", "영세", "면세"]),
            _select(B_TI_PURPOSE, "영수/청구", ["영수", "청구"]),
            _input(B_TI_CORPNUM, "공급받는자 사업자번호", placeholder="숫자 10자리"),
            _input(B_TI_CORPNAME, "공급받는자 상호"),
            _input(B_TI_CEO, "공급받는자 대표자"),
            _input(B_TI_EMAIL, "공급받는자 담당자 이메일", placeholder="발행 메일 수신"),
            _input(B_TI_ITEM, "품목명"),
            _input(B_TI_TOTAL, "합계금액(VAT 포함금액)", placeholder="예: 110000"),
        ],
    }


def build_cashbill_cancel_modal():
    return {
        "type": "modal",
        "callback_id": CB_CANCEL,
        "title": {"type": "plain_text", "text": "현금영수증 취소"},
        "submit": {"type": "plain_text", "text": "취소하기"},
        "close": {"type": "plain_text", "text": "닫기"},
        "blocks": [
            _input(B_CBX_MGTKEY, "문서번호(mgtKey)", placeholder="발행 결과 메시지의 문서번호"),
            _input(B_CBX_MEMO, "취소 사유", optional=True),
        ],
    }


def build_taxinvoice_cancel_modal():
    return {
        "type": "modal",
        "callback_id": TI_CANCEL,
        "title": {"type": "plain_text", "text": "세금계산서 취소"},
        "submit": {"type": "plain_text", "text": "취소하기"},
        "close": {"type": "plain_text", "text": "닫기"},
        "blocks": [
            _input(B_TIX_MGTKEY, "문서번호(mgtKey)", placeholder="발행 결과 메시지의 문서번호"),
            _input(B_TIX_MEMO, "취소 사유", optional=True),
        ],
    }


# =========================
# 핸들러 등록
# =========================
def register_invoice_handlers(app):
    # ----- 모달 열기 -----
    @app.command("/현금영수증")
    def open_cashbill(ack, body, client):
        ack()
        client.views_open(trigger_id=body["trigger_id"], view=build_cashbill_modal())

    @app.command("/현금영수증취소")
    def open_cashbill_cancel(ack, body, client):
        ack()
        client.views_open(trigger_id=body["trigger_id"], view=build_cashbill_cancel_modal())

    @app.command("/세금계산서")
    def open_taxinvoice(ack, body, client):
        ack()
        client.views_open(trigger_id=body["trigger_id"], view=build_taxinvoice_modal())

    @app.command("/세금계산서취소")
    def open_taxinvoice_cancel(ack, body, client):
        ack()
        client.views_open(trigger_id=body["trigger_id"], view=build_taxinvoice_cancel_modal())

    # ----- 현금영수증 발행 제출 -----
    @app.view(CB_SUBMIT)
    def submit_cashbill(ack, body, client):
        state = body["view"]["state"]["values"]
        user = body["user"]["id"]
        errors = {}

        # 합계금액(VAT 포함)만 입력받고 공급가액/세액은 자동 계산
        total = 0
        try:
            total = _amount(_txt(state, B_CB_TOTAL))
            if total <= 0:
                errors[B_CB_TOTAL] = "0보다 큰 금액을 입력하세요."
        except ValueError:
            errors[B_CB_TOTAL] = "숫자만 입력하세요."

        idnum = _digits(_txt(state, B_CB_IDNUM))
        if not idnum:
            errors[B_CB_IDNUM] = "식별번호(숫자)를 입력하세요."

        if errors:
            ack(response_action="errors", errors=errors)
            return

        ack()  # 모달 닫기

        if dedup_seen(f"cb:{user}:{idnum}:{total}", ttl_sec=60):
            return

        # 과세: 공급가액 = 합계/1.1(반올림), 세액 = 합계 - 공급가액
        supply = round(total / 1.1)
        tax = total - supply
        service = 0

        data = {
            "tradeUsage": _sel(state, B_CB_USAGE),
            "identityNum": idnum,
            "supplyCost": supply, "tax": tax, "serviceFee": service, "totalAmount": total,
        }

        def work():
            r = issue_cash_receipt(data)
            if r["ok"]:
                text = (
                    "✅ 현금영수증 발행 완료\n"
                    f"• 요청자: <@{user}>\n"
                    f"• 거래구분: {data['tradeUsage']} · 식별번호: {idnum}\n"
                    f"• 금액: 공급 {_won(supply)} / 세 {_won(tax)} / 합계 {_won(total)}\n"
                    f"• 국세청 승인번호: {r['confirmNum'] or '(조회 지연)'}\n"
                    f"• 문서번호(mgtKey): `{r['mgtKey']}`  ← 취소 시 사용"
                )
            else:
                text = (
                    "⚠️ 현금영수증 발행 실패\n"
                    f"• 요청자: <@{user}>\n"
                    f"• 사유: [{r['code']}] {r['message']}\n"
                    f"• 문서번호(mgtKey): `{r['mgtKey']}`"
                )
            _notify(client, text, fallback_user=user)

        _run_async(work)

    # ----- 현금영수증 취소 제출 -----
    @app.view(CB_CANCEL)
    def submit_cashbill_cancel(ack, body, client):
        state = body["view"]["state"]["values"]
        user = body["user"]["id"]
        mgt_key = _txt(state, B_CBX_MGTKEY)
        if not mgt_key:
            ack(response_action="errors", errors={B_CBX_MGTKEY: "문서번호를 입력하세요."})
            return
        ack()
        memo = _txt(state, B_CBX_MEMO)

        def work():
            r = cancel_cash_receipt(mgt_key, memo)
            if r["ok"]:
                text = f"🗑️ 현금영수증 발행취소 완료\n• 요청자: <@{user}>\n• 문서번호: `{mgt_key}`"
            else:
                text = (f"⚠️ 현금영수증 취소 실패\n• 요청자: <@{user}>\n"
                        f"• 문서번호: `{mgt_key}`\n• 사유: [{r['code']}] {r['message']}")
            _notify(client, text, fallback_user=user)

        _run_async(work)

    # ----- 세금계산서 발행 제출 -----
    @app.view(TI_SUBMIT)
    def submit_taxinvoice(ack, body, client):
        state = body["view"]["state"]["values"]
        user = body["user"]["id"]
        errors = {}

        write_date = _date(state, B_TI_WRITEDATE)
        if not write_date:
            errors[B_TI_WRITEDATE] = "작성일자를 선택하세요."

        corpnum = _digits(_txt(state, B_TI_CORPNUM))
        if len(corpnum) != 10:
            errors[B_TI_CORPNUM] = "사업자번호는 숫자 10자리여야 합니다."
        elif not _valid_brn(corpnum):
            errors[B_TI_CORPNUM] = "유효하지 않은 사업자번호입니다. 다시 확인해 주세요."

        email = _txt(state, B_TI_EMAIL)
        if "@" not in email:
            errors[B_TI_EMAIL] = "이메일을 입력하세요."

        # 합계금액(VAT 포함)만 입력받고 공급가액/세액은 과세형태에 따라 자동계산
        total = 0
        try:
            total = _amount(_txt(state, B_TI_TOTAL))
            if total <= 0:
                errors[B_TI_TOTAL] = "0보다 큰 금액을 입력하세요."
        except ValueError:
            errors[B_TI_TOTAL] = "숫자만 입력하세요."

        if errors:
            ack(response_action="errors", errors=errors)
            return

        ack()

        if dedup_seen(f"ti:{user}:{corpnum}:{total}:{write_date}", ttl_sec=60):
            return

        tax_type = _sel(state, B_TI_TAXTYPE)
        if tax_type == "과세":
            # 공급가액 = 합계/1.1(반올림), 세액 = 합계 - 공급가액
            supply = round(total / 1.1)
            tax = total - supply
        else:
            # 영세/면세: 세액 없음
            supply = total
            tax = 0

        data = {
            "writeDate": write_date.replace("-", ""),
            "taxType": tax_type,
            "purposeType": _sel(state, B_TI_PURPOSE),
            "invoiceeCorpNum": corpnum,
            "invoiceeCorpName": _txt(state, B_TI_CORPNAME),
            "invoiceeCEOName": _txt(state, B_TI_CEO),
            "invoiceeEmail1": email,
            "itemName": _txt(state, B_TI_ITEM),
            "supplyCostTotal": supply, "taxTotal": tax, "totalAmount": total,
        }

        def work():
            r = issue_tax_invoice(data)
            if r["ok"]:
                text = (
                    "✅ 전자세금계산서 발행 완료\n"
                    f"• 요청자: <@{user}>\n"
                    f"• 공급받는자: {data['invoiceeCorpName']} ({corpnum})\n"
                    f"• 금액: 공급 {_won(supply)} / 세 {_won(tax)} / 합계 {_won(total)}\n"
                    f"• 국세청 승인번호: {r['ntsConfirmNum'] or '(조회 지연)'}\n"
                    f"• 문서번호(mgtKey): `{r['mgtKey']}`  ← 취소 시 사용"
                )
            else:
                text = (
                    "⚠️ 전자세금계산서 발행 실패\n"
                    f"• 요청자: <@{user}>\n"
                    f"• 공급받는자: {data['invoiceeCorpName']} ({corpnum})\n"
                    f"• 사유: [{r['code']}] {r['message']}\n"
                    f"• 문서번호(mgtKey): `{r['mgtKey']}`"
                )
            _notify(client, text, fallback_user=user)

        _run_async(work)

    # ----- 세금계산서 취소 제출 -----
    @app.view(TI_CANCEL)
    def submit_taxinvoice_cancel(ack, body, client):
        state = body["view"]["state"]["values"]
        user = body["user"]["id"]
        mgt_key = _txt(state, B_TIX_MGTKEY)
        if not mgt_key:
            ack(response_action="errors", errors={B_TIX_MGTKEY: "문서번호를 입력하세요."})
            return
        ack()
        memo = _txt(state, B_TIX_MEMO)

        def work():
            r = cancel_tax_invoice(mgt_key, memo)
            if r["ok"]:
                text = f"🗑️ 전자세금계산서 발행취소 완료\n• 요청자: <@{user}>\n• 문서번호: `{mgt_key}`"
            else:
                text = (f"⚠️ 전자세금계산서 취소 실패\n• 요청자: <@{user}>\n"
                        f"• 문서번호: `{mgt_key}`\n• 사유: [{r['code']}] {r['message']}")
            _notify(client, text, fallback_user=user)

        _run_async(work)
