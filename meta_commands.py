"""
메타 광고 대시보드 / 일일예산 Slack 명령 모듈.
slack_card_bot.py 에서 register_meta_handlers(app) 로 등록되어
기존 카드/예약 봇과 같은 Socket Mode 앱 안에서 함께 동작한다.

명령:
  /부제meta  /브올meta   → 대시보드(광고비/구매/ROAS 등)
  /부제예산  /브올예산   → 일일예산 합계
  멘션(스레드에서도 사용 가능): "@봇 부제meta" / "브올예산" 등
"""
from datetime import datetime, timedelta, timezone

import meta_dashboard
from common import dedup_seen

KST = timezone(timedelta(hours=9))

# 채널 공유를 원하면 "in_channel" 로 변경 (기본: 요청자에게만 보이는 ephemeral)
RESPONSE_TYPE = "ephemeral"


def _now_kst_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M")


def _fmt_metrics_lines(m: dict) -> list:
    return [
        f"• 광고비: {m['spend']:,.0f}원",
        f"• 구매수: {m['purchases']:,} / 결제당 비용(CPA): {m['cpa']:,.0f}원 / ROAS {m['roas']:,.2f}",
        f"• 결제시작수: {m['checkouts']:,} / 결제시작당 비용: {m['cost_per_checkout']:,.0f}원",
        f"• 클릭수: {m['clicks']:,} / CPC: {m['cpc']:,.0f}원",
    ]


def format_burdenzero(data: dict) -> str:
    lines = [f"*👀 부담제로 메타 대시보드 현황* ({_now_kst_str()} KST)"]
    lines += _fmt_metrics_lines(data["metrics"])
    return "\n".join(lines)


def format_brainology(data: dict) -> str:
    lines = [f"*👀 브레인올로지 메타 대시보드 현황* ({_now_kst_str()} KST)"]
    for product, m in data["products"].items():
        lines.append(f"\n*✅ {product}*")
        lines += _fmt_metrics_lines(m)
    lines.append("\n*합계*")
    lines += _fmt_metrics_lines(data["total"])
    return "\n".join(lines)


def _budget_unit(data: dict) -> str:
    cur = (data.get("currency") or "KRW").upper()
    return "원" if cur == "KRW" else f" {cur}"


def _budget_block(m: dict, unit: str) -> list:
    def money(v):
        return f"{v:,.0f}{unit}"

    return [
        f"• 일일 총예산: {money(m['total'])}",
        f"  - 캠페인 예산: {money(m['campaign_budget'])} ({m['campaign_count']}개)",
        f"  - 광고세트 예산: {money(m['adset_budget'])} ({m['adset_count']}개)",
    ]


def format_burdenzero_budget(data: dict) -> str:
    unit = _budget_unit(data)
    return "\n".join([f"*💰 부담제로 메타 일일예산* ({_now_kst_str()} KST)"] + _budget_block(data, unit))


def format_brainology_budget(data: dict) -> str:
    unit = _budget_unit(data)
    lines = [f"*💰 브레인올로지 메타 일일예산* ({_now_kst_str()} KST)"]
    for product, m in data["products"].items():
        lines.append(f"\n*✅ {product}*")
        lines += _budget_block(m, unit)
    lines.append("\n*합계*")
    lines += _budget_block(data["total"], unit)
    return "\n".join(lines)


def _fmt_sales_lines(m: dict) -> list:
    return [
        f"• 현재 매출 {m['revenue']:,} / {m['purchases']:,}",
        f"• 메타 광고비: {m['spend']:,.0f}",
        f"• ROAS {m['roas']:,.2f} / CPA {m['cpa']:,.0f}",
    ]


def format_brainology_sales(data: dict) -> str:
    lines = [f"*👀 브레인올로지 현재 매출 현황* ({_now_kst_str()} KST)"]
    if data.get("coupang_failed"):
        lines.append("⚠️ 쿠팡 조회는 에러가 발생해서 집계에서 제외되었습니다.")
    for product, m in data["products"].items():
        lines.append(f"\n*✅ {product}*")
        lines += _fmt_sales_lines(m)
    lines.append("\n*합계*")
    lines += _fmt_sales_lines(data["total"])
    lines.append("\n_(실 결제금액 기준)_")
    return "\n".join(lines)


def handle_sales_mention(channel: str, thread_ts: str, client):
    """멘션 '브올매출' → 브레인올로지 제품별 매출/광고비/ROAS/CPA 집계 후 스레드 회신.

    카페24/쿠팡은 Playwright 로 브라우저를 띄워 수십 초~수 분 걸리므로 먼저 진행 메시지를
    보낸 뒤 집계한다. 무거운 의존성은 지연 import 한다.
    """
    client.chat_postMessage(
        channel=channel, thread_ts=thread_ts, text="⏳ 브레인올로지 현재 매출 집계 중… (카페24/쿠팡/네이버/메타, 수 분 걸릴 수 있어요)"
    )
    try:
        import brainology_sales

        data = brainology_sales.get_brainology_sales()
        client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=format_brainology_sales(data))
    except Exception as e:
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f"브레인올로지 매출 집계 실패 ❌\n{type(e).__name__}: {e}",
        )


def handle_performance_mention(text: str, channel: str, thread_ts: str, client):
    """멘션 '성과 0601-0612' → 기간 성과를 키워드별로 집계해 노션에 기록하고 스레드 회신.

    무거운 의존성(performance_report/notion_writer)은 지연 import 하여
    노션 미설정 시에도 봇 나머지 기능은 정상 동작하도록 한다.
    """
    import performance_report as perf

    try:
        since, until = perf.parse_period(text)
    except Exception as e:
        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts, text=f"성과 기간 인식 실패 ❌\n{e}"
        )
        return

    client.chat_postMessage(
        channel=channel, thread_ts=thread_ts, text=f"⏳ Meta 광고 성과 개인별 집계 중… ({since} ~ {until})"
    )

    try:
        report = perf.build_report(since, until)
        import notion_writer

        url = notion_writer.create_performance_page(report)
        t = report["total"]
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=(
                f"✅ Meta 광고 성과 개인별 집계 완료 ({since} ~ {until})\n"
                f"총 구매전환값 {t['revenue']:,.0f}원 · 총 지출 {t['spend']:,.0f}원 · ROAS {t['roas']:.2f}\n"
                f"🔗 {url}"
            ),
        )
    except Exception as e:
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f"Meta 광고 성과 개인별 집계 실패 ❌\n{type(e).__name__}: {e}",
        )


# 멘션 라우팅 (스레드 안에서는 슬래시 명령이 막히므로 멘션도 지원)
def _route_mention_text(text: str):
    t = text or ""
    if "부제예산" in t:
        return (format_burdenzero_budget, meta_dashboard.get_burdenzero_budget)
    if "브올예산" in t:
        return (format_brainology_budget, meta_dashboard.get_brainology_budget)
    if "부제meta" in t:
        return (format_burdenzero, meta_dashboard.get_burdenzero)
    if "브올meta" in t:
        return (format_brainology, meta_dashboard.get_brainology)
    return None


def register_meta_handlers(app):
    """slack_card_bot.py 에서 호출되어 메타 관련 핸들러를 app 에 등록한다."""

    @app.command("/부제meta")
    def cmd_burdenzero(ack, respond):
        ack()
        try:
            respond(response_type=RESPONSE_TYPE, text=format_burdenzero(meta_dashboard.get_burdenzero()))
        except Exception as e:
            respond(response_type="ephemeral", text=f"조회 실패 ❌\n{type(e).__name__}: {e}")

    @app.command("/브올meta")
    def cmd_brainology(ack, respond):
        ack()
        try:
            respond(response_type=RESPONSE_TYPE, text=format_brainology(meta_dashboard.get_brainology()))
        except Exception as e:
            respond(response_type="ephemeral", text=f"조회 실패 ❌\n{type(e).__name__}: {e}")

    @app.command("/부제예산")
    def cmd_burdenzero_budget(ack, respond):
        ack()
        try:
            respond(response_type=RESPONSE_TYPE, text=format_burdenzero_budget(meta_dashboard.get_burdenzero_budget()))
        except Exception as e:
            respond(response_type="ephemeral", text=f"조회 실패 ❌\n{type(e).__name__}: {e}")

    @app.command("/브올예산")
    def cmd_brainology_budget(ack, respond):
        ack()
        try:
            respond(response_type=RESPONSE_TYPE, text=format_brainology_budget(meta_dashboard.get_brainology_budget()))
        except Exception as e:
            respond(response_type="ephemeral", text=f"조회 실패 ❌\n{type(e).__name__}: {e}")

    @app.event("app_mention")
    def on_app_mention(event, client):
        channel = event.get("channel")
        thread_ts = event.get("thread_ts") or event.get("ts")  # 스레드 안이면 같은 스레드에 답글
        text = event.get("text", "")

        # Meta 광고 성과 개인별 집계: "@봇 성과 0601-0612"
        if "성과" in text:
            if dedup_seen(f"perf:{event.get('ts')}"):  # Slack 이벤트 재시도 중복 방지
                return
            handle_performance_mention(text, channel, thread_ts, client)
            return

        # 브레인올로지 현재 매출 집계: "@봇 브올매출"
        if "브올매출" in text:
            if dedup_seen(f"sales:{event.get('ts')}"):  # Slack 이벤트 재시도 중복 방지
                return
            handle_sales_mention(channel, thread_ts, client)
            return

        route = _route_mention_text(text)
        if not route:
            client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text="사용법: 멘션과 함께 `부제meta`/`브올meta`(대시보드), `부제예산`/`브올예산`(일일예산), `브올매출`(브레인올로지 현재 매출) 을 입력하세요.\n예) `@봇이름 브올매출`",
            )
            return
        formatter, fetcher = route
        try:
            client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=formatter(fetcher()))
        except Exception as e:
            client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=f"조회 실패 ❌\n{type(e).__name__}: {e}")
