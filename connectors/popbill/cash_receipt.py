"""
현금영수증 발행/취소 (팝빌 CashbillService).

issue_cash_receipt(data)  -> dict : 즉시 발행(registIssue)
cancel_cash_receipt(mgt_key, memo) -> dict : 발행취소(cancelIssue)

반환 dict: {"ok": bool, "mgtKey": str, "confirmNum": str, "tradeDate": str,
            "code": int|None, "message": str}
"""
import time

from popbill import Cashbill, PopbillException

from common import now_kst, POPBILL_CORP_NUM
from .client import get_cashbill_service


def make_mgt_key() -> str:
    """문서번호(파트너 관리번호) 생성. 24자 이내, 영숫자/-/_ 만 허용."""
    return "CB" + now_kst().strftime("%Y%m%d%H%M%S%f")[:-3]  # CB + 17자리 = 19자


def issue_cash_receipt(data: dict) -> dict:
    """
    data 키:
      tradeUsage  : '소득공제용' | '지출증빙용'
      identityNum : 식별번호 (휴대폰번호/사업자번호/카드번호)
      supplyCost, tax, serviceFee, totalAmount : 금액(int)
      customerName, itemName, email, hp : 선택
    """
    svc = get_cashbill_service()
    mgt_key = make_mgt_key()

    cashbill = Cashbill(
        mgtKey=mgt_key,
        tradeType="승인거래",
        tradeUsage=data["tradeUsage"],
        taxationType="과세",
        totalAmount=str(data["totalAmount"]),
        supplyCost=str(data["supplyCost"]),
        tax=str(data["tax"]),
        serviceFee=str(data.get("serviceFee", 0)),
        franchiseCorpNum=POPBILL_CORP_NUM,
        identityNum=data["identityNum"],
        customerName=data.get("customerName", ""),
        itemName=data.get("itemName", ""),
        email=data.get("email", ""),
        hp=data.get("hp", ""),
        smssendYN=False,
    )

    try:
        res = svc.registIssue(POPBILL_CORP_NUM, cashbill, "")
    except PopbillException as e:
        return {
            "ok": False, "mgtKey": mgt_key, "confirmNum": "", "tradeDate": "",
            "code": e.code, "message": e.message,
        }

    # 국세청 승인번호/거래일자는 발행 후 상세조회로 확인.
    # 발행 직후엔 승인번호가 붙기까지 찰나의 지연이 있어, 잡힐 때까지 잠깐 재조회한다.
    # (실패해도 발행 자체는 성공)
    confirm_num, trade_date = "", ""
    for _ in range(5):
        try:
            info = svc.getInfo(POPBILL_CORP_NUM, mgt_key)
            confirm_num = getattr(info, "confirmNum", "") or ""
            trade_date = getattr(info, "tradeDate", "") or ""
        except PopbillException:
            pass
        if confirm_num:
            break
        time.sleep(0.6)

    return {
        "ok": True, "mgtKey": mgt_key, "confirmNum": confirm_num, "tradeDate": trade_date,
        "code": getattr(res, "code", None), "message": getattr(res, "message", "발행 완료"),
    }


def cancel_cash_receipt(mgt_key: str, memo: str = "") -> dict:
    """
    발행한 현금영수증을 취소.
    현금영수증은 '발행취소(cancelIssue)'가 아니라 원본을 참조하는 '취소거래(revokeRegistIssue)'로
    취소 현금영수증을 새로 발행하는 방식이다. 원본 승인번호/거래일자는 원본 mgtKey로 조회한다.

    인자 mgt_key: 원본 현금영수증 문서번호.
    """
    svc = get_cashbill_service()

    # 1) 원본 정보 조회 (국세청 승인번호 + 거래일자)
    try:
        info = svc.getInfo(POPBILL_CORP_NUM, mgt_key)
        org_confirm = getattr(info, "confirmNum", "") or ""
        org_trade_date = getattr(info, "tradeDate", "") or ""
    except PopbillException as e:
        return {
            "ok": False, "mgtKey": mgt_key, "confirmNum": "", "tradeDate": "",
            "code": e.code, "message": f"원본 조회 실패: {e.message}",
        }

    if not org_confirm or not org_trade_date:
        return {
            "ok": False, "mgtKey": mgt_key, "confirmNum": org_confirm, "tradeDate": org_trade_date,
            "code": None, "message": "원본 승인번호/거래일자를 확인할 수 없어 취소할 수 없습니다.",
        }

    # 2) 취소거래(취소 현금영수증) 발행 — 취소 문서는 새 mgtKey 사용
    cancel_key = make_mgt_key()
    try:
        res = svc.revokeRegistIssue(
            POPBILL_CORP_NUM, cancel_key, org_confirm, org_trade_date,
            smssendYN=False, memo=memo,
        )
    except PopbillException as e:
        return {
            "ok": False, "mgtKey": mgt_key, "confirmNum": org_confirm, "tradeDate": org_trade_date,
            "code": e.code, "message": e.message,
        }
    return {
        "ok": True, "mgtKey": mgt_key, "cancelMgtKey": cancel_key,
        "confirmNum": org_confirm, "tradeDate": org_trade_date,
        "code": getattr(res, "code", None), "message": getattr(res, "message", "취소 완료"),
    }
