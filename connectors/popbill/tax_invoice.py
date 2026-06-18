"""
전자세금계산서 발행/취소 (팝빌 TaxinvoiceService).

issue_tax_invoice(data)  -> dict : 즉시 발행(registIssue, 정발행)
cancel_tax_invoice(mgt_key, memo) -> dict : 발행취소(cancelIssue)

공급자(발행자) 정보는 common.POPBILL_INVOICER / POPBILL_CORP_NUM 고정값을 사용.
반환 dict: {"ok", "mgtKey", "ntsConfirmNum", "code", "message"}
"""
import time

from popbill import Taxinvoice, TaxinvoiceDetail, PopbillException

from common import now_kst, POPBILL_CORP_NUM, POPBILL_INVOICER
from .client import get_taxinvoice_service

# 문서 관리번호 종류: 매출(SELL) 기준으로 발행/조회/취소
# (popbill SDK는 MgtKeyType 상수를 export 하지 않아 문자열을 직접 사용)
_KEYTYPE = "SELL"


def make_mgt_key() -> str:
    """문서번호(파트너 관리번호) 생성. 24자 이내, 영숫자/-/_ 만 허용."""
    return "TI" + now_kst().strftime("%Y%m%d%H%M%S%f")[:-3]  # TI + 17자리 = 19자


def issue_tax_invoice(data: dict) -> dict:
    """
    data 키:
      writeDate        : 작성일자 'yyyyMMdd'
      taxType          : '과세' | '영세' | '면세'
      purposeType      : '영수' | '청구'
      invoiceeCorpNum  : 공급받는자 사업자번호(10자리)
      invoiceeCorpName : 상호
      invoiceeCEOName  : 대표자 성명
      invoiceeEmail1   : 공급받는자 담당자 이메일(발행 메일 수신)
      invoiceeAddr     : 주소(선택)
      itemName         : 품목명
      supplyCostTotal, taxTotal, totalAmount : 금액(int)
      memo             : 비고(선택)
    """
    svc = get_taxinvoice_service()
    mgt_key = make_mgt_key()
    inv = POPBILL_INVOICER

    taxinvoice = Taxinvoice(
        writeDate=data["writeDate"],
        chargeDirection="정과금",      # 과금방향: 정과금(공급자 발행)
        issueType="정발행",
        purposeType=data.get("purposeType", "영수"),
        taxType=data.get("taxType", "과세"),

        # --- 공급자(발행자) : 고정값 ---
        invoicerCorpNum=POPBILL_CORP_NUM,
        invoicerCorpName=inv["corpName"],
        invoicerMgtKey=mgt_key,
        invoicerCEOName=inv["ceoName"],
        invoicerAddr=inv["addr"],
        invoicerBizType=inv["bizType"],
        invoicerBizClass=inv["bizClass"],
        invoicerContactName=inv["contactName"],
        invoicerTEL=inv["tel"],
        invoicerEmail=inv["email"],

        # --- 공급받는자 : 모달 입력 ---
        invoiceeType="사업자",
        invoiceeCorpNum=data["invoiceeCorpNum"],
        invoiceeCorpName=data["invoiceeCorpName"],
        invoiceeCEOName=data["invoiceeCEOName"],
        invoiceeAddr=data.get("invoiceeAddr", ""),
        invoiceeEmail1=data.get("invoiceeEmail1", ""),

        # --- 금액 ---
        supplyCostTotal=str(data["supplyCostTotal"]),
        taxTotal=str(data["taxTotal"]),
        totalAmount=str(data["totalAmount"]),
        modifyYN=False,

        detailList=[
            TaxinvoiceDetail(
                serialNum=1,
                itemName=data.get("itemName", ""),
                supplyCost=str(data["supplyCostTotal"]),
                tax=str(data["taxTotal"]),
            )
        ],
    )

    memo = data.get("memo", "")
    try:
        res = svc.registIssue(
            POPBILL_CORP_NUM, taxinvoice,
            writeSpecification=False, forceIssue=False,
            dealInvoiceMgtKey=None, memo=memo,
        )
    except PopbillException as e:
        return {
            "ok": False, "mgtKey": mgt_key, "ntsConfirmNum": "",
            "code": e.code, "message": e.message,
        }

    # 국세청 승인번호는 발행 후 상세조회로 확인. 잡힐 때까지 잠깐 재조회.
    # (운영에서는 국세청 전송까지 더 걸릴 수 있어 그대로 '(조회 지연)'으로 남을 수 있음 — 정상)
    nts = ""
    for _ in range(5):
        try:
            info = svc.getInfo(POPBILL_CORP_NUM, _KEYTYPE, mgt_key)
            nts = getattr(info, "ntsconfirmNum", "") or getattr(info, "ntsConfirmNum", "") or ""
        except PopbillException:
            pass
        if nts:
            break
        time.sleep(0.6)

    return {
        "ok": True, "mgtKey": mgt_key, "ntsConfirmNum": nts,
        "code": getattr(res, "code", None), "message": getattr(res, "message", "발행 완료"),
    }


def cancel_tax_invoice(mgt_key: str, memo: str = "") -> dict:
    """발행한 세금계산서를 문서번호(mgtKey)로 발행취소."""
    svc = get_taxinvoice_service()
    try:
        res = svc.cancelIssue(POPBILL_CORP_NUM, _KEYTYPE, mgt_key, memo)
    except PopbillException as e:
        return {
            "ok": False, "mgtKey": mgt_key, "ntsConfirmNum": "",
            "code": e.code, "message": e.message,
        }
    return {
        "ok": True, "mgtKey": mgt_key, "ntsConfirmNum": "",
        "code": getattr(res, "code", None), "message": getattr(res, "message", "취소 완료"),
    }
