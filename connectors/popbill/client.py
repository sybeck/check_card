"""
팝빌 SDK 서비스 객체 생성 + 공통 설정 주입.

의존성: popbill (pip install popbill)
.env: POPBILL_LINK_ID, POPBILL_SECRET_KEY, POPBILL_IS_TEST, POPBILL_CORP_NUM

서비스 객체는 매 호출마다 새로 만들지 않고 모듈 레벨에서 한 번만 생성해 재사용한다.
"""
from popbill import CashbillService, TaxinvoiceService

from common import (
    POPBILL_LINK_ID,
    POPBILL_SECRET_KEY,
    POPBILL_IS_TEST,
    POPBILL_CORP_NUM,
)

_cashbill_service = None
_taxinvoice_service = None


def _require_credentials() -> None:
    if not POPBILL_LINK_ID or not POPBILL_SECRET_KEY:
        raise RuntimeError(
            "[ENV ERROR] POPBILL_LINK_ID / POPBILL_SECRET_KEY 가 필요합니다. (.env 확인)"
        )
    if not POPBILL_CORP_NUM or len(POPBILL_CORP_NUM) != 10:
        raise RuntimeError(
            "[ENV ERROR] POPBILL_CORP_NUM(사업자번호 10자리, 하이픈 없이) 이 필요합니다. (.env 확인)"
        )


def _configure(svc):
    """모든 팝빌 서비스 공통 설정."""
    svc.IsTest = POPBILL_IS_TEST          # True=테스트베드, False=운영
    svc.IPRestrictOnOff = True            # 팝빌에 등록된 IP만 허용(보안 권장)
    svc.UseStaticIP = False               # 고정 IP 회선(g1) 사용 안 함
    svc.UseLocalTimeYN = True             # 로컬(서버) 시간 사용
    return svc


def get_cashbill_service() -> CashbillService:
    global _cashbill_service
    if _cashbill_service is None:
        _require_credentials()
        _cashbill_service = _configure(
            CashbillService(POPBILL_LINK_ID, POPBILL_SECRET_KEY)
        )
    return _cashbill_service


def get_taxinvoice_service() -> TaxinvoiceService:
    global _taxinvoice_service
    if _taxinvoice_service is None:
        _require_credentials()
        _taxinvoice_service = _configure(
            TaxinvoiceService(POPBILL_LINK_ID, POPBILL_SECRET_KEY)
        )
    return _taxinvoice_service


def get_balance() -> float:
    """연동 점검용: 잔여 포인트 조회. 정상 연동이면 잔액(float)을 반환."""
    return get_cashbill_service().getBalance(POPBILL_CORP_NUM)
