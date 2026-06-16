"""
서비스 계정 기반 구글 스프레드시트 클라이언트.

의존성: gspread, google-auth
  pip install gspread google-auth

.env 설정:
  GOOGLE_SERVICE_ACCOUNT_JSON=C:\\path\\to\\service_account.json
  META_CREATIVE_SHEET_ID=<스프레드시트 ID 또는 전체 URL>
  META_CREATIVE_WORKSHEET=creative_raw

대상 스프레드시트는 서비스 계정 이메일(client_email)에 '편집자'로 공유되어 있어야 한다.
"""
import os
import re
from typing import Dict, List, Sequence, Tuple

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"[ENV ERROR] {key} 가 필요합니다. .env를 확인하세요.")
    return v


def _extract_sheet_id(value: str) -> str:
    """전체 URL이 들어와도 스프레드시트 ID만 추출."""
    value = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    if m:
        return m.group(1)
    return value


def get_client() -> gspread.Client:
    key_path = _must_env("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not os.path.exists(key_path):
        raise RuntimeError(
            f"[ENV ERROR] GOOGLE_SERVICE_ACCOUNT_JSON 경로의 파일이 없습니다: {key_path}"
        )
    creds = Credentials.from_service_account_file(key_path, scopes=SCOPES)
    return gspread.authorize(creds)


def get_worksheet(header: Sequence[str]) -> gspread.Worksheet:
    """
    스프레드시트를 열고 대상 워크시트를 반환한다.
    워크시트가 없으면 생성하고 헤더 1행을 기록한다.
    헤더가 비어 있으면 헤더를 써넣는다.
    """
    client = get_client()
    sheet_id = _extract_sheet_id(_must_env("META_CREATIVE_SHEET_ID"))
    ws_name = os.getenv("META_CREATIVE_WORKSHEET", "creative_raw").strip() or "creative_raw"

    spreadsheet = client.open_by_key(sheet_id)
    try:
        ws = spreadsheet.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=ws_name, rows=1000, cols=max(len(header), 16)
        )
        ws.update([list(header)], "A1")
        return ws

    # 기존 워크시트 헤더 확인 — 비어 있으면 기록
    first_row = ws.row_values(1)
    if not first_row:
        ws.update([list(header)], "A1")
    return ws


def _col_letter(n: int) -> str:
    """1-based 컬럼 번호 -> 스프레드시트 컬럼 문자 (1->A, 27->AA)."""
    s = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


def upsert_rows(
    ws: gspread.Worksheet,
    rows: List[Dict[str, object]],
    header: Sequence[str],
    key_cols: Tuple[str, ...] = ("일자", "ad_id"),
) -> Dict[str, int]:
    """
    key_cols 기준으로 기존 행은 갱신(batch_update), 신규 행은 일괄 추가(append_rows).
    재실행/소급수정에도 중복 없이 누적된다.

    반환: {"updated": n, "appended": m}
    """
    header = list(header)
    if not rows:
        return {"updated": 0, "appended": 0}

    # 키 컬럼의 0-based 인덱스
    key_idx = [header.index(k) for k in key_cols]

    # 현재 시트 전체 읽기 (헤더 포함)
    all_values = ws.get_all_values()
    existing_index: Dict[Tuple[str, ...], int] = {}  # key -> 1-based 행번호
    for i, row in enumerate(all_values):
        if i == 0:
            continue  # 헤더
        try:
            key = tuple((row[ix] if ix < len(row) else "").strip() for ix in key_idx)
        except Exception:
            continue
        # 빈 행 스킵
        if all(not c for c in key):
            continue
        existing_index[key] = i + 1  # gspread는 1-based

    def row_to_list(r: Dict[str, object]) -> List[object]:
        return [r.get(col, "") for col in header]

    updates = []  # batch_update payload
    appends = []  # 신규 행(값 리스트)
    last_col = _col_letter(len(header))

    for r in rows:
        key = tuple(str(r.get(header[ix], "")).strip() for ix in key_idx)
        values = row_to_list(r)
        if key in existing_index:
            row_no = existing_index[key]
            updates.append(
                {
                    "range": f"A{row_no}:{last_col}{row_no}",
                    "values": [values],
                }
            )
        else:
            appends.append(values)

    # 대용량 백필(수천 행) 대비: 단일 요청 payload가 과도하게 커지지 않도록
    # 청크 단위로 나눠 쓴다. (쿼터에는 거의 영향 없음 — 호출 몇 번 추가될 뿐)
    WRITE_CHUNK = 2000

    for i in range(0, len(updates), WRITE_CHUNK):
        chunk = updates[i : i + WRITE_CHUNK]
        ws.batch_update(
            [{"range": u["range"], "values": u["values"]} for u in chunk],
            value_input_option="USER_ENTERED",
        )

    # 신규 행은 append_rows(테이블 자동 감지)를 쓰지 않는다.
    # 시트에 무관한 열(예: R열 이후)을 임의로 추가하면 자동 감지가 표의 시작 열을
    # 잘못 잡아 신규 행이 A열이 아닌 엉뚱한 열부터 쌓이는 문제가 있었다.
    # 항상 'A열 + 실제 데이터(키가 채워진 행)의 마지막 다음 행'에 명시적 범위로 기록한다.
    if appends:
        # existing_index 는 키(일자/ad_id)가 채워진 '실제' 행만 담으므로,
        # A열이 빈 임의 추가열/오기록 행은 자연히 무시된다.
        start_row = (max(existing_index.values()) + 1) if existing_index else 2

        # 그리드 행이 부족하면 확장 (대량 백필 안전장치)
        need_last = start_row + len(appends) - 1
        if need_last > ws.row_count:
            ws.add_rows(need_last - ws.row_count)

        for i in range(0, len(appends), WRITE_CHUNK):
            chunk = appends[i : i + WRITE_CHUNK]
            r1 = start_row + i
            r2 = r1 + len(chunk) - 1
            ws.update(
                chunk,
                f"A{r1}:{last_col}{r2}",
                value_input_option="USER_ENTERED",
            )

    return {"updated": len(updates), "appended": len(appends)}
