"""
Meta 광고 성과 개인별 집계 결과를 노션 데이터베이스에 한 페이지로 기록한다.

의존성: notion-client
  pip install notion-client

.env 설정:
  NOTION_TOKEN=<내부 통합(Integration) 토큰>
  NOTION_PERFORMANCE_DB_ID=<데이터베이스 ID 또는 URL>

대상 DB 는 위 통합과 연결(공유)되어 있어야 한다.
속성(properties)에는 제목 + '기간' + '집계일시' 만,
나머지 8개 지표는 본문 표(키워드별 행)로 정리한다.
"""
import os
import re

from notion_client import Client
from dotenv import load_dotenv

load_dotenv()

# 본문 표 컬럼 (키워드 + 지표 8개)
TABLE_HEADERS = [
    "키워드",
    "구매전환값",
    "ROAS",
    "구매당비용",
    "유입대비전환율",
    "객단가",
    "구매",
    "지출",
    "CPC",
]

# 제품별 성과 표 컬럼 (키워드 + 제품 + 지표 8개)
PRODUCT_TABLE_HEADERS = [
    "키워드",
    "제품",
    "구매전환값",
    "ROAS",
    "구매당비용",
    "유입대비전환율",
    "객단가",
    "구매",
    "지출",
    "CPC",
]

PERIOD_PROP = "기간"
GENERATED_PROP = "집계일시"


def _must_env(key: str) -> str:
    v = os.getenv(key, "").strip()
    if not v:
        raise RuntimeError(f"[ENV ERROR] {key} 가 필요합니다. .env를 확인하세요.")
    return v


def _extract_db_id(value: str) -> str:
    """URL이 와도 32자리 hex DB ID 추출 (대시 유무 모두 허용)."""
    value = (value or "").strip()
    m = re.search(r"([0-9a-fA-F]{32})", value.replace("-", ""))
    return m.group(1) if m else value


def _fmt_money(v) -> str:
    return f"{v:,.0f}"


def _fmt_pct(v) -> str:
    return f"{v:.2f}%"


def _fmt_roas(v) -> str:
    return f"{v:.2f}"


def _row_cells(g: dict) -> list:
    """그룹 dict -> 표 한 행(문자열 리스트). TABLE_HEADERS 순서."""
    return [
        g["label"],
        _fmt_money(g["revenue"]),
        _fmt_roas(g["roas"]),
        _fmt_money(g["cpa"]),
        _fmt_pct(g["conv_rate"]),
        _fmt_money(g["aov"]),
        f"{g['purchases']:,}",
        _fmt_money(g["spend"]),
        _fmt_money(g["cpc"]),
    ]


def _product_row_cells(g: dict) -> list:
    """제품별 성과 한 행. PRODUCT_TABLE_HEADERS 순서."""
    return [
        g["keyword"],
        g["product"],
        _fmt_money(g["revenue"]),
        _fmt_roas(g["roas"]),
        _fmt_money(g["cpa"]),
        _fmt_pct(g["conv_rate"]),
        _fmt_money(g["aov"]),
        f"{g['purchases']:,}",
        _fmt_money(g["spend"]),
        _fmt_money(g["cpc"]),
    ]


def _rich(text: str) -> list:
    return [{"type": "text", "text": {"content": str(text)}}]


def _table_row(cells) -> dict:
    return {
        "object": "block",
        "type": "table_row",
        "table_row": {"cells": [_rich(c) for c in cells]},
    }


def _build_children(report: dict) -> list:
    since, until = report["period"]
    rows = [g for g in report["groups"]] + [report["total"]]

    table_children = [_table_row(TABLE_HEADERS)]
    table_children += [_table_row(_row_cells(g)) for g in rows]

    children = [
        {
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": _rich("📊 Meta 광고 성과 개인별 집계 (당월에 제작된 콘텐츠 한정 집계)")
            },
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": _rich(
                    f"기간: {since} ~ {until} · 대상: {report['account']} · "
                    f"집계일시: {report['generated_at']} · 수집 행수: {report['row_count']}"
                )
            },
        },
        {
            "object": "block",
            "type": "table",
            "table": {
                "table_width": len(TABLE_HEADERS),
                "has_column_header": True,
                "has_row_header": True,
                "children": table_children,
            },
        },
    ]

    # 제품별 성과 표 (지출 0 행은 build_report 단계에서 이미 제외)
    product_groups = report.get("product_groups") or []
    if product_groups:
        prod_children = [_table_row(PRODUCT_TABLE_HEADERS)]
        prod_children += [_table_row(_product_row_cells(g)) for g in product_groups]
        children += [
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": _rich("🧩 제품별 성과")},
            },
            {
                "object": "block",
                "type": "table",
                "table": {
                    "table_width": len(PRODUCT_TABLE_HEADERS),
                    "has_column_header": True,
                    "has_row_header": False,
                    "children": prod_children,
                },
            },
        ]

    return children


def _resolve_schema_and_parent(client, db_id: str):
    """
    DB의 속성 스키마와 페이지 생성용 parent 를 반환.
    구형(API): databases.retrieve 가 properties 를 직접 제공 → parent=database_id
    신형(데이터 소스 모델): properties 가 비어 있고 data_sources 제공
        → 데이터 소스에서 properties 조회, parent=data_source_id
    """
    db = client.databases.retrieve(database_id=db_id)
    props = db.get("properties") or {}
    if props:
        return props, {"database_id": db_id}

    data_sources = db.get("data_sources") or []
    if not data_sources:
        raise RuntimeError(
            "노션 DB에서 속성을 찾지 못했습니다 (properties/data_sources 모두 비어 있음). "
            "통합 연결/DB ID를 확인하세요."
        )
    ds_id = data_sources[0]["id"]
    ds = client.request(path=f"data_sources/{ds_id}", method="GET")
    return (ds.get("properties") or {}), {"type": "data_source_id", "data_source_id": ds_id}


def _build_properties(props_schema: dict, report: dict) -> dict:
    """DB 스키마에 맞춰 제목 + 기간 + 집계일시 속성을 구성. 없는 속성은 건너뛴다."""
    since, until = report["period"]
    properties = {}

    # 1) title 타입 속성 자동탐지 -> 제목
    for name, meta in props_schema.items():
        if meta.get("type") == "title":
            properties[name] = {"title": _rich(f"성과 {since}~{until}")}
            break

    # 2) '기간' 속성: date 면 범위, rich_text 면 문자열
    period_meta = props_schema.get(PERIOD_PROP)
    if period_meta:
        if period_meta.get("type") == "date":
            properties[PERIOD_PROP] = {"date": {"start": since, "end": until}}
        elif period_meta.get("type") == "rich_text":
            properties[PERIOD_PROP] = {"rich_text": _rich(f"{since} ~ {until}")}

    # 3) '집계일시' 속성: date 면 날짜, rich_text 면 문자열
    gen_meta = props_schema.get(GENERATED_PROP)
    if gen_meta:
        gen = report["generated_at"]
        if gen_meta.get("type") == "date":
            # "YYYY-MM-DD HH:MM:SS" -> ISO (KST +09:00)
            properties[GENERATED_PROP] = {
                "date": {"start": gen.replace(" ", "T") + "+09:00"}
            }
        elif gen_meta.get("type") == "rich_text":
            properties[GENERATED_PROP] = {"rich_text": _rich(gen)}

    return properties


def create_performance_page(report: dict) -> str:
    """노션 DB에 성과 페이지를 생성하고 페이지 URL을 반환한다."""
    token = _must_env("NOTION_TOKEN")
    db_id = _extract_db_id(_must_env("NOTION_PERFORMANCE_DB_ID"))

    client = Client(auth=token)
    props_schema, parent = _resolve_schema_and_parent(client, db_id)
    properties = _build_properties(props_schema, report)
    children = _build_children(report)

    page = client.pages.create(
        parent=parent,
        properties=properties,
        children=children,
    )
    return page.get("url", "")
