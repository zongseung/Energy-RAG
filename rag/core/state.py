from typing import List, Dict, Any, TypedDict

class QAState(TypedDict, total=False):
    query: str
    query_embedding: List[float]
    category: str
    candidates: List[Dict[str, Any]]
    intent: Dict[str, Any]
    route: str
    result: Dict[str, Any]          # agent 1회 실행 결과
    partials: List[Dict[str, Any]]  # 누적된 결과
    final: str                      # supervisor가 합성한 최종 답변
    target_years: List[int]         # 연도 필터링
    history: List[Dict[str, Any]]   # 대화 히스토리
    analysis_intent: Dict[str, Any] # 분석 의도
    reflection_count: int           # 성찰 횟수
