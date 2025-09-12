from typing import List, Dict, Any, TypedDict

class QAState(TypedDict, total=False):
    query: str
    query_embedding: List[float]
    category: str
    candidates: List[Dict[str, Any]]
    intent: Dict[str, Any]
    route: str
    result: Dict[str, Any]
    final: str