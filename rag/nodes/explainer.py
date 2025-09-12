from typing import Dict

def node_explainer(state: Dict):
    """최종 답변 정리 및 참고 문서 추가"""
    r = state.get("result", {})
    candidates = state.get("candidates", [])
    
    # 기본 답변 추출
    answer = r.get("answer", "")
    
    if not answer:
        state["final"] = "죄송합니다. 답변을 생성할 수 없습니다."
        return state
    
    # 참고 문서 목록 추가
    if candidates:
        answer += "\n\n**참고 문서:**\n"
        for i, c in enumerate(candidates[:8], 1):
            filename = c.get('filename', 'N/A')
            page = c.get('page', 'N/A')
            answer += f"{i}. {filename} (p.{page})\n"
    
    state["final"] = answer
    return state
