from typing import Dict
from core.settings import PROVIDER, CHAT_MODEL, OPENAI_API_KEY, OLLAMA_HOST, OLLAMA_CHAT_MODEL
import requests

_openai_client = None
def _get_openai():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client

def _history_to_messages(history: list[dict]) -> list[dict]:
    msgs = []
    for h in history or []:
        role = h.get("role")
        content = h.get("content", "")
        if role in {"user", "assistant", "system"} and content:
            msgs.append({"role": role, "content": content})
    return msgs

REFLECTION_PROMPT = (
    "You are a senior energy industry analyst reviewing and improving your previous analysis.\n\n"
    
    "CRITICAL REFLECTION CRITERIA:\n"
    "1. **ACCURACY**: Are all facts, figures, and claims directly supported by the source documents?\n"
    "2. **COMPLETENESS**: Did I miss any important aspects of the energy market analysis?\n"
    "3. **DEPTH**: Can I provide more specific data, percentages, or quantitative insights?\n"
    "4. **BALANCE**: Did I address both opportunities and risks comprehensively?\n"
    "5. **CITATIONS**: Are all sources properly cited with [S# p.PAGE] format?\n"
    "6. **LOGIC**: Do my conclusions logically follow from the evidence?\n"
    "7. **RELEVANCE**: Is my analysis directly answering the user's question?\n"
    "8. **INVESTMENT VALUE**: Are my insights actionable for investment decisions?\n\n"
    
    "IMPROVEMENT INSTRUCTIONS:\n"
    "- Add missing quantitative data or financial metrics\n"
    "- Strengthen weak arguments with more evidence\n"
    "- Remove any unsupported claims\n"
    "- Enhance strategic insights and recommendations\n"
    "- Ensure professional analyst-level depth\n\n"
    
    "Based on the original question and source documents, provide an IMPROVED version of the analysis."
)

def llm_reflection(query: str, context: str, previous_answer: str, iteration: int) -> str:
    """성찰을 통한 답변 개선"""
    reflection_query = (
        f"ORIGINAL QUESTION: {query}\n\n"
        f"PREVIOUS ANALYSIS (Iteration {iteration}):\n{previous_answer}\n\n"
        f"Please improve this analysis based on the reflection criteria above."
    )
    
    if PROVIDER == "ollama":
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": [
                {"role": "system", "content": REFLECTION_PROMPT},
                {"role": "user", "content": f"Context:\n{context}\n\n{reflection_query}"}
            ],
            "stream": False,
            "options": {"temperature": 0.1}  # 성찰에서는 낮은 temperature
        }
        r = requests.post(f"{OLLAMA_HOST}/api/chat", json=payload, timeout=180)
        r.raise_for_status()
        return r.json()["message"]["content"]

    # openai
    client = _get_openai()
    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": REFLECTION_PROMPT},
            {"role": "user", "content": f"Context:\n{context}\n\n{reflection_query}"}
        ],
        temperature=0.1,
        max_tokens=4000,
    )
    return resp.choices[0].message.content

def node_reflection_agent(state: Dict):
    """에너지 산업 분석 성찰 및 개선 에이전트 (1회 반복)"""
    
    # 이전 결과 확인
    result = state.get("result", {})
    if not result or "answer" not in result:
        # 성찰할 답변이 없으면 그대로 반환
        return state
    
    query = state.get("query", "")
    candidates = state.get("candidates", [])
    
    # 컨텍스트 재구성
    blocks = []
    for i, c in enumerate(candidates[:8], 1):
        src = f"[S{i}] {c.get('filename','N/A')} p.{c.get('page','N/A')}"
        blocks.append(f"{src}\n{c.get('content','')}")
    context = "\n\n".join(blocks)
    
    # 1회 성찰 수행
    previous_answer = result.get("answer", "")
    improved_answer = llm_reflection(query, context, previous_answer, 1)
    
    # 개선된 답변으로 업데이트
    state["result"]["answer"] = improved_answer
    state["reflection_count"] = 1
    
    return state
