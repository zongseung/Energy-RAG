from typing import Dict
from core.settings import PROVIDER, CHAT_MODEL, OPENAI_API_KEY, OLLAMA_HOST, OLLAMA_CHAT_MODEL
import requests
from langchain import hub
from langchain_teddynote.messages import stream_response
import io
import sys

_openai_client = None
def _get_openai():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client

SYSTEM_PROMPT = (
    "You are a thorough energy-market analyst.\n"
    "Answer ONLY using the provided context documents.\n"
    "Write a comprehensive, detailed answer with:\n"
    "- A clear, direct conclusion first (2-3 sentences).\n"
    "- 5-8 detailed bullet points with specific figures, data, comparisons, trends, and important caveats.\n"
    "- Include quantitative data, percentages, dates, and specific examples when available.\n"
    "- Explain the implications and significance of the information.\n"
    "- Cite 3-6 sources inline as [S# p.PAGE].\n"
    "- Aim for 300-500 words minimum.\n"
    "If the requested information is not in the context, respond: 'I don't have information about that in the provided documents.'\n"
)

def _render_cod_prompt(
    context: str,
    *,
    content_category: str = "research report",
    entity_range: str = "1-5",
    max_word: int = 3000,
    iteration: int = 3,
) -> str:
    """Render Chain-of-Density prompt from Hub with variables filled."""
    cod = hub.pull("nicobutes/chain-of-density-prompt")

    # Attempt .format(**kwargs)
    try:
        return cod.format(
            content_category=content_category,
            content=context,
            entity_range=entity_range,
            max_word=max_word,
            iteration=iteration,
        )
    except Exception:
        pass

    # Attempt attribute .template then Python format
    try:
        templ = getattr(cod, "template", None)
        if templ:
            return templ.format(
                content_category=content_category,
                content=context,
                entity_range=entity_range,
                max_word=max_word,
                iteration=iteration,
            )
    except Exception:
        pass

    # Attempt .invoke(kwargs) returning messages
    rendered = cod.invoke({
        "content_category": content_category,
        "content": context,
        "entity_range": entity_range,
        "max_word": max_word,
        "iteration": iteration,
    })
    # Join message contents if it's a list of messages
    if isinstance(rendered, (list, tuple)):
        return "\n".join(getattr(m, "content", str(m)) for m in rendered)
    # Or single object with .content/.text
    return getattr(rendered, "content", getattr(rendered, "text", str(rendered)))

def _history_to_messages(history: list[dict]) -> list[dict]:
    msgs = []
    for h in history or []:
        role = h.get("role")
        content = h.get("content", "")
        if role in {"user", "assistant", "system"} and content:
            msgs.append({"role": role, "content": content})
    return msgs

def llm_answer(user_query: str, context: str, history: list[dict] | None = None) -> str:
    """에너지 산업 분석 초기 답변 생성 (성찰은 reflection_agent에서 처리)"""
    energy_analyst_prompt = (
        "You are a senior energy industry and market analyst with 15+ years of experience.\n\n"
        
        "EXPERTISE AREAS:\n"
        "- Energy market trends, pricing, and forecasting\n"
        "- Renewable energy technologies (solar, wind, hydro, battery storage)\n"
        "- Traditional energy sectors (oil, gas, coal, nuclear)\n"
        "- Energy policy, regulations, and government incentives\n"
        "- Corporate strategy, M&A, and investment analysis\n"
        "- ESG factors and sustainability metrics\n"
        "- Supply chain and infrastructure analysis\n\n"
        
        "ANALYSIS FRAMEWORK:\n"
        "1. **Market Overview**: Current market conditions and key drivers\n"
        "2. **Trend Analysis**: Short-term and long-term industry trends\n"
        "3. **Financial Metrics**: Revenue, margins, ROI, CAPEX, market cap analysis\n"
        "4. **Competitive Landscape**: Key players, market share, competitive advantages\n"
        "5. **Risk Assessment**: Market risks, regulatory risks, technology risks\n"
        "6. **Investment Implications**: Buy/Hold/Sell recommendations with rationale\n"
        "7. **Future Outlook**: 1-3 year and 5-10 year projections\n\n"
        
        "RESPONSE REQUIREMENTS:\n"
        "- Provide data-driven insights with specific numbers, percentages, and dates\n"
        "- Include market valuations, growth rates, and financial projections\n"
        "- Cite sources using [S# p.PAGE] format for all claims\n"
        "- Compare with industry benchmarks and peer companies\n"
        "- Address both opportunities and risks\n"
        "- Conclude with actionable investment insights\n"
        "- Write 800-1500 words for comprehensive analysis\n\n"
        
        "Answer ONLY using the provided context documents. If information is not available, "
        "state: 'This information is not available in the provided documents.'\n"
    )
    
    if PROVIDER == "ollama":
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": [
                {"role":"system","content": energy_analyst_prompt},
                *(_history_to_messages(history)),
                {"role":"user","content": f"Question:\n{user_query}\n\nContext:\n{context}"}
            ],
            "stream": False,
            "options": {"temperature": 0.0}
        }
        r = requests.post(f"{OLLAMA_HOST}/api/chat", json=payload, timeout=180)
        r.raise_for_status()
        return r.json()["message"]["content"]

    # openai
    client = _get_openai()
    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": energy_analyst_prompt},
            *(_history_to_messages(history)),
            {"role": "user", "content": f"Question:\n{user_query}\n\nContext:\n{context}"}
        ],
        temperature=0.0,
        max_tokens=4000,
    )
    return resp.choices[0].message.content

def node_text_agent(state: Dict):
    """에너지산업분석전문가 - 전통 에너지, 정책, 시장 분석"""
    cands = state.get("candidates", [])
    
    # 에너지산업분석전문가 프롬프트
    energy_industry_prompt = (
        "You are a senior Energy Industry Analyst specializing in traditional energy sectors.\n\n"
        
        "EXPERTISE AREAS:\n"
        "- Oil & Gas: Upstream, midstream, downstream operations\n"
        "- Coal & Nuclear: Traditional power generation\n"
        "- Energy Policy: Regulations, government incentives, policy changes\n"
        "- Market Dynamics: Supply/demand, pricing, macroeconomic impacts\n"
        "- Corporate Strategy: M&A, investments, financial performance\n"
        "- Energy Security: Geopolitical risks, supply chain analysis\n\n"
        
        "ANALYSIS FOCUS:\n"
        "1. **Market Overview**: Current energy market conditions and drivers\n"
        "2. **Policy Impact**: Regulatory changes and government support\n"
        "3. **Financial Performance**: Revenue, margins, CAPEX analysis\n"
        "4. **Competitive Landscape**: Market share, strategic positioning\n"
        "5. **Risk Assessment**: Geopolitical, regulatory, market risks\n"
        "6. **Investment Outlook**: Traditional energy sector prospects\n\n"
        
        "Provide comprehensive analysis with specific data, policy implications, "
        "and investment insights for traditional energy sectors.\n"
    )
    
    # Tag context blocks with [S#] to enable inline citations in answers
    blocks = []
    for i, c in enumerate(cands[:8], 1):
        src = f"[S{i}] {c.get('filename','N/A')} p.{c.get('page','N/A')}"
        blocks.append(f"{src}\n{c.get('content','')}")
    context = "\n\n".join(blocks)
    
    # 에너지산업분석전문가 답변 생성
    ans = llm_answer_energy_industry(state.get("query", ""), context, state.get("history"), energy_industry_prompt)
    
    state["result"] = {"kind":"text","type":"energy_industry","answer":ans,"evidence":cands[:8]}
    return state

def llm_answer_energy_industry(user_query: str, context: str, history: list[dict] | None = None, system_prompt: str = "") -> str:
    """에너지산업분석전문가 답변 생성"""
    if PROVIDER == "ollama":
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": [
                {"role":"system","content": system_prompt},
                *(_history_to_messages(history)),
                {"role":"user","content": f"Question:\n{user_query}\n\nContext:\n{context}"}
            ],
            "stream": False,
            "options": {"temperature": 0.0}
        }
        r = requests.post(f"{OLLAMA_HOST}/api/chat", json=payload, timeout=180)
        r.raise_for_status()
        return r.json()["message"]["content"]

    # openai
    client = _get_openai()
    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            *(_history_to_messages(history)),
            {"role": "user", "content": f"Question:\n{user_query}\n\nContext:\n{context}"}
        ],
        temperature=0.0,
        max_tokens=4000,
    )
    return resp.choices[0].message.content
