from typing import Dict, Any
import pandas as pd
from db.deps import load_structured_by_keys

def _to_df(tj: Dict[str, Any]) -> pd.DataFrame:
    cols = tj.get("columns", [])
    rows = tj.get("rows", [])
    df = pd.DataFrame(rows, columns=cols) if cols else pd.DataFrame(rows)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    return df

def node_table_agent(state: Dict):
    """재생에너지분석전문가 - 신재생 기술, ESG, 투자 트렌드 분석"""
    cands = state.get("candidates", [])
    
    # 재생에너지분석전문가 프롬프트
    renewable_energy_prompt = (
        "You are a senior Renewable Energy Analyst specializing in clean energy technologies.\n\n"
        
        "EXPERTISE AREAS:\n"
        "- Solar Energy: Photovoltaic, CSP, solar storage\n"
        "- Wind Energy: Onshore/offshore wind, turbine technology\n"
        "- Energy Storage: Battery systems, grid-scale storage\n"
        "- Hydrogen: Green hydrogen production and applications\n"
        "- ESG Factors: Sustainability metrics, carbon neutrality\n"
        "- Green Investment: ESG investing, green bonds, clean tech funding\n"
        "- Technology Innovation: R&D trends, efficiency improvements\n\n"
        
        "ANALYSIS FOCUS:\n"
        "1. **Technology Trends**: Innovation, efficiency, cost reduction\n"
        "2. **Market Growth**: Capacity additions, investment flows\n"
        "3. **ESG Impact**: Carbon reduction, sustainability metrics\n"
        "4. **Policy Support**: Green New Deal, renewable incentives\n"
        "5. **Investment Opportunities**: Growth sectors, emerging technologies\n"
        "6. **Future Outlook**: Long-term renewable energy prospects\n\n"
        
        "Provide comprehensive analysis with technology insights, ESG implications, "
        "and investment opportunities in renewable energy sectors.\n"
    )
    
    # Tag context blocks with [S#] to enable inline citations in answers
    blocks = []
    for i, c in enumerate(cands[:8], 1):
        src = f"[S{i}] {c.get('filename','N/A')} p.{c.get('page','N/A')}"
        blocks.append(f"{src}\n{c.get('content','')}")
    context = "\n\n".join(blocks)
    
    # 재생에너지분석전문가 답변 생성
    ans = llm_answer_renewable_energy(state.get("query", ""), context, state.get("history"), renewable_energy_prompt)
    
    state["result"] = {"kind":"text","type":"renewable_energy","answer":ans,"evidence":cands[:8]}
    return state

def llm_answer_renewable_energy(user_query: str, context: str, history: list[dict] | None = None, system_prompt: str = "") -> str:
    """재생에너지분석전문가 답변 생성"""
    from core.settings import PROVIDER, CHAT_MODEL, OPENAI_API_KEY, OLLAMA_HOST, OLLAMA_CHAT_MODEL
    import requests
    
    if PROVIDER == "ollama":
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": [
                {"role":"system","content": system_prompt},
                {"role":"user","content": f"Question:\n{user_query}\n\nContext:\n{context}"}
            ],
            "stream": False,
            "options": {"temperature": 0.0}
        }
        r = requests.post(f"{OLLAMA_HOST}/api/chat", json=payload, timeout=180)
        r.raise_for_status()
        return r.json()["message"]["content"]

    # openai
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Question:\n{user_query}\n\nContext:\n{context}"}
        ],
        temperature=0.0,
        max_tokens=4000,
    )
    return resp.choices[0].message.content
