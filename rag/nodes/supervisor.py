# nodes/supervisor.py
from typing import Dict, Any, List

# 에너지 산업 분석 키워드
ENERGY_INDUSTRY_HINTS = ["석유","가스","석탄","원자력","정책","규제","시장","가격","수급","M&A","기업","투자","정부","요약","분석","전망"]
RENEWABLE_ENERGY_HINTS = ["태양광","풍력","수력","배터리","수소","재생","신재생","ESG","그린","친환경","기술","혁신","효율","신재생에너지","재생에너지"]
POLICY_HINTS = ["정책","규제","법률","인센티브","세제","지원","정부","제도","요약","분석","전망"]

def _compose_final(parts: List[Dict[str, Any]]) -> str:
    # 필요 시 템플릿 고도화
    joined = "\n\n".join(str(p.get("content", "")) for p in parts)
    return f"최종 답변:\n{joined}"

def node_supervisor(state: Dict[str, Any]) -> Dict[str, Any]:
    """에너지 산업 분석 전문 수퍼바이저 - 쿼리 유형을 분석하여 적절한 에이전트로 라우팅"""
    
    # 0) 이미 최종 답변이 있으면 종료
    if state.get("final"):
        state["route"] = "done"
        return state

    # 1) 최신 result를 최상위 partials에 누적 (answer는 보존)
    top_parts: List[Dict[str, Any]] = state.setdefault("partials", [])
    incoming = state.get("result")

    if isinstance(incoming, dict) and incoming and "kind" in incoming:
        top_parts.append(incoming)

    # 2) 어떤 종류가 모였는지 확인
    has_table = any(p.get("kind") == "table" for p in top_parts)
    has_text  = any(p.get("kind") == "text"  for p in top_parts)

    # 3) 두 종류가 모두 모이면 최종 합성 후 종료
    if has_table and has_text:
        state["final"] = _compose_final(top_parts)
        state["route"] = "done"
        return state

    # 4) 에너지 산업 분석을 위한 라우팅 결정
    ql = (state.get("query") or "").lower()
    cands = state.get("candidates") or []
    
    # 에너지 산업 분석 유형 판단
    requires_energy_industry = any(h in ql for h in ENERGY_INDUSTRY_HINTS)
    requires_renewable_energy = any(h in ql for h in RENEWABLE_ENERGY_HINTS)
    requires_policy_analysis = any(h in ql for h in POLICY_HINTS)
    
    # 분석 의도 저장
    state["analysis_intent"] = {
        "energy_industry": requires_energy_industry,
        "renewable_energy": requires_renewable_energy,
        "policy_analysis": requires_policy_analysis
    }

    # 개선된 라우팅 우선순위 결정
    if not has_text:
        # 1순위: 명확한 재생에너지 키워드가 있고 전통 에너지 키워드가 없는 경우
        if requires_renewable_energy and not requires_energy_industry:
            state["route"] = "renewable"  # 재생에너지분석전문가
            
        # 2순위: 정책/정부/요약 관련 질문 (에너지산업분석전문가가 더 적합)
        elif requires_policy_analysis:
            state["route"] = "text"  # 에너지산업분석전문가
            
        # 3순위: 전통 에너지 키워드가 있는 경우
        elif requires_energy_industry:
            state["route"] = "text"  # 에너지산업분석전문가
            
        # 4순위: 혼합 또는 일반적인 질문 - 에너지산업분석전문가로 시작
        else:
            state["route"] = "text"  # 에너지산업분석전문가
    else:
        # 분석 완료
        state["route"] = "done"

    return state
