# graph.py
from langgraph.graph import StateGraph, END
from state import QAState
from nodes.router import node_router
from nodes.retriever import node_retriever
from nodes.supervisor import node_supervisor
from nodes.text_agent import node_text_agent
from nodes.table_agent import node_table_agent
from nodes.reflection_agent import node_reflection_agent
from nodes.explainer import node_explainer

def _route(state: QAState):
    """에너지 산업 분석 라우팅 로직"""
    route = state.get("route", "text")
    return route

def build_graph():
    """에너지 산업 분석 전문 멀티에이전트 그래프 구성 (1회 성찰 포함)"""
    g = StateGraph(QAState)

    # 에너지 산업 분석 노드들 추가
    g.add_node("router", node_router)               # 쿼리 분석 및 연도 필터링
    g.add_node("retriever", node_retriever)         # 문서 검색
    g.add_node("supervisor", node_supervisor)       # 에너지 분석 라우팅
    g.add_node("energy_industry_agent", node_text_agent)       # 에너지산업분석전문가 (전통 에너지, 정책, 시장)
    g.add_node("renewable_energy_agent", node_table_agent)     # 재생에너지분석전문가 (신재생, ESG, 기술)
    g.add_node("reflection_agent", node_reflection_agent)  # 1회 성찰 및 개선
    g.add_node("explainer", node_explainer)         # 최종 결과 정리

    # 그래프 플로우 설정
    g.set_entry_point("router")
    g.add_edge("router", "retriever")
    g.add_edge("retriever", "supervisor")
    
    # supervisor의 라우팅 결정에 따른 조건부 엣지
    g.add_conditional_edges(
        "supervisor", 
        _route, 
        {
            "text": "energy_industry_agent",         # 에너지산업분석전문가 (전통 에너지, 정책, 시장)
            "renewable": "renewable_energy_agent",   # 재생에너지분석전문가 (신재생, ESG, 기술)
            "done": "explainer"           # 분석 완료 시 바로 설명자로
        }
    )
    
    # 전문가 에이전트들이 성찰 단계로 이동
    g.add_edge("energy_industry_agent", "reflection_agent")      # 에너지산업분석전문가 → 성찰
    g.add_edge("renewable_energy_agent", "reflection_agent")     # 재생에너지분석전문가 → 성찰
    
    # 성찰 에이전트는 1회만 수행 후 supervisor로
    g.add_edge("reflection_agent", "supervisor")
    
    g.add_edge("explainer", END)

    return g.compile()
