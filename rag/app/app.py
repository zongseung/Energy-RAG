import re
import os
import sys
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
from langchain_core.messages.chat import ChatMessage
from core.graph import build_graph

st.set_page_config(page_title="Energy-gpt", layout="wide")
st.title("Energy-gpt에 오신걸 환영합니다.")

# 대화 저장소 초기화
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# 사이드바
with st.sidebar:
    if st.button("대화초기화"):
        st.session_state["messages"] = []

# 메시지 출력
def print_messages():
    for chat_message in st.session_state["messages"]:
        st.chat_message(chat_message.role).write(chat_message.content)

print_messages()

# 사용자 입력
user_input = st.chat_input("궁금한 내용을 물어보세요!")

if user_input:
    st.chat_message("user").write(user_input)

    # LangGraph 실행
    graph = build_graph()
    ai_answer = ""

    with st.chat_message("assistant"):
        container = st.empty()
        thinking_container = st.empty()
        
        # 최근 3턴(history) 구성: user-assistant 페어 기준으로 최대 3개 묶음
        msgs = st.session_state["messages"]
        pairs = []
        i = len(msgs) - 1
        # messages는 [user, assistant, user, assistant, ...] 형태로 저장됨
        while i >= 1 and len(pairs) < 3:
            if msgs[i-1].role == "user" and msgs[i].role == "assistant":
                pairs.append({
                    "user": msgs[i-1].content,
                    "assistant": msgs[i].content,
                })
                i -= 2
            else:
                i -= 1

        history = []
        # 최근 것이 먼저였으므로 역순으로 정렬하여 오래된 것부터 전달
        for p in reversed(pairs):
            history.append({"role": "user", "content": p["user"]})
            history.append({"role": "assistant", "content": p["assistant"]})

        # LangGraph 스트림 실행
        final_output = {}
        node_messages = {
            "router": "🔍 질문을 분석하고 있습니다...",
            "retriever": "📚 관련 문서를 검색하고 있습니다...",
            "supervisor": "🤔 전문가를 선택하고 있습니다...",
            "energy_industry_agent": "⚡ 에너지산업분석전문가가 분석하고 있습니다...",
            "renewable_energy_agent": "🌱 재생에너지분석전문가가 분석하고 있습니다...",
            "reflection_agent": "🧠 답변을 성찰하고 개선하고 있습니다...",
            "explainer": "🎯 최종 답변을 정리하고 있습니다...",
        }
        
        try:
            for chunk in graph.stream({"query": user_input, "history": history}):
                for node_name, node_output in chunk.items():
                    if node_name in node_messages:
                        thinking_container.markdown(
                            f"<small style='color: gray;'>{node_messages[node_name]}</small>", 
                            unsafe_allow_html=True
                        )
                    final_output.update(node_output)
        except Exception as e:
            thinking_container.markdown(
                f"<small style='color: red;'>⚠️ 처리 중 오류가 발생했습니다: {str(e)}</small>", 
                unsafe_allow_html=True
            )
            final_output = {"final": "죄송합니다. 답변 생성 중 오류가 발생했습니다."}
        
        # 타자기 효과로 최종 답변 표시
        ai_answer = final_output.get("final", "답변을 생성할 수 없습니다.")
        thinking_container.markdown(
            f"<small style='color: gray;'>💬 답변을 출력하고 있습니다...</small>", 
            unsafe_allow_html=True
        )
        
        # 타자기 효과
        displayed_text = ""
        for i, char in enumerate(ai_answer):
            displayed_text += char
            container.markdown(displayed_text + "▊", unsafe_allow_html=True)
            if i % 3 == 0:  # 3글자마다 업데이트
                time.sleep(0.01)
        
        thinking_container.empty()  # 생각 중 메시지 제거
        container.markdown(ai_answer, unsafe_allow_html=True)  # 최종 커서 제거
        
        # 최종 output을 out 변수에 할당 (기존 코드와 호환성 유지)
        out = final_output

        # 문서 출처 표시 (이미지 경로 추출)
        candidates = out.get("candidates", [])
        if candidates:
            st.write("---")
            st.subheader("참고 문서")
            for i, candidate in enumerate(candidates[:8], 1):
                filename = candidate.get('filename', 'N/A')
                page = candidate.get('page', 'N/A')
                chunk_type = candidate.get('chunk_type', 'N/A')
                content = candidate.get('content', '')
                
                st.write(f"**{i}.** {filename} (페이지 {page}, {chunk_type})")
                
                # 이미지 경로 추출 및 표시
                image_paths = re.findall(r'!\[.*?\]\((.*?)\)', content)
                for img_path in image_paths:
                    if img_path.startswith("/static/"):
                        st.markdown(f"![그림]({img_path})", unsafe_allow_html=True)
                    elif os.path.exists(img_path):
                        st.image(img_path, caption=os.path.basename(img_path))

        # 테이블 결과 표시
        if out.get("result", {}).get("type") == "table":
            import pandas as pd
            df = pd.DataFrame(out["result"].get("preview", []))
            st.dataframe(df, use_container_width=True)

    # 대화 기록 저장
    st.session_state["messages"].append(ChatMessage(role="user", content=user_input))
    st.session_state["messages"].append(ChatMessage(role="assistant", content=ai_answer))