import streamlit as st
from langchain_core.messages.chat import ChatMessage
from make_chain import create_chain

st.title("Energy-gpt에 오신걸 환영합니다.") # Title 지정하기

# 대화 저장소 초기화
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# 사이드바
with st.sidebar:
    clear_btn = st.button("대화초기화")

if clear_btn:
    st.session_state["messages"] = []

# 메시지 추가 함수
def add_message(role, message):
    st.session_state["messages"].append(ChatMessage(role=role, content=message))

# 메시지 출력 함수
def print_messages():
    for chat_message in st.session_state["messages"]:
        st.chat_message(chat_message.role).write(chat_message.content)

# 기존 대화 출력
print_messages()

###############################################################################################
# 유저 입력 받기
###############################################################################################
user_input = st.chat_input("궁금한 내용을 물어보세요!")

if user_input:
    # 사용자 메시지 출력
    st.chat_message("user").write(user_input)

    # 체인 실행
    chain = create_chain()

    with st.chat_message("assistant"):
        container = st.empty()
        ai_answer = ""
        for chunk in chain.stream(user_input):   
            ai_answer += chunk
            container.markdown(ai_answer)

    # 대화 기록 저장
    add_message("user", user_input)
    add_message("assistant", ai_answer)
