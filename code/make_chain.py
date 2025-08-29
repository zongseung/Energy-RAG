from dotenv import load_dotenv
import streamlit as st
import os
from langchain_core.messages.chat import ChatMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import OpenAIEmbeddings
from langchain_community.chat_models import ChatOllama # 모델은 거의 다 여기에 있음.
from langchain.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.schema.runnable import RunnableParallel, RunnablePassthrough

# uv run streamlit run make_chain.py -> streamlit 안에 make_chain.py를 씌워야 한다.

def add_message(role, message):
    st.session_state["messages"].append(ChatMessage(role=role, content=message))


# loader1 은 먼저 FAISS를 기반으로 하나 만들고 from_document를 생성하고, 이후 add_document를 생성한다.  
pdf = PyMuPDFLoader("/home/user/langchain-app/naverDB/(2025-08-06) 전기화 시대의 아이콘, 실리콘(Silicone) - 하나증권.pdf")
pdf2 = PyMuPDFLoader("/home/user/langchain-app/naverDB/(2025-07-21) 에폭시 체인 개선 가능성 점검 - 하나증권.pdf")


## 
def preprocessing_pdf(*pdf_paths):
    splitter = RecursiveCharacterTextSplitter(chunk_size=600, chunk_overlap=20)

    # 첫 번째 파일 처리
    loader = PyMuPDFLoader(pdf_paths[0])
    docs = loader.load_and_split(splitter)

    # FAISS DB 생성
    faiss_db = FAISS.from_documents(docs, OpenAIEmbeddings(api_key=""))

    # 나머지 파일들 처리
    for path in pdf_paths[1:]:
        loader_tmp = PyMuPDFLoader(path)
        docs_tmp = loader_tmp.load_and_split(splitter)
        faiss_db.add_documents(docs_tmp)

    return faiss_db.as_retriever(search_kwargs={"k": 3})

## 
faiss_db = preprocessing_pdf("/home/user/langchain-app/naverDB/(2025-08-06) 전기화 시대의 아이콘, 실리콘(Silicone) - 하나증권.pdf",
                                "/home/user/langchain-app/naverDB/(2025-07-21) 에폭시 체인 개선 가능성 점검 - 하나증권.pdf",
                                "/home/user/langchain-app/naverDB/이슈분석_중동 석유 개발의 큰 손으로 부상하고 있는 중국.pdf")

def create_chain():
    prompt = ChatPromptTemplate.from_messages([
        ("system", 
            """당신은 전문적인 AI 요약 비서입니다. 다음 규칙을 반드시 지켜주세요:
            1. 주어진 문서({context})만을 기반으로 답변하세요.
            2. 문서에 없는 내용은 추측하지 말고 '관련 정보가 없습니다'라고 말하세요.
            3. 답변은 구체적이고 한국어로 요약하세요.
            4. 사용자가 원한다면 bullet-point 형식으로 정리할 수 있습니다.
            5. 존대말로 답변해주세요
        """),
        ("user", """# 질문 {question}

        # 참고 문서 {context}
            ---
            위 문서를 기반으로 질문에 대한 요약 답변을 작성하세요.""")
        ])

    llm = ChatOllama(model="gemma3:1b", base_url="http://localhost:11434")

    chain = (
    {
        "context": RunnablePassthrough() | faiss_db | (lambda docs: "\n\n".join([d.page_content for d in docs])),
        "question": RunnablePassthrough(),
    }
    | prompt
    | llm
    | StrOutputParser()
)
    return chain
