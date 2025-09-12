import os
import psycopg
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import PGVector
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from core.settings import DB_URL, OPENAI_API_KEY, EMBED_MODEL

# NAS PostgreSQL 연결 설정
NAS_DB_URL = "postgresql://postgres@192.9.66.151:5432/naver"  # NAS 연결 URL

def get_nas_connection():
    """NAS PostgreSQL 연결 테스트 및 반환"""
    try:
        # 먼저 포트 5432 시도
        conn = psycopg.connect(NAS_DB_URL, connect_timeout=5)
        return conn
    except Exception as e:
        print(f"포트 5432 연결 실패: {e}")
        
        # 포트 5433 시도
        try:
            nas_db_url_5433 = "postgresql://postgres@192.9.66.151:5433/naver"
            conn = psycopg.connect(nas_db_url_5433, connect_timeout=5)
            return conn
        except Exception as e2:
            print(f"포트 5433 연결 실패: {e2}")
            
            # 포트 5434 시도
            try:
                nas_db_url_5434 = "postgresql://postgres@192.9.66.151:5434/naver"
                conn = psycopg.connect(nas_db_url_5434, connect_timeout=5)
                return conn
            except Exception as e3:
                print(f"모든 포트 연결 실패: {e3}")
                # 로컬 DB로 폴백
                print("로컬 DB로 폴백합니다.")
                return psycopg.connect(DB_URL)

def create_retriever():
    """NAS 데이터베이스에서 벡터 검색을 위한 retriever 생성"""
    try:
        # OpenAI 임베딩 모델 초기화
        embeddings = OpenAIEmbeddings(
            model=EMBED_MODEL,
            openai_api_key=OPENAI_API_KEY
        )
        
        # NAS PostgreSQL 연결
        conn = get_nas_connection()
        
        # PGVector를 사용한 벡터 스토어 생성
        vectorstore = PGVector(
            connection_string=NAS_DB_URL,
            embedding_function=embeddings,
            collection_name="documents"
        )
        
        # retriever 생성
        retriever = vectorstore.as_retriever(
            search_type="similarity",
            search_kwargs={"k": 5}
        )
        
        return retriever
        
    except Exception as e:
        print(f"Retriever 생성 실패: {e}")
        # 로컬 DB로 폴백
        try:
            embeddings = OpenAIEmbeddings(
                model=EMBED_MODEL,
                openai_api_key=OPENAI_API_KEY
            )
            
            vectorstore = PGVector(
                connection_string=DB_URL,
                embedding_function=embeddings,
                collection_name="documents"
            )
            
            retriever = vectorstore.as_retriever(
                search_type="similarity",
                search_kwargs={"k": 5}
            )
            
            return retriever
            
        except Exception as e2:
            print(f"로컬 DB 폴백도 실패: {e2}")
            return None

def format_docs(docs):
    """검색된 문서들을 포맷팅"""
    if not docs:
        return "관련 문서를 찾을 수 없습니다."
    
    formatted_docs = []
    for i, doc in enumerate(docs, 1):
        content = doc.page_content
        metadata = doc.metadata
        
        # 메타데이터에서 파일명과 페이지 정보 추출
        filename = metadata.get('filename', 'Unknown')
        page = metadata.get('page', 'Unknown')
        
        formatted_doc = f"**문서 {i}:** {filename} (페이지 {page})\n{content}\n"
        formatted_docs.append(formatted_doc)
    
    return "\n".join(formatted_docs)

def create_chain():
    """RAG 체인 생성"""
    try:
        # OpenAI 채팅 모델 초기화
        llm = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key=OPENAI_API_KEY,
            temperature=0.1
        )
        
        # retriever 생성
        retriever = create_retriever()
        
        if not retriever:
            raise Exception("Retriever 생성 실패")
        
        # 프롬프트 템플릿
        prompt_template = ChatPromptTemplate.from_template("""
당신은 에너지 관련 질문에 답변하는 AI 어시스턴트입니다.

다음 문서들을 참고하여 질문에 답변해주세요:

{context}

질문: {question}

답변할 때 다음 사항을 고려해주세요:
1. 제공된 문서의 내용을 바탕으로 정확한 답변을 제공하세요
2. 문서에 없는 내용은 추측하지 마세요
3. 답변의 근거가 되는 문서 정보를 포함해주세요
4. 한국어로 답변해주세요

답변:
""")
        
        # 체인 구성
        chain = (
            {"context": retriever | format_docs, "question": RunnablePassthrough()}
            | prompt_template
            | llm
            | StrOutputParser()
        )
        
        return chain
        
    except Exception as e:
        print(f"체인 생성 실패: {e}")
        # 간단한 폴백 체인
        def fallback_chain(question):
            return f"죄송합니다. 현재 데이터베이스 연결에 문제가 있어 답변을 생성할 수 없습니다. 오류: {str(e)}"
        
        return fallback_chain

# 전역 변수로 retriever 생성
retriever = create_retriever()

