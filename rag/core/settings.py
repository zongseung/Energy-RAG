import os
from dotenv import load_dotenv

load_dotenv()

IS_DOCKERIZED = os.getenv("IS_DOCKERIZED", "false").lower() == "true"

if IS_DOCKERIZED:
    DB_URL = os.getenv("DB_URL", "postgresql://zongseung:1234@postgres_db1:5432/naver")
else:
    # NAS PostgreSQL 연결 우선 시도
    NAS_DB_URL = os.getenv("NAS_DB_URL", "postgresql://postgres@192.9.66.151:5432/naver")
    LOCAL_DB_URL = os.getenv("DB_URL", "host=localhost port=5432 dbname=naver user=zongseung password=1234")
    
    try:
        import psycopg
        conn = psycopg.connect(NAS_DB_URL, connect_timeout=3)
        conn.close()
        DB_URL = NAS_DB_URL
        print("✅ NAS PostgreSQL 연결 성공")
    except:
        DB_URL = LOCAL_DB_URL
        print("⚠️ NAS 연결 실패, 로컬 DB 사용")

# Provider: openai | ollama
PROVIDER = os.getenv("PROVIDER", "openai").lower()

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # ✅ .env 에서 불러오기
if PROVIDER == "openai" and not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY 가 설정되지 않았습니다. .env 파일을 확인하세요.")

EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")   # 1536-d
CHAT_MODEL  = os.getenv("CHAT_MODEL", "gpt-4o-mini")

# Ollama
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")  # 768-d
OLLAMA_CHAT_MODEL  = os.getenv("OLLAMA_CHAT_MODEL", "llama3.1:8b")

# Embedding dimension must match DB schema
EMBED_DIM = int(os.getenv("EMBED_DIM", "1536"))
