import time
import psycopg

# Docker Compose 서비스 이름(postgres_db1)을 DB 호스트로 사용
CONN_STR = "postgresql://zongseung:1234@postgres_db1:5432/naver"

time.sleep(5)

with psycopg.connect(CONN_STR, autocommit=True) as conn:
    # 확장 설치 (pgvector)
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    print("Connected and pgvector extension created")
    
    # 메모리 및 병렬작업 설정 (세션 기준)
    conn.execute("SET maintenance_work_mem = '2GB'") #
    conn.execute("SET max_parallel_maintenance_workers = 4")

    # documents 테이블 생성 (파티션 기준 테이블) - 이미 존재하면 건너뛰기
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id bigserial,
            content text,
            category text NOT NULL,
            page int,
            filename text,
            embedding vector(1536),
            PRIMARY KEY (id, category)
        ) PARTITION BY LIST (category);
    """)

    # 파티션 생성 - 이미 존재하면 건너뛰기
    conn.execute("""CREATE TABLE IF NOT EXISTS NAVER PARTITION OF documents FOR VALUES IN ('NAVER');""")

    # 각 파티션별 인덱스 생성 (HNSW) - 이미 존재하면 건너뛰기
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_NAVER_embedding
        ON NAVER USING hnsw (embedding vector_l2_ops)
        WITH (m = 16, ef_construction = 64);
    """)

    # 인덱싱 진행 상황 확인
    cur = conn.execute("""
        SELECT phase,
               round(100.0 * blocks_done / nullif(blocks_total, 0), 1) AS progress
        FROM pg_stat_progress_create_index;
    """)
    print(cur.fetchall())

    # 통계 갱신
    conn.execute("ANALYZE documents;")