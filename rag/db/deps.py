
import psycopg
from core.settings import DB_URL

def vector_search(category: str, embedding: list[float], k: int = 8):
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT content, page, filename, chunk_type FROM documents WHERE category = %s 
                   ORDER BY embedding <=> %s::vector LIMIT %s""",
                (category, embedding, k),
            )
            results = cur.fetchall()
    return [{"content": r[0], "page": r[1], "filename": r[2], "chunk_type": r[3]} for r in results]

def load_structured_by_keys(keys: set[tuple[str, int]]):
    if not keys:
        return []
    
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            # Create a temporary table to hold the keys
            cur.execute("CREATE TEMPORARY TABLE temp_keys (doc_id TEXT, page INT)")
            
            # Insert the keys into the temporary table
            with cur.copy("COPY temp_keys (doc_id, page) FROM STDIN") as copy:
                for doc_id, page in keys:
                    copy.write_row((doc_id, page))
            
            # Select from structured_tables where (doc_id, page) is in temp_keys
            cur.execute(
                """SELECT st.doc_id, st.page, st.caption, st.domain, st.table_json
                   FROM structured_tables st
                   JOIN temp_keys tk ON st.doc_id = tk.doc_id AND st.page = tk.page"""
            )
            results = cur.fetchall()
            
            # Drop the temporary table
            cur.execute("DROP TABLE temp_keys")
            
    return [{"doc_id": r[0], "page": r[1], "caption": r[2], "domain": r[3], "table_json": r[4]} for r in results]
