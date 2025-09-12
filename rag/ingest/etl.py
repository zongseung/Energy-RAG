import psycopg
import json
from core.settings import DB_URL
from nodes.router import embed
from ingest.pdf_to_chunks import extract_text_chunks, extract_tables_json

def ingest_pdf(filepath: str, category: str, domain: str):
    texts = extract_text_chunks(filepath)
    tables = extract_tables_json(filepath)

    try:
        with psycopg.connect(DB_URL, autocommit=True) as conn, conn.cursor() as cur:
            for ch in texts:
                emb = embed(ch["content"])
                cur.execute("""
                  INSERT INTO documents (chunk_type, content, category, page, filename, embedding)
                  VALUES ('text', %s, %s, %s, %s, %s)
                """, (ch["content"], category, ch["page"], ch["filename"], emb))

            for t in tables:
                emb = embed(t["markdown"])
                cur.execute("""
                  INSERT INTO documents (chunk_type, content, category, page, filename, embedding)
                  VALUES ('table', %s, %s, %s, %s, %s)
                """, (t["markdown"], category, t["page"], t["doc_id"], emb))
                cur.execute("""
                  INSERT INTO structured_tables (doc_id, page, caption, domain, table_json)
                  VALUES (%s, %s, %s, %s, %s)
                """, (t["doc_id"], t["page"], t.get("caption"), domain, json.dumps(t["table_json"])))
    except Exception as e:
        print(f"Error connecting to DB or executing query: {e}")
        raise
