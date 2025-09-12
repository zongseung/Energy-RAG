
import os
import sys
from tqdm import tqdm

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingest.etl import ingest_pdf
from nodes.router import pick_category

NAVER_DB_DIR = "/app/naverDB"

def bulk_ingest_main():
    pdf_files = [f for f in os.listdir(NAVER_DB_DIR) if f.endswith(".pdf")]
    
    # 100개만 처리
    print(f"Processing {len(pdf_files)} PDF files...")
    
    for filename in tqdm(pdf_files, desc="Ingesting PDFs"):
        filepath = os.path.join(NAVER_DB_DIR, filename)
        category = pick_category(filename)
        try:
            ingest_pdf(filepath, category=category, domain="finance")
            print(f"Successfully ingested: {filename}")
        except Exception as e:
            print(f"Error ingesting {filename}: {e}")

if __name__ == "__main__":
    bulk_ingest_main()
