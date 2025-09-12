from typing import List, Dict, Any
import os
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pymupdf4llm
import camelot
import json

def _extract_raw_text(filepath: str) -> str:
    """pymupdf4llm을 사용하여 PDF에서 텍스트 추출"""
    try:
        # pymupdf4llm으로 PDF 텍스트 추출
        md_text = pymupdf4llm.to_markdown(filepath)
        return md_text
    except Exception as e:
        print(f"Error extracting text from {filepath}: {e}")
        return ""

def extract_text_chunks(filepath: str) -> List[Dict[str, Any]]:
    """PDF에서 텍스트를 추출하고 청크로 분할"""
    raw = _extract_raw_text(filepath)
    if not raw:
        return []
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=800, chunk_overlap=120, separators=["\n\n", "\n", " ", ""]
    )
    chunks = splitter.split_text(raw)
    
    # 파일명에서 페이지 정보 추출 시도
    filename = os.path.basename(filepath)
    chunks_with_metadata = []
    
    for i, chunk in enumerate(chunks):
        chunks_with_metadata.append({
            "content": chunk,
            "page": i + 1,  # 페이지 번호는 청크 순서로 설정
            "filename": filename,
            "chunk_type": "text"
        })
    
    return chunks_with_metadata

def extract_tables_json(filepath: str) -> List[Dict[str, Any]]:
    """PDF에서 테이블을 추출하여 JSON 형태로 변환"""
    tables = []
    filename = os.path.basename(filepath)
    
    try:
        # camelot으로 테이블 추출
        pdf_tables = camelot.read_pdf(filepath, pages='all', flavor='lattice')
        
        for i, table in enumerate(pdf_tables):
            if table.df.empty:
                continue
                
            # DataFrame을 JSON 형태로 변환
            table_data = table.df.fillna('').to_dict('records')
            columns = list(table.df.columns)
            rows = [list(row.values()) for row in table_data]
            
            # Markdown 형태로도 변환
            markdown_table = table.df.to_markdown(index=False)
            
            tables.append({
                "doc_id": filename,
                "page": i + 1,
                "caption": f"Table {i + 1}",
                "domain": "energy",  # 기본 도메인
                "markdown": markdown_table,
                "table_json": {
                    "columns": columns,
                    "rows": rows
                }
            })
            
    except Exception as e:
        print(f"Error extracting tables from {filepath}: {e}")
        # 에러 발생 시 빈 리스트 반환
        return []
    
    return tables