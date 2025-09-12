from typing import Dict
from db.deps import vector_search

def node_retriever(state: Dict):
    c = state["category"]
    emb = state["query_embedding"]
    state["candidates"] = vector_search(c, emb, k=8)
    return state
