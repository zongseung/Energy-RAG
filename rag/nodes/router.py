from typing import Dict
from core.settings import PROVIDER, EMBED_MODEL, OPENAI_API_KEY, OLLAMA_HOST, OLLAMA_EMBED_MODEL
import requests

# OpenAI client (lazy import)
_openai_client = None
def _get_openai():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client

OIL_HINTS = ["oil","petro","정유","opec","barrel"]
PV_HINTS  = ["pv","solar","태양광","module","inverter"]

def pick_category(q: str) -> str:
    return "NAVER"

def embed(text: str):
    # Always use OpenAI for embeddings
    client = _get_openai()
    resp = client.embeddings.create(model=EMBED_MODEL, input=text)
    return resp.data[0].embedding

def node_router(state: Dict):
    q = state["query"]
    state["query_embedding"] = embed(q)
    state["category"] = pick_category(q)
    return state
