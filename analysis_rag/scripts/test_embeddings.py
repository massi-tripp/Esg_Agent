# FILE: scripts/test_azure_embedding.py
from openai import AzureOpenAI
import os
from typing import List

endpoint = os.getenv("ENDPOINT_URL", "https://openaimaurino2.openai.azure.com/")
subscription_key = os.getenv("AZURE_OPENAI_API_KEY", "wLPBFmPkwquNFwn5IKDR3W8mv1ZKb95FGnxLZ0RgUiEl32D9qFaGJQQJ99BHACI8hq2XJ3w3AAABACOGyD04")
api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")
embed_deployment = os.getenv("AZURE_DEPLOYMENT_EMBEDDINGS", "text-embedding-3-large")

class AzureEmbeddingFunction:
    def __init__(self, endpoint: str, api_key: str, api_version: str, model: str):
        self.client = AzureOpenAI(azure_endpoint=endpoint, api_key=api_key, api_version=api_version)
        self.model = model  # deve essere il *deployment name* esatto

    def embed(self, texts: List[str]) -> List[List[float]]:
        res = self.client.embeddings.create(model=self.model, input=texts)
        return [d.embedding for d in res.data]

def get_embedder():
    if endpoint and subscription_key and embed_deployment:
        return AzureEmbeddingFunction(endpoint, subscription_key, api_version, embed_deployment)

if __name__ == "__main__":
    print("[DEBUG] endpoint =", endpoint)
    print("[DEBUG] api_version =", api_version)
    print("[DEBUG] embedding_deployment =", embed_deployment)

    emb = get_embedder()
    if not emb:
        raise RuntimeError("Variabili mancanti per inizializzare l'embedder.")

    try:
        vecs = emb.embed(["hello world"])
        print("EMB OK, dim =", len(vecs[0]))
    except Exception as e:
        print("EMB ERROR:", type(e).__name__, "-", e)
        raise
