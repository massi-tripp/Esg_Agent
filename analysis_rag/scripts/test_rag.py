from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
import os

CHROMA_DIR = "analysis_rag/data/benchmark/chroma"
COLLECTION_NAME = "ESG_RAG"

emb = AzureOpenAIEmbeddings(
    azure_endpoint=os.getenv("ENDPOINT_URL"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_deployment="text-embedding-3-large",
)

llm = AzureChatOpenAI(
    azure_endpoint=os.getenv("ENDPOINT_URL"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_deployment="gpt-5-mini",
    api_version="2025-01-01-preview",
    temperature=1.0,
)

vs = Chroma(collection_name=COLLECTION_NAME, embedding_function=emb, persist_directory=CHROMA_DIR)

# Domanda test su una sola azienda
docs = vs.similarity_search("Quali attività ambientali fa ASSICURAZIONI GENERALI SPA?", k=6, filter={"company_id": "ASSICURAZIONI GENERALI SPA"})
context = "\n---\n".join([d.page_content for d in docs])

res = llm.invoke([
    {"role": "system", "content": "Sei un analista ESG. Rispondi brevemente con solo ciò che trovi nel contesto."},
    {"role": "user", "content": f"Context:\n{context}\n\nDomanda: Quali attività ambientali svolge ASSICURAZIONI GENERALI SPA?"}
])
print(res.content)
