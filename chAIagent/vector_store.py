import faiss
import os
import pickle
from sentence_transformers import SentenceTransformer

INDEX_FILE = "vector.index"
DATA_FILE = "vector_data.pkl"
model = SentenceTransformer("all-MiniLM-L6-v2")

# Create or load index
def load_index():
    if os.path.exists(INDEX_FILE) and os.path.exists(DATA_FILE):
        with open(DATA_FILE, "rb") as f:
            data = pickle.load(f)
        index = faiss.read_index(INDEX_FILE)
        return index, data
    else:
        dim = 384  # Embedding size for MiniLM
        index = faiss.IndexFlatL2(dim)
        return index, []

def save_index(index, data):
    faiss.write_index(index, INDEX_FILE)
    with open(DATA_FILE, "wb") as f:
        pickle.dump(data, f)

# Add a question to the store
def add_question(question: str, sql: str):
    embedding = model.encode([question])
    index, data = load_index()
    index.add(embedding)
    data.append((question, sql))
    save_index(index, data)

# Find top-k similar questions
def find_similar(question: str, top_k=3):
    embedding = model.encode([question])
    index, data = load_index()
    if index.ntotal == 0:
        return []
    D, I = index.search(embedding, top_k)
    return [data[i] for i in I[0]]
