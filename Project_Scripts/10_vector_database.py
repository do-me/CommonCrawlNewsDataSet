from usearch.index import Index
import pandas as pd
import numpy as np
from glob import glob
from tqdm import tqdm
import hashlib

def hash_uuid(uuid_str: str) -> int:
    """Generate a hashed integer (63-bit) from UUID using SHA-256."""
    return int(hashlib.sha256(uuid_str.encode()).hexdigest(), 16) % (2**63 - 1)
  
data=pd.read_feather(PATH_TO_DATA)
data["hashed_id"]=data["id"].apply(hash_uuid)
index = Index(ndim=1024, metric="cos", dtype="f32")
embeddings=np.array(list(data["embeddings"]))
index.add(list(df["hashed_id"]), embeddings)
index.save(".\NewsIndex_f32.usearch")

index = Index(ndim=128, metric="Hamming", dtype="i8")
binary_embeddings=np.array(list(data["binary_embeddings"]))
# Add quantized embeddings and hashed IDs to the Usearch index
index.add(list(df["hashed_id"]), binary_embeddings)
index.save(".\NewsIndex_binary.usearch")


index = Index(ndim=1024, metric="ip", dtype="i8")
int8_embeddings=np.array(list(data["int8_embeddings"]))
# Add quantized embeddings and hashed IDs to the Usearch index
index.add(list(df["hashed_id"]), int8_embeddings)
index.save(".\NewsIndex_int8.usearch")
