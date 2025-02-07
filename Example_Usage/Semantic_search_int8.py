from usearch.index import Index
import pandas as pd
from sentence_transformers import SentenceTransformer, quantize_embeddings
import sqlite3
import numpy as np

# Load the integer 8-bit vector search index with 1024-dimensional vectors using inner product distance
indexint8 = Index(ndim=1024, metric="ip", dtype="i8")
indexint8.load(r"./NewsIndex_int8.usearch")

#Load calibration embeddings
calibration_embeddings=np.load(r"./calibration_embeddings.npy")

# Database connection setup
DB_PATH = r'./CommonCrawlNews.db'
conn = sqlite3.connect(DB_PATH)

# Load the SentenceTransformer model
model = SentenceTransformer("mixedbread-ai/deepset-mxbai-embed-de-large-v1")

# Encode the query text into an embedding with normalization for better retrieval
query_embedding = model.encode("query: Pizza", normalize_embeddings=True)

# Quantize query_embedding into int8 precision
query = quantize_embeddings(query_embedding, precision="int8",ranges=calibration_ranges)

# Perform a search in the int8 index, retrieving up to 10,000 matches
matches = indexint8.search(query, 10000)

# Extract distances (Inner Product distances from query vector) from the search results
distances = [match.distance for match in matches]

# Extract unique IDs from search results
ids_int8 = [str(match.key) for match in matches]

# Ensure we have search results before querying the database
if ids_int8:
    # Create a DataFrame to store search results
    search_results = pd.DataFrame({
        "distance": distances,
        "ids": ids_int8
    })
    
    # Prepare SQL query to retrieve article details based on retrieved IDs
    placeholders = ', '.join('?' for _ in ids_int8)  # Create placeholders for parameterized query
    query = f"""
    SELECT a.text, a.id, a.date_crawled, a.hostname, a.title
    FROM Articles a
    JOIN Article_Vectors av ON a.id = av.article_id
    WHERE av.hashed_id IN ({placeholders});
    """
    
    # Execute the query with parameterized placeholders to prevent SQL injection
    db_result = pd.read_sql_query(query, conn, params=ids_int8)
    
    # Add Inner Product distance values to the retrieved articles
    db_result["ip_dist"] = distances
else:
    db_result = pd.DataFrame()  # Return an empty DataFrame if no matches are found

# Close the database connection
conn.close()

# Display the results
print(db_result.head())  # Print a preview of results

