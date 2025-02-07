from usearch.index import Index
import pandas as pd
from sentence_transformers import SentenceTransformer, quantize_embeddings
import sqlite3

# Load the binary vector search index with 128-dimensional vectors using Hamming distance and 8-bit integer precision
indexBinary = Index(ndim=128, metric="hamming", dtype="i8")
indexBinary.load(r"./NewsIndex_binary.usearch")

# Database connection setup
DB_PATH = r'./CommonCrawlNews.db'
conn = sqlite3.connect(DB_PATH)

# Load the SentenceTransformer model
model = SentenceTransformer("mixedbread-ai/deepset-mxbai-embed-de-large-v1")

# Encode the query text into an embedding with normalization for better retrieval
query_embedding = model.encode("query: Pizza", normalize_embeddings=True)

#Quantize query_embedding into binary precision
query_embedding=quantize_embeddings(query_embedding, precision="binary")

# Perform a search in the binary index, retrieving up to 10,000 matches
matches = indexBinary.search(query_embedding, 10000)

# Extract distances (Hamming distances from query vector) from the search results
distances = [match.distance for match in matches]

# Extract unique IDs from search results
ids_binary = [str(match.key) for match in matches]

# Ensure we have search results before querying the database
if ids_binary:
    # Create a DataFrame to store search results
    search_results = pd.DataFrame({
        "distance": distances,
        "ids": ids_f32
    })
    
    # Prepare SQL query to retrieve article details based on retrieved IDs
    placeholders = ', '.join('?' for _ in ids_binary)  # Create placeholders for parameterized query
    query = f"""
    SELECT a.text, a.id, a.date_crawled, a.hostname, a.title
    FROM Articles a
    JOIN Article_Vectors av ON a.id = av.article_id
    WHERE av.hashed_id IN ({placeholders});
    """
    
    # Execute the query with parameterized placeholders to prevent SQL injection
    db_result = pd.read_sql_query(query, conn, params=ids_binary)
    
    # Add Hamming distance values to the retrieved articles
    db_result["hamming_dist"] = distances
else:
    db_result = pd.DataFrame()  # Return an empty DataFrame if no matches are found

# Close the database connection
conn.close()

# Display the results
print(db_result.head())  # Print a preview of results

