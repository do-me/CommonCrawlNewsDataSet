from usearch.index import Index
import pandas as pd
from sentence_transformers import SentenceTransformer
import sqlite3

# Load the vector search index with 1024-dimensional vectors using cosine similarity and 32-bit float precision
index = Index(ndim=1024, metric="cos", dtype="f32")
index.load(r"./NewsIndex_f32.usearch")

# Database connection setup
DB_PATH = r'./CommonCrawlNews.db'
conn = sqlite3.connect(DB_PATH)

# Load the SentenceTransformer model
model = SentenceTransformer("mixedbread-ai/deepset-mxbai-embed-de-large-v1")

# Encode the query text with "query: " prompt into an embedding with normalization for better retrieval
query_embedding = model.encode("query: Pizza", normalize_embeddings=True)

# Perform a search in the index, retrieving up to 10,000 matches
matches = index.search(query_embedding, 10000)

# Extract distances (cosine distances from query vector) from the search results
distances = [match.distance for match in matches]

# Extract unique IDs from search results
ids_f32 = [str(match.key) for match in matches]

# Ensure we have search results before querying the database
if ids_f32:
    # Create a DataFrame to store search results
    search_results = pd.DataFrame({
        "distance": distances,
        "ids": ids_f32
    })
    
    # Prepare SQL query to retrieve article details based on retrieved IDs
    placeholders = ', '.join('?' for _ in ids_f32)  # Create placeholders for parameterized query
    query = f"""
    SELECT a.text, a.id, a.date_crawled, a.hostname, a.title
    FROM Articles a
    JOIN Article_Vectors av ON a.id = av.article_id
    WHERE av.hashed_id IN ({placeholders});
    """
    
    # Execute the query with parameterized placeholders to prevent SQL injection
    db_result = pd.read_sql_query(query, conn, params=ids_f32)
    
    # Add cosine distance values to the retrieved articles
    db_result["cos_dist"] = distances
else:
    db_result = pd.DataFrame()  # Return an empty DataFrame if no matches are found

# Close the database connection
conn.close()

# Display the results
print(db_result.head())  # Print a preview of results
