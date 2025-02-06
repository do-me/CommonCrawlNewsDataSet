# CommonCrawl News Dataset

This repository contains scripts and utilities for downloading, extracting, parsing, filtering, geocoding, storing, and querying the CommonCrawl News dataset. The goal is to enable spatial and semantic analysis of news articles with a focus on reproducibility and ease of use.

## Repository Structure

### Project Scripts
**01_download_newscrawl.py**: 
   - Downloads CommonCrawl News WARC files for a specified month.
   - Handles concurrent downloads with retries and exponential backoff.
   - Ensures folder structure is created and manages file paths dynamically.

**02_extract_newscrawl.py**:
   - Extracts text content and metadata from WARC files.
   - Converts the extracted data into Feather format for efficient processing.
   - Supports parallel processing to handle large datasets.

**03_extract_text.py**:
   - Extracts main text and metadata from articles using `trafilatura`.
   - Filters articles based on predefined exclusion rules (e.g., TLD).
   - Outputs processed data in Feather format.

**04_compute_quality_metrics.py**:
   - Computes quality metrics for articles, such as sentence count, word length, and non-alphanumeric word ratio.
   - Filters low-quality articles based on these metrics.
   - Outputs the processed data in Feather format.

**05_filter_news.py**:
   - Applies additional filtering criteria to ensure article quality.
   - Filters articles based on metrics like word count, mean word length, and ellipsis usage.
   - Saves filtered data to a new directory.

**06_named_entity_recognition.py**:
   - Performs named entity recognition (NER) to extract geographic entities from article text.
   - Utilizes a custom spaCy model for NER: https://huggingface.co/LKriesch/LLAMA_fast_geotag
   - Outputs data enriched with geographic entity information.

**07_geocode_news.py**:
   - Extracts and cleans location data, geocode entities, and map them to administrative boundaries.
   - Outputs a geocoded dataset for spatial analysis.
     
**08_sqlite_setup.py**:
   - Stores article metadata and geolocation data into an SQLite database.
   - Creates and manages tables for articles, locations, and their relationships.
   - Supports incremental updates to the database with new data.
     
**09_embedding_transformation.py**:
   - Transforms article texts into sentence embeddings for semantic retrieval and clustering.
   - Quantization of embeddings for reduced storage requirements and faster retrieval.
   
**10_vectordatabase.py**:
   - Builds a Usearch vector database for semantic search on article embeddings and maintains a mapping of custom IDs.
   - Supports efficient similarity-based querying of article content.


### Example Usage Scripts
- **spatial_analysis.py**: Demonstrates how to query the database and perform a basic spatial analysis.
- **semantic_search.py**: Provides sample code for using the vectordatabase and retrieve article information from SQLite.

## Installation

### Prerequisites
- Python 3.8+
- Required libraries listed in `requirements.txt` (e.g., pandas, sqlite3, SentenceTransformers, FAISS, tqdm).

### Setup
1. Clone this repository:
   ```bash
   git clone https://github.com/<your-username>/commoncrawl-news-analysis.git
   cd commoncrawl-news-analysis
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the SQLite database:
   ```bash
   python storage_script.py
   ```

## Usage

### Running Scripts
Scripts should be executed in sequence to build the complete dataset. For example:
```bash
python 01_download_newscrawl.py
python 02_extract_newscrawl.py
python 03_extract_text.py
python 04_compute_quality_metrics.py
python 05_filter_news.py
python 06_named_entity_recognition.py
python 07_geocode_news.py
python 08_Vectordatabase.py
```

### Querying the Database
Run example scripts to explore and analyze the data:
```bash
python example_analysis.py
```

## Features
- Automated processing workflow for CommonCrawl News data.
- Named entity recognition and geocoding for geographic analysis.
- Flexible querying and spatial analysis using SQLite and FAISS.
- Reproducible pipeline for research and visualization.

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## License
This project is licensed under the MIT License.

## Acknowledgements
- CommonCrawl for providing open-access web data.
- SentenceTransformers, spaCy, and FAISS for NLP and vector search tools.
- Geopy and Google Maps API for geocoding support.
