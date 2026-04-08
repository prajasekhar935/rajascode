# Databricks notebook source
# DBTITLE 1,Technical Documentation - Supplier Parent Company Matching System
# MAGIC %md
# MAGIC # Supplier Parent Company Matching System - Technical Documentation
# MAGIC
# MAGIC ## Overview
# MAGIC This POC implements an automated supplier-to-parent-company matching system using a **hybrid AI approach** combining semantic similarity and generative AI.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture & Workflow
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐
# MAGIC │ Stage 1: Input Data │ → Supplier records from various sources
# MAGIC └──────────┬──────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────────┐
# MAGIC │ Stage 2: Golden Source  │ → Reference data with known parent companies
# MAGIC └──────────┬──────────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────────┐
# MAGIC │ Stage 3: Translation    │ → Translate to English using Google Translate
# MAGIC │  (langdetect + googletrans) │
# MAGIC └──────────┬──────────────┘
# MAGIC            │
# MAGIC ┌──────────▼──────────────────────┐
# MAGIC │ Stage 4: Semantic Matching      │ → SBERT (Sentence-BERT) embeddings
# MAGIC │  Model: all-MiniLM-L6-v2        │ → Cosine similarity matching
# MAGIC │  Threshold: 0.85                │
# MAGIC └──────────┬──────────────────────┘
# MAGIC            │
# MAGIC       ┌────▼────┐
# MAGIC       │ Matched? │
# MAGIC       └─┬────┬──┘
# MAGIC         │    │
# MAGIC      YES│    │NO
# MAGIC         │    │
# MAGIC         │    └─────────────────────────┐
# MAGIC         │                              │
# MAGIC         │                     ┌────────▼─────────────────┐
# MAGIC         │                     │ Stage 5: GPT-3.5 Search  │
# MAGIC         │                     │  via OpenRouter API      │
# MAGIC         │                     │  Top 5 candidates        │
# MAGIC         │                     └────────┬─────────────────┘
# MAGIC         │                              │
# MAGIC         └──────────┬───────────────────┘
# MAGIC                    │
# MAGIC          ┌─────────▼──────────┐
# MAGIC          │  Final Output      │
# MAGIC          │  (Denormalized)    │
# MAGIC          └────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Technical Components
# MAGIC
# MAGIC ### 1. **Data Sources**
# MAGIC
# MAGIC **Input Table:** `tamr_poc_ref.data_source.supplier_input_to_tamr`
# MAGIC - Contains supplier records without parent company assignments
# MAGIC - Fields: corporate_name, trade_name, legal_form, city, country, etc.
# MAGIC
# MAGIC **Golden Source Table:** `tamr_poc_ref.data_source.supplier_golden_source`
# MAGIC - Reference dataset with known parent companies
# MAGIC - Used as ground truth for semantic matching
# MAGIC
# MAGIC ```sql
# MAGIC -- Example structure
# MAGIC CREATE TABLE supplier_input_to_tamr (
# MAGIC     corporate_name VARCHAR(255),
# MAGIC     trade_name VARCHAR(255),
# MAGIC     name_of_the_city VARCHAR(255),
# MAGIC     country VARCHAR(255),
# MAGIC     ...
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ML & AI Models Used
# MAGIC
# MAGIC ### **Model 1: Language Detection (langdetect)**
# MAGIC - **Purpose:** Detect source language before translation
# MAGIC - **Method:** Statistical analysis using character n-grams
# MAGIC - **Output:** ISO 639-1 language code (e.g., 'en', 'fr', 'de')
# MAGIC
# MAGIC ```python
# MAGIC from langdetect import detect
# MAGIC
# MAGIC lang = detect("Société Générale")  # Returns: 'fr'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Model 2: Google Translate API (googletrans)**
# MAGIC - **Purpose:** Translate non-English supplier names to English
# MAGIC - **API:** Unofficial Google Translate API wrapper
# MAGIC - **Batch Processing:** Processed via Spark `mapInPandas` for scalability
# MAGIC
# MAGIC ```python
# MAGIC from googletrans import Translator
# MAGIC
# MAGIC translator = Translator()
# MAGIC result = translator.translate("Société Générale", src='fr', dest='en')
# MAGIC print(result.text)  # Output: "General Society"
# MAGIC ```
# MAGIC
# MAGIC **Why Translation?**
# MAGIC - Standardizes names across languages
# MAGIC - Improves embedding quality for non-English text
# MAGIC - Reduces false negatives in matching
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Model 3: Sentence-BERT (all-MiniLM-L6-v2)**
# MAGIC
# MAGIC **Type:** Semantic Similarity Model (Transformer-based)
# MAGIC
# MAGIC **Architecture:**
# MAGIC - Base: MiniLM (distilled BERT variant)
# MAGIC - Training: Siamese/Triplet networks for sentence embeddings
# MAGIC - Output: 384-dimensional dense vectors
# MAGIC
# MAGIC **Technical Specifications:**
# MAGIC - Parameters: \~23 million
# MAGIC - Speed: \~2,800 sentences/second (CPU)
# MAGIC - Use Case: Semantic search, duplicate detection, clustering
# MAGIC
# MAGIC **How It Works:**
# MAGIC ```python
# MAGIC from sentence_transformers import SentenceTransformer
# MAGIC import numpy as np
# MAGIC
# MAGIC model = SentenceTransformer('all-MiniLM-L6-v2')
# MAGIC
# MAGIC # Generate embeddings
# MAGIC name1 = "BCD TRAVEL HONG KONG LIMITED"
# MAGIC name2 = "BCD TRAVEL ASIA PACIFIC PTE LTD"
# MAGIC
# MAGIC emb1 = model.encode([name1])[0]  # Shape: (384,)
# MAGIC emb2 = model.encode([name2])[0]
# MAGIC
# MAGIC # Cosine similarity
# MAGIC similarity = np.dot(emb1, emb2) / (np.linalg.norm(emb1) * np.linalg.norm(emb2))
# MAGIC print(f"Similarity: {similarity:.3f}")  # Output: 0.923
# MAGIC ```
# MAGIC
# MAGIC **Matching Logic:**
# MAGIC ```python
# MAGIC threshold = 0.85
# MAGIC if similarity >= threshold:
# MAGIC     match_status = "matched"
# MAGIC     parent_company = golden_source_parent[best_match_idx]
# MAGIC else:
# MAGIC     match_status = "not_matched"
# MAGIC     # Send to GPT for fallback
# MAGIC ```
# MAGIC
# MAGIC **Why SBERT?**
# MAGIC - Captures semantic meaning ("BCD Travel" ≈ "BCD Meetings")
# MAGIC - Fast computation (pre-computed golden source embeddings)
# MAGIC - Language-agnostic after translation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Model 4: GPT-3.5-Turbo (via OpenRouter)**
# MAGIC
# MAGIC **Type:** Large Language Model (LLM) - Generative AI
# MAGIC
# MAGIC **Purpose:** Fallback for unmatched suppliers
# MAGIC - Leverages world knowledge about companies
# MAGIC - Infers parent-subsidiary relationships
# MAGIC - Provides confidence scores
# MAGIC
# MAGIC **API Configuration:**
# MAGIC ```python
# MAGIC import openai
# MAGIC
# MAGIC openai.api_base = "https://openrouter.ai/api/v1"
# MAGIC openai.api_key = "your-api-key"
# MAGIC
# MAGIC response = openai.ChatCompletion.create(
# MAGIC     model="gpt-3.5-turbo",
# MAGIC     messages=[{"role": "user", "content": query}],
# MAGIC     max_tokens=300,
# MAGIC     temperature=0.3  # Lower = more deterministic
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Prompt Engineering:**
# MAGIC ```python
# MAGIC query = f"""
# MAGIC Find the top 5 most likely parent companies for '{corp_name}' 
# MAGIC located in {city}, {country}.
# MAGIC
# MAGIC For each parent company, provide:
# MAGIC 1. The parent company name
# MAGIC 2. A confidence score between 0.0 and 1.0
# MAGIC
# MAGIC Format your response as:
# MAGIC 1. ParentCompanyName1 (score: 0.X)
# MAGIC 2. ParentCompanyName2 (score: 0.X)
# MAGIC ...
# MAGIC """
# MAGIC ```
# MAGIC
# MAGIC **Output Processing:**
# MAGIC - Regex parsing to extract company names and scores
# MAGIC - Returns top 5 candidates per supplier
# MAGIC - Denormalized format: 5 rows per supplier (rank 1-5)
# MAGIC
# MAGIC **Why GPT?**
# MAGIC - Handles cases where golden source lacks reference data
# MAGIC - Uses contextual information (city, country)
# MAGIC - Provides ranked alternatives with confidence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Technical Terms
# MAGIC
# MAGIC ### **Embeddings**
# MAGIC Dense vector representations of text that capture semantic meaning. Similar texts have similar embeddings in high-dimensional space.
# MAGIC
# MAGIC ### **Cosine Similarity**
# MAGIC Measures similarity between two vectors based on the cosine of the angle between them:
# MAGIC ```
# MAGIC cos(θ) = (A · B) / (||A|| × ||B||)
# MAGIC ```
# MAGIC Range: -1 (opposite) to 1 (identical)
# MAGIC
# MAGIC ### **mapInPandas (Spark)**
# MAGIC Distributed processing method that applies a Pandas UDF to partitions of a Spark DataFrame:
# MAGIC ```python
# MAGIC df.mapInPandas(function, schema=output_schema)
# MAGIC ```
# MAGIC - Enables Python library usage (googletrans, sentence-transformers) in distributed context
# MAGIC - Serverless-compatible (vs. mapPartitions)
# MAGIC
# MAGIC ### **Denormalization**
# MAGIC Data structure where one input row produces multiple output rows:
# MAGIC - **Matched:** 1 supplier → 1 row (rank=1, score=1.0)
# MAGIC - **Unmatched:** 1 supplier → 5 rows (rank=1-5, varying scores)
# MAGIC
# MAGIC ### **Temperature (LLM Parameter)**
# MAGIC Controls randomness in GPT responses:
# MAGIC - **0.0:** Deterministic (always same output)
# MAGIC - **0.3:** Low variability (used here for consistency)
# MAGIC - **1.0:** High creativity
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Performance Considerations
# MAGIC
# MAGIC ### **SBERT Processing**
# MAGIC - Golden source embeddings computed once (\~50 records)
# MAGIC - Input embeddings computed per batch
# MAGIC - Cosine similarity: O(n) per query
# MAGIC
# MAGIC ### **GPT API Calls**
# MAGIC - Rate limits apply (OpenRouter tier-dependent)
# MAGIC - Batched processing via `mapInPandas`
# MAGIC - Timeout/retry logic recommended for production
# MAGIC
# MAGIC ### **Spark Optimization**
# MAGIC ```python
# MAGIC # Cache intermediate results
# MAGIC translated.cache()
# MAGIC
# MAGIC # Repartition for parallel processing
# MAGIC df.repartition(10).mapInPandas(...)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Output Schema
# MAGIC
# MAGIC **Final Table:** `tamr_poc_ref.data_source.supplier_matched_with_ai_denorm`
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | corporate_name | STRING | Original supplier name |
# MAGIC | corporate_name_en | STRING | Translated name |
# MAGIC | parent_company_name | STRING | Matched parent company |
# MAGIC | confidence_score | FLOAT | Match confidence (0.0-1.0) |
# MAGIC | rank | INT | Ranking (1=best, 5=worst) |
# MAGIC | match_status | STRING | 'matched' or 'not_matched' |
# MAGIC | name_of_the_city | STRING | Location data |
# MAGIC | country | STRING | Country code |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Threshold Tuning
# MAGIC
# MAGIC **Current:** `threshold = 0.85` (strict matching)
# MAGIC
# MAGIC **Impact:**
# MAGIC - **Higher (0.90+):** Fewer false positives, more GPT calls
# MAGIC - **Lower (0.75):** More SBERT matches, risk of false positives
# MAGIC
# MAGIC **Recommended Approach:**
# MAGIC ```python
# MAGIC # Precision-Recall analysis
# MAGIC for threshold in [0.75, 0.80, 0.85, 0.90]:
# MAGIC     evaluate_matches(threshold)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Error Handling
# MAGIC
# MAGIC ```python
# MAGIC try:
# MAGIC     response = openai.ChatCompletion.create(...)
# MAGIC except openai.error.RateLimitError:
# MAGIC     # Implement exponential backoff
# MAGIC     time.sleep(2 ** retry_count)
# MAGIC except Exception as e:
# MAGIC     # Fallback: return NA with score 0.0
# MAGIC     matches = [{"parent_company_name": "NA", "confidence_score": 0.0}] * 5
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## References
# MAGIC
# MAGIC - **Sentence-BERT Paper:** [arxiv.org/abs/1908.10084](https://arxiv.org/abs/1908.10084)
# MAGIC - **all-MiniLM-L6-v2:** HuggingFace Model Card
# MAGIC - **GPT-3.5:** OpenAI Documentation
# MAGIC - **OpenRouter:** [openrouter.ai/docs](https://openrouter.ai/docs)

# COMMAND ----------

# DBTITLE 1,Step-by-Step Implementation Guide
# MAGIC %md
# MAGIC # Step-by-Step Implementation Guide
# MAGIC
# MAGIC ## Stage 1: Create Input Data Table
# MAGIC
# MAGIC **Objective:** Set up the source data table with supplier records
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE IF EXISTS tamr_poc_ref.data_source.supplier_input_to_tamr;
# MAGIC
# MAGIC CREATE TABLE tamr_poc_ref.data_source.supplier_input_to_tamr (
# MAGIC     corporate_name VARCHAR(255),
# MAGIC     trade_name VARCHAR(255),
# MAGIC     legal_form VARCHAR(255),
# MAGIC     box_zip_code VARCHAR(50),
# MAGIC     name_of_the_city VARCHAR(255),
# MAGIC     state_region VARCHAR(255),
# MAGIC     country VARCHAR(255),
# MAGIC     address_1 VARCHAR(255),
# MAGIC     tech_file_name VARCHAR(255)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr 
# MAGIC VALUES 
# MAGIC     ('BCD TRAVEL HONG KONG LIMITED', NULL, NULL, NULL, 'HONG KONG', NULL, 'HK', NULL, 'FCA81225.CSV'),
# MAGIC     ('OBJETRAMA', 'SAS', 'SAS', NULL, 'MUNDOLSHEIM CEDEX', NULL, 'FR', NULL, 'FAA11219.CSV'),
# MAGIC     -- Add more records...
# MAGIC ```
# MAGIC
# MAGIC **Key Points:**
# MAGIC - Stores raw supplier data from multiple file sources
# MAGIC - No parent company information initially
# MAGIC - Country codes follow ISO 3166-1 alpha-2 standard
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Stage 2: Create Golden Source Table
# MAGIC
# MAGIC **Objective:** Build reference dataset with known parent companies
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE tamr_poc_ref.data_source.supplier_golden_source (
# MAGIC     corporate_name VARCHAR(255),
# MAGIC     trade_name VARCHAR(255),
# MAGIC     legal_form VARCHAR(255),
# MAGIC     box_zip_code VARCHAR(50),
# MAGIC     name_of_the_city VARCHAR(255),
# MAGIC     state_region VARCHAR(255),
# MAGIC     country VARCHAR(255),
# MAGIC     parent_company_name VARCHAR(255),  -- Key addition!
# MAGIC     tech_file_name VARCHAR(255)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source 
# MAGIC VALUES 
# MAGIC     ('BCD TRAVEL HONG KONG LIMITED', NULL, NULL, NULL, 'HONG KONG', NULL, 'HK', 'BCD TRAVEL', 'FCA81225.CSV'),
# MAGIC     ('OBJETRAMA', 'SAS', 'SAS', NULL, 'MUNDOLSHEIM CEDEX', NULL, 'FR', 'OBJETRAMA', 'FAA11219.CSV'),
# MAGIC     -- Add more records...
# MAGIC ```
# MAGIC
# MAGIC **Key Points:**
# MAGIC - Contains `parent_company_name` column (the ground truth)
# MAGIC - Used as the reference corpus for SBERT matching
# MAGIC - Should cover major parent companies in your domain
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Stage 3: Install Python Packages
# MAGIC
# MAGIC **Objective:** Install required libraries for translation and embeddings
# MAGIC
# MAGIC ```python
# MAGIC %pip install --upgrade \
# MAGIC     googletrans==4.0.0rc1 \
# MAGIC     langdetect \
# MAGIC     sentence-transformers \
# MAGIC     "huggingface-hub>=0.34.0,<1.0" \
# MAGIC     openai
# MAGIC
# MAGIC dbutils.library.restartPython()  # Restart to load new packages
# MAGIC ```
# MAGIC
# MAGIC **Package Breakdown:**
# MAGIC - **googletrans:** Google Translate API wrapper
# MAGIC - **langdetect:** Language detection library
# MAGIC - **sentence-transformers:** SBERT models from HuggingFace
# MAGIC - **huggingface-hub:** Download and cache transformer models
# MAGIC - **openai:** OpenAI/OpenRouter API client
# MAGIC
# MAGIC **Important:** Always restart Python after pip install in Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Stage 4: Translate to English
# MAGIC
# MAGIC **Objective:** Normalize all supplier names to English for consistent embedding
# MAGIC
# MAGIC ```python
# MAGIC from googletrans import Translator
# MAGIC from langdetect import detect
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.types import StructType, StructField, StringType
# MAGIC
# MAGIC def translate_batch(iterator):
# MAGIC     """
# MAGIC     Pandas UDF to translate corporate and trade names to English
# MAGIC     Runs in parallel across Spark partitions
# MAGIC     """
# MAGIC     translator = Translator()
# MAGIC     
# MAGIC     for pdf in iterator:
# MAGIC         results = []
# MAGIC         for _, row in pdf.iterrows():
# MAGIC             corp_name = row['corporate_name']
# MAGIC             trade_name = row['trade_name']
# MAGIC             
# MAGIC             # Translate corporate name
# MAGIC             if corp_name and isinstance(corp_name, str) and corp_name.strip():
# MAGIC                 try:
# MAGIC                     corp_lang = detect(corp_name)  # Detect language
# MAGIC                     if corp_lang != 'en':
# MAGIC                         corp_trans = translator.translate(
# MAGIC                             corp_name, 
# MAGIC                             src=corp_lang, 
# MAGIC                             dest='en'
# MAGIC                         ).text
# MAGIC                     else:
# MAGIC                         corp_trans = corp_name  # Already English
# MAGIC                 except Exception as e:
# MAGIC                     corp_trans = corp_name  # Fallback to original
# MAGIC             else:
# MAGIC                 corp_trans = corp_name
# MAGIC             
# MAGIC             # Translate trade name (same logic)
# MAGIC             if trade_name and isinstance(trade_name, str) and trade_name.strip():
# MAGIC                 try:
# MAGIC                     trade_lang = detect(trade_name)
# MAGIC                     if trade_lang != 'en':
# MAGIC                         trade_trans = translator.translate(
# MAGIC                             trade_name, 
# MAGIC                             src=trade_lang, 
# MAGIC                             dest='en'
# MAGIC                         ).text
# MAGIC                     else:
# MAGIC                         trade_trans = trade_name
# MAGIC                 except Exception as e:
# MAGIC                     trade_trans = trade_name
# MAGIC             else:
# MAGIC                 trade_trans = trade_name
# MAGIC             
# MAGIC             # Build result row with new columns
# MAGIC             result_row = row.to_dict()
# MAGIC             result_row['corporate_name_en'] = corp_trans
# MAGIC             result_row['trade_name_en'] = trade_trans
# MAGIC             results.append(result_row)
# MAGIC         
# MAGIC         yield pd.DataFrame(results)
# MAGIC
# MAGIC # Read input data
# MAGIC df = spark.table("tamr_poc_ref.data_source.supplier_input_to_tamr")
# MAGIC
# MAGIC # Define output schema (original + 2 new columns)
# MAGIC output_schema = df.schema \
# MAGIC     .add(StructField("corporate_name_en", StringType(), True)) \
# MAGIC     .add(StructField("trade_name_en", StringType(), True))
# MAGIC
# MAGIC # Apply translation in parallel
# MAGIC translated = df.mapInPandas(translate_batch, schema=output_schema)
# MAGIC
# MAGIC display(translated)
# MAGIC ```
# MAGIC
# MAGIC **Technical Details:**
# MAGIC - **mapInPandas:** Distributes translation across Spark workers
# MAGIC - **Language Detection:** Avoids unnecessary translation for English text
# MAGIC - **Error Handling:** Falls back to original text if translation fails
# MAGIC - **Batch Processing:** Processes DataFrame partitions in parallel
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Stage 5: Semantic Matching with SBERT
# MAGIC
# MAGIC **Objective:** Match suppliers to parent companies using sentence embeddings
# MAGIC
# MAGIC ```python
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC import os
# MAGIC from pyspark.sql.types import StructType, StructField, StringType
# MAGIC from sentence_transformers import SentenceTransformer
# MAGIC
# MAGIC # Set cache directory for model downloads
# MAGIC os.environ['HF_HOME'] = '/tmp/huggingface_cache'
# MAGIC os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface_cache'
# MAGIC
# MAGIC # Load golden source as Pandas DataFrame
# MAGIC golden_df = spark.table("tamr_poc_ref.data_source.supplier_golden_source")
# MAGIC golden_pd = golden_df.select("corporate_name", "parent_company_name").toPandas()
# MAGIC
# MAGIC golden_names = golden_pd["corporate_name"].fillna("").tolist()
# MAGIC golden_parents = golden_pd["parent_company_name"].fillna("NA").tolist()
# MAGIC
# MAGIC def match_parent_company(iterator):
# MAGIC     """
# MAGIC     Pandas UDF to find best matching parent company using SBERT
# MAGIC     """
# MAGIC     # Set cache inside UDF
# MAGIC     import os
# MAGIC     os.environ['HF_HOME'] = '/tmp/huggingface_cache'
# MAGIC     os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface_cache'
# MAGIC     
# MAGIC     from sentence_transformers import SentenceTransformer
# MAGIC     import numpy as np
# MAGIC     
# MAGIC     # Load SBERT model
# MAGIC     model = SentenceTransformer('all-MiniLM-L6-v2')
# MAGIC     
# MAGIC     # Pre-compute golden source embeddings (once per partition)
# MAGIC     golden_embeddings = model.encode(golden_names, show_progress_bar=False)
# MAGIC     
# MAGIC     for pdf in iterator:
# MAGIC         results = []
# MAGIC         for _, row in pdf.iterrows():
# MAGIC             # Use translated name if available
# MAGIC             query_name = row.get("corporate_name_en") or row.get("corporate_name") or ""
# MAGIC             
# MAGIC             # Generate query embedding
# MAGIC             query_emb = model.encode([query_name])[0]
# MAGIC             
# MAGIC             # Compute cosine similarities
# MAGIC             sims = np.dot(golden_embeddings, query_emb) / (
# MAGIC                 np.linalg.norm(golden_embeddings, axis=1) * np.linalg.norm(query_emb) + 1e-10
# MAGIC             )
# MAGIC             
# MAGIC             # Find best match
# MAGIC             best_idx = np.argmax(sims)
# MAGIC             best_score = sims[best_idx]
# MAGIC             
# MAGIC             # Apply threshold
# MAGIC             threshold = 0.85
# MAGIC             if best_score >= threshold:
# MAGIC                 parent_company = golden_parents[best_idx]
# MAGIC                 match_status = "matched"
# MAGIC             else:
# MAGIC                 parent_company = "NA"
# MAGIC                 match_status = "not_matched"
# MAGIC             
# MAGIC             # Build result
# MAGIC             result_row = row.to_dict()
# MAGIC             result_row["parent_company_name"] = parent_company
# MAGIC             result_row["match_status"] = match_status
# MAGIC             results.append(result_row)
# MAGIC         
# MAGIC         yield pd.DataFrame(results)
# MAGIC
# MAGIC # Apply matching
# MAGIC output_schema = translated.schema \
# MAGIC     .add("parent_company_name", "string") \
# MAGIC     .add("match_status", "string")
# MAGIC
# MAGIC matched_all = translated.mapInPandas(match_parent_company, schema=output_schema)
# MAGIC
# MAGIC # Split results
# MAGIC matched_df = matched_all.filter("match_status = 'matched'")
# MAGIC not_matched_df = matched_all.filter("match_status = 'not_matched'")
# MAGIC
# MAGIC print(f"✓ Matched: {matched_df.count()} records")
# MAGIC print(f"✓ Not matched: {not_matched_df.count()} records")
# MAGIC
# MAGIC # Save to temp tables
# MAGIC matched_df.write.mode("overwrite").saveAsTable("tamr_poc_ref.data_source.temp_match")
# MAGIC not_matched_df.write.mode("overwrite").saveAsTable("tamr_poc_ref.data_source.temp_not_match")
# MAGIC ```
# MAGIC
# MAGIC **Mathematical Foundation:**
# MAGIC
# MAGIC **Cosine Similarity Formula:**
# MAGIC ```
# MAGIC similarity = (A · B) / (||A|| × ||B||)
# MAGIC
# MAGIC Where:
# MAGIC   A · B = dot product of embeddings
# MAGIC   ||A|| = L2 norm (magnitude) of vector A
# MAGIC   ||B|| = L2 norm of vector B
# MAGIC ```
# MAGIC
# MAGIC **Example Similarity Scores:**
# MAGIC - "BCD TRAVEL HONG KONG" vs "BCD TRAVEL ASIA PACIFIC": 0.92 ✓ (matched)
# MAGIC - "OBJETRAMA" vs "DERICHEBOURG": 0.31 ✗ (not matched)
# MAGIC - "PWC ENTERPRISES" vs "PRICEWATERHOUSECOOPERS": 0.88 ✓ (matched)
# MAGIC
# MAGIC **Threshold Selection:**
# MAGIC - **0.85:** Balances precision and recall
# MAGIC - **Higher:** Fewer false positives, more GPT fallback
# MAGIC - **Lower:** More SBERT matches, risk of incorrect matches
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Stage 6: GPT-3.5 Fallback for Unmatched Records
# MAGIC
# MAGIC **Objective:** Use LLM to find parent companies for records that failed SBERT matching
# MAGIC
# MAGIC ### Step 6a: Configure API
# MAGIC
# MAGIC ```python
# MAGIC import openai
# MAGIC
# MAGIC openai.api_base = "https://openrouter.ai/api/v1"
# MAGIC openai.api_key = "sk-or-v1-YOUR-API-KEY-HERE"  # Replace with your key
# MAGIC ```
# MAGIC
# MAGIC ### Step 6b: Test API Call (Single Record)
# MAGIC
# MAGIC ```python
# MAGIC corp_name = "BCD TRAVEL HONG KONG LIMITED"
# MAGIC city = "HONG KONG"
# MAGIC country = "HK"
# MAGIC
# MAGIC query = f"""
# MAGIC Find the top 5 most likely parent companies for '{corp_name}' 
# MAGIC located in {city}, {country}.
# MAGIC
# MAGIC For each parent company, provide:
# MAGIC 1. The parent company name
# MAGIC 2. A confidence score between 0.0 and 1.0
# MAGIC
# MAGIC Format your response as:
# MAGIC 1. ParentCompanyName1 (score: 0.X)
# MAGIC 2. ParentCompanyName2 (score: 0.X)
# MAGIC ...
# MAGIC """
# MAGIC
# MAGIC try:
# MAGIC     response = openai.ChatCompletion.create(
# MAGIC         model="gpt-3.5-turbo",
# MAGIC         messages=[{"role": "user", "content": query}],
# MAGIC         max_tokens=300,
# MAGIC         temperature=0.3  # Lower = more consistent
# MAGIC     )
# MAGIC     print("API call successful!")
# MAGIC     print(f"Response: {response.choices[0].message.content}")
# MAGIC except Exception as e:
# MAGIC     print(f"API call failed: {type(e).__name__}: {str(e)}")
# MAGIC ```
# MAGIC
# MAGIC **Expected Output:**
# MAGIC ```
# MAGIC API call successful!
# MAGIC Response: 
# MAGIC 1. BCD Travel (score: 0.95)
# MAGIC 2. BCD Meetings & Events (score: 0.85)
# MAGIC 3. Advito (score: 0.75)
# MAGIC 4. TripActions (score: 0.40)
# MAGIC 5. American Express Global Business Travel (score: 0.35)
# MAGIC ```
# MAGIC
# MAGIC ### Step 6c: Batch Processing with Denormalization
# MAGIC
# MAGIC ```python
# MAGIC import pandas as pd
# MAGIC import re
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
# MAGIC
# MAGIC API_KEY = "sk-or-v1-YOUR-API-KEY-HERE"
# MAGIC
# MAGIC def gpt_search_parent_company_denorm(iterator):
# MAGIC     """
# MAGIC     Pandas UDF to query GPT for parent companies
# MAGIC     Returns 5 rows per supplier (denormalized)
# MAGIC     """
# MAGIC     import openai
# MAGIC     import re
# MAGIC     
# MAGIC     openai.api_base = "https://openrouter.ai/api/v1"
# MAGIC     openai.api_key = API_KEY
# MAGIC     
# MAGIC     for pdf in iterator:
# MAGIC         results = []
# MAGIC         for _, row in pdf.iterrows():
# MAGIC             corp_name = row['corporate_name']
# MAGIC             city = row.get('name_of_the_city', '') or ''
# MAGIC             country = row.get('country', '') or ''
# MAGIC             
# MAGIC             # Build query
# MAGIC             query = f"""
# MAGIC Find the top 5 most likely parent companies for '{corp_name}' 
# MAGIC located in {city}, {country}.
# MAGIC
# MAGIC For each parent company, provide:
# MAGIC 1. The parent company name
# MAGIC 2. A confidence score between 0.0 and 1.0
# MAGIC
# MAGIC Format your response as:
# MAGIC 1. ParentCompanyName1 (score: 0.X)
# MAGIC 2. ParentCompanyName2 (score: 0.X)
# MAGIC ...
# MAGIC """
# MAGIC             
# MAGIC             try:
# MAGIC                 response = openai.ChatCompletion.create(
# MAGIC                     model="gpt-3.5-turbo",
# MAGIC                     messages=[{"role": "user", "content": query}],
# MAGIC                     max_tokens=300,
# MAGIC                     temperature=0.3
# MAGIC                 )
# MAGIC                 text = response.choices[0].message.content
# MAGIC                 
# MAGIC                 # Parse response with regex
# MAGIC                 matches = []
# MAGIC                 pattern = r'(?:\d+\.)?\s*([^(\n]+?)\s*(?:\(score:\s*|[-:]\s*)?([0-1]\.[0-9]+|[0-1])'
# MAGIC                 
# MAGIC                 for match in re.finditer(pattern, text, re.IGNORECASE):
# MAGIC                     company = match.group(1).strip()
# MAGIC                     score_str = match.group(2)
# MAGIC                     try:
# MAGIC                         score = float(score_str)
# MAGIC                         matches.append({
# MAGIC                             "parent_company_name": company,
# MAGIC                             "confidence_score": score
# MAGIC                         })
# MAGIC                     except:
# MAGIC                         continue
# MAGIC                 
# MAGIC                 # Ensure 5 results
# MAGIC                 matches = matches[:5]
# MAGIC                 while len(matches) < 5:
# MAGIC                     matches.append({
# MAGIC                         "parent_company_name": "NA", 
# MAGIC                         "confidence_score": 0.0
# MAGIC                     })
# MAGIC                 
# MAGIC             except Exception as e:
# MAGIC                 # Fallback on error
# MAGIC                 matches = [
# MAGIC                     {"parent_company_name": "NA", "confidence_score": 0.0}
# MAGIC                 ] * 5
# MAGIC             
# MAGIC             # Denormalize: create 5 rows per supplier
# MAGIC             for idx, m in enumerate(matches):
# MAGIC                 result_row = row.to_dict()
# MAGIC                 result_row["parent_company_name"] = m["parent_company_name"]
# MAGIC                 result_row["confidence_score"] = m["confidence_score"]
# MAGIC                 result_row["rank"] = idx + 1  # 1 = best, 5 = worst
# MAGIC                 results.append(result_row)
# MAGIC         
# MAGIC         yield pd.DataFrame(results)
# MAGIC
# MAGIC # Load temp tables
# MAGIC matched_df = spark.table("tamr_poc_ref.data_source.temp_match")
# MAGIC not_matched_df = spark.table("tamr_poc_ref.data_source.temp_not_match")
# MAGIC
# MAGIC # Process matched records (add confidence_score=1.0, rank=1)
# MAGIC def denorm_matched(iterator):
# MAGIC     for pdf in iterator:
# MAGIC         results = []
# MAGIC         for _, row in pdf.iterrows():
# MAGIC             result_row = row.to_dict()
# MAGIC             result_row["confidence_score"] = 1.0
# MAGIC             result_row["rank"] = 1
# MAGIC             results.append(result_row)
# MAGIC         yield pd.DataFrame(results)
# MAGIC
# MAGIC # Define schemas
# MAGIC denorm_schema = matched_df.schema \
# MAGIC     .add(StructField("confidence_score", FloatType(), True)) \
# MAGIC     .add(StructField("rank", IntegerType(), True))
# MAGIC
# MAGIC search_schema = not_matched_df.schema \
# MAGIC     .add(StructField("confidence_score", FloatType(), True)) \
# MAGIC     .add(StructField("rank", IntegerType(), True))
# MAGIC
# MAGIC # Process both sets
# MAGIC matched_denorm = matched_df.mapInPandas(denorm_matched, schema=denorm_schema)
# MAGIC searched_denorm = not_matched_df.mapInPandas(gpt_search_parent_company_denorm, schema=search_schema)
# MAGIC
# MAGIC # Combine
# MAGIC final_denorm = matched_denorm.unionByName(searched_denorm)
# MAGIC
# MAGIC # Save final output
# MAGIC final_denorm.write.mode("overwrite").saveAsTable(
# MAGIC     "tamr_poc_ref.data_source.supplier_matched_with_ai_denorm"
# MAGIC )
# MAGIC
# MAGIC print(f"✓ Total rows (denormalized): {final_denorm.count()}")
# MAGIC display(final_denorm)
# MAGIC ```
# MAGIC
# MAGIC **Output Structure (Denormalized):**
# MAGIC
# MAGIC | corporate_name | parent_company_name | confidence_score | rank | match_status |
# MAGIC |----------------|---------------------|------------------|------|---------------|
# MAGIC | FLORAFLORE | FLORAFLORE | 1.0 | 1 | matched |
# MAGIC | TROPPER DATA | Tropper AG | 0.92 | 1 | not_matched |
# MAGIC | TROPPER DATA | Tropper Group | 0.78 | 2 | not_matched |
# MAGIC | TROPPER DATA | Data Services Inc | 0.45 | 3 | not_matched |
# MAGIC | TROPPER DATA | NA | 0.0 | 4 | not_matched |
# MAGIC | TROPPER DATA | NA | 0.0 | 5 | not_matched |
# MAGIC
# MAGIC **Denormalization Benefits:**
# MAGIC - Shows alternative matches for manual review
# MAGIC - Enables confidence-based filtering
# MAGIC - Supports ranking/sorting by score
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Complete Pipeline Summary
# MAGIC
# MAGIC ```python
# MAGIC # 1. Load data
# MAGIC input_df = spark.table("tamr_poc_ref.data_source.supplier_input_to_tamr")
# MAGIC golden_df = spark.table("tamr_poc_ref.data_source.supplier_golden_source")
# MAGIC
# MAGIC # 2. Translate
# MAGIC translated = input_df.mapInPandas(translate_batch, schema=translation_schema)
# MAGIC
# MAGIC # 3. SBERT matching
# MAGIC matched_all = translated.mapInPandas(match_parent_company, schema=match_schema)
# MAGIC matched = matched_all.filter("match_status = 'matched'")
# MAGIC not_matched = matched_all.filter("match_status = 'not_matched'")
# MAGIC
# MAGIC # 4. GPT fallback for unmatched
# MAGIC matched_final = matched.mapInPandas(denorm_matched, schema=final_schema)
# MAGIC searched_final = not_matched.mapInPandas(gpt_search_parent_company_denorm, schema=final_schema)
# MAGIC
# MAGIC # 5. Combine and save
# MAGIC final = matched_final.unionByName(searched_final)
# MAGIC final.write.mode("overwrite").saveAsTable("supplier_matched_with_ai_denorm")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Query Examples for Analysis
# MAGIC
# MAGIC ### Get Best Matches Only
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     corporate_name,
# MAGIC     parent_company_name,
# MAGIC     confidence_score,
# MAGIC     match_status
# MAGIC FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
# MAGIC WHERE rank = 1  -- Best match only
# MAGIC ORDER BY confidence_score DESC;
# MAGIC ```
# MAGIC
# MAGIC ### High-Confidence Matches
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
# MAGIC WHERE confidence_score >= 0.85
# MAGIC AND rank = 1;
# MAGIC ```
# MAGIC
# MAGIC ### Review Low-Confidence Matches (Manual Review Needed)
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     corporate_name,
# MAGIC     parent_company_name,
# MAGIC     confidence_score,
# MAGIC     rank
# MAGIC FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
# MAGIC WHERE confidence_score < 0.70
# MAGIC AND match_status = 'not_matched'
# MAGIC ORDER BY corporate_name, rank;
# MAGIC ```
# MAGIC
# MAGIC ### Match Rate Analysis
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     match_status,
# MAGIC     COUNT(DISTINCT corporate_name) as supplier_count,
# MAGIC     ROUND(COUNT(DISTINCT corporate_name) * 100.0 / SUM(COUNT(DISTINCT corporate_name)) OVER(), 2) as percentage
# MAGIC FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
# MAGIC WHERE rank = 1
# MAGIC GROUP BY match_status;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Recommendations
# MAGIC
# MAGIC ### 1. **Add Retry Logic**
# MAGIC ```python
# MAGIC import time
# MAGIC
# MAGIC def call_gpt_with_retry(query, max_retries=3):
# MAGIC     for attempt in range(max_retries):
# MAGIC         try:
# MAGIC             response = openai.ChatCompletion.create(...)
# MAGIC             return response
# MAGIC         except openai.error.RateLimitError:
# MAGIC             time.sleep(2 ** attempt)  # Exponential backoff
# MAGIC         except Exception as e:
# MAGIC             if attempt == max_retries - 1:
# MAGIC                 raise
# MAGIC     return None
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Cache Golden Source Embeddings**
# MAGIC ```python
# MAGIC # Compute once, save to Delta table
# MAGIC golden_embeddings = model.encode(golden_names)
# MAGIC spark.createDataFrame(
# MAGIC     pd.DataFrame({
# MAGIC         'corporate_name': golden_names,
# MAGIC         'embedding': golden_embeddings.tolist()
# MAGIC     })
# MAGIC ).write.mode("overwrite").saveAsTable("golden_embeddings_cache")
# MAGIC ```
# MAGIC
# MAGIC ### 3. **Monitor API Costs**
# MAGIC ```python
# MAGIC # Track GPT calls
# MAGIC total_api_calls = not_matched_df.count()
# MAGIC estimated_cost = total_api_calls * 0.0015  # GPT-3.5 pricing
# MAGIC print(f"Estimated OpenRouter cost: ${estimated_cost:.2f}")
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Add Logging**
# MAGIC ```python
# MAGIC import logging
# MAGIC
# MAGIC logging.basicConfig(level=logging.INFO)
# MAGIC logger = logging.getLogger(__name__)
# MAGIC
# MAGIC logger.info(f"Processing batch of {len(pdf)} records")
# MAGIC logger.warning(f"API call failed for {corp_name}: {str(e)}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Tune Threshold:** Experiment with SBERT threshold values
# MAGIC 2. **Enrich Golden Source:** Add more reference records
# MAGIC 3. **Evaluate Accuracy:** Manual review of sample matches
# MAGIC 4. **Optimize Performance:** Cache embeddings, batch GPT calls
# MAGIC 5. **Add Monitoring:** Track match rates, API costs, latency

# COMMAND ----------

# DBTITLE 1,Stage1 - Create Source Data : Data which will be use as Input
# MAGIC %sql
# MAGIC drop table tamr_poc_ref.data_source.supplier_input_to_tamr;
# MAGIC CREATE TABLE tamr_poc_ref.data_source.supplier_input_to_tamr (
# MAGIC     corporate_name VARCHAR(255),
# MAGIC     trade_name VARCHAR(255),
# MAGIC     legal_form VARCHAR(255),
# MAGIC     box_zip_code VARCHAR(50),
# MAGIC     name_of_the_city VARCHAR(255),
# MAGIC     state_region VARCHAR(255),
# MAGIC     country VARCHAR(255),
# MAGIC     address_1 VARCHAR(255),
# MAGIC     tech_file_name VARCHAR(255)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('BCD TRAVEL HONG KONG LIMITED',NULL,NULL,NULL,'HONG KONG',NULL,'HK',NULL,'FCA81225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('OBJETRAMA','SAS','SAS',NULL,'MUNDOLSHEIM CEDEX',NULL,'FR',NULL,'FAA11219.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('EAU-01206','AUTRE','AUTRE',NULL,'LYON',NULL,'FR',NULL,'FAA11219.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('BCD MEETINGS ET EVENTS FRANCE',NULL,NULL,NULL,'PUTEAUX',NULL,'FR',NULL,'FSG11225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('CONSTRUCTA ASSET MANAGEMENT',NULL,NULL,NULL,'PARIS',NULL,'FR',NULL,'FAD11222.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('SG AIR TRAVEL',NULL,NULL,NULL,'SEOUL',NULL,'KR',NULL,'FCA81218.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('BCD TRAVEL HONG KONG LIMITED',NULL,NULL,NULL,'HONG KONG',NULL,'HK',NULL,'FCB91220.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('TECHNOSOL (BNP FACTOR)',NULL,NULL,NULL,'BALLAINVILLIERS',NULL,'FRA',NULL,'FSO11223.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('STADT LEIPZIG',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'HB_0118.XLS');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('BCD TRAVEL ASIA PACIFIC PTE LTD',NULL,NULL,NULL,'SINGAPORE',NULL,'SG',NULL,'FCA81225.CSV');
# MAGIC
# MAGIC -- 11–20
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('CHAMBRE DE COMMERCE ET D INDUSTRIE DU LO',NULL,NULL,NULL,'ORLEANS CEDEX 1',NULL,'FR',NULL,'FAA21224.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('LLOYDS REGISTER EMEA',NULL,NULL,NULL,'SOUTHAMPTON',NULL,'GB',NULL,'FAL21221.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('SOCIETE GENERALE AFRICAN BUSINESS S',NULL,NULL,NULL,'CASABLANCA',NULL,'MA',NULL,'FAV50918.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('PRICEWATERHOUSECOOPERS ENTERPRISES',NULL,NULL,NULL,'NEUILLY SUR SEINE CEDEX',NULL,'FR',NULL,'FAU41118.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('DERICHEBOURG','SAS','SAS',NULL,'PARIS',NULL,'FR',NULL,'FAA11219.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('COMMUNICATIONS A.S.',NULL,NULL,NULL,'PRAHA 1',NULL,'CZ',NULL,'FAT11225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('EVOLUTION',NULL,NULL,NULL,'SAINT LAURENT DU PONT',NULL,'FR',NULL,'FAA21220.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('ITALIAN PROPRETE EST',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'20210716_FILE.XLSX');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('NEXITY PROPERTY MANAGEMENT',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'BOURSORAMA_2022.XLSX');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('SOLUTEC',NULL,NULL,NULL,'LYON',NULL,'FR',NULL,'FAV51225.CSV');
# MAGIC
# MAGIC -- 21–30
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('OOO VLADOBLTORG',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'RB_1121.XLSX');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('FLORAFLORE',NULL,NULL,NULL,'GISORS',NULL,'FR',NULL,'FAU41020.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('SARL TECHNIC REPRO SERVICES',NULL,NULL,NULL,'PAU',NULL,'FR',NULL,'FSG11224.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('TROPPER DATA SERVICE AG',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'FGE11224.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('FREELANCE VINS FINS SA',NULL,NULL,NULL,'BECH',NULL,'LU',NULL,'FCW21225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('GARAGE COPERNIC',NULL,NULL,NULL,'NANTES',NULL,'FR',NULL,'FAA11219.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('SCI l \'Oratoire',NULL,NULL,NULL,'BOLLENE',NULL,'FR',NULL,'FSG11225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('Jeanne d\'ARC de Vichy',NULL,NULL,NULL,'BELLERIVE SUR ALLIER',NULL,'FR',NULL,'FAA21225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('LAFFITTE PIERRE SCPI',NULL,NULL,NULL,'PARIS',NULL,'FR',NULL,'FAD11222.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr VALUES ('ABBAYE DES PREMONTRES',NULL,NULL,NULL,'PONT A MOUSSON CEDEX',NULL,'FR',NULL,'FSG11223.CSV');

# COMMAND ----------

# DBTITLE 1,Stage2 - Create Golder Source Data : Data which will be use as reference
# MAGIC %sql
# MAGIC CREATE TABLE tamr_poc_ref.data_source.supplier_golden_source (
# MAGIC     corporate_name VARCHAR(255),
# MAGIC     trade_name VARCHAR(255),
# MAGIC     legal_form VARCHAR(255),
# MAGIC     box_zip_code VARCHAR(50),
# MAGIC     name_of_the_city VARCHAR(255),
# MAGIC     state_region VARCHAR(255),
# MAGIC     country VARCHAR(255),
# MAGIC     parent_company_name VARCHAR(255),
# MAGIC     tech_file_name VARCHAR(255)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('BCD TRAVEL HONG KONG LIMITED',NULL,NULL,NULL,'HONG KONG',NULL,'HK','BCD TRAVEL','FCA81225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('OBJETRAMA','SAS','SAS',NULL,'MUNDOLSHEIM CEDEX',NULL,'FR','OBJETRAMA','FAA11219.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('EAU-01206','AUTRE','AUTRE',NULL,'LYON',NULL,'FR','VEOLIA EAU','FAA11219.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('BCD MEETINGS ET EVENTS FRANCE',NULL,NULL,NULL,'PUTEAUX',NULL,'FR','BCD TRAVEL','FSG11225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('CONSTRUCTA ASSET MANAGEMENT',NULL,NULL,NULL,'PARIS',NULL,'FR','CONSTRUCTA ASSET MANAGEMENT','FAD11222.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('SG AIR TRAVEL',NULL,NULL,NULL,'SEOUL',NULL,'KR','BCD TRAVEL','FCA81218.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('BCD TRAVEL HONG KONG LIMITED',NULL,NULL,NULL,'HONG KONG',NULL,'HK','BCD TRAVEL','FCB91220.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('TECHNOSOL (BNP FACTOR)',NULL,NULL,NULL,'BALLAINVILLIERS',NULL,'FRA','BNP PARIBAS FACTOR (TECHNOSOL)','FSO11223.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('STADT LEIPZIG',NULL,NULL,NULL,NULL,NULL,NULL,'STADT LEIPZIG','HB_0118.XLS');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('BCD TRAVEL ASIA PACIFIC PTE LTD',NULL,NULL,NULL,'SINGAPORE',NULL,'SG','BCD TRAVEL','FCA81225.CSV');
# MAGIC
# MAGIC -- 11–20
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('CHAMBRE DE COMMERCE ET D INDUSTRIE DU LO',NULL,NULL,NULL,'ORLEANS CEDEX 1',NULL,'FR','CHAMBRE DE COMMERCE ET D INDUSTRIE','FAA21224.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('LLOYDS REGISTER EMEA',NULL,NULL,NULL,'SOUTHAMPTON',NULL,'GB','LLOYDS','FAL21221.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('SOCIETE GENERALE AFRICAN BUSINESS S',NULL,NULL,NULL,'CASABLANCA',NULL,'MA','SOCIETE GENERALE','FAV50918.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('PRICEWATERHOUSECOOPERS ENTERPRISES',NULL,NULL,NULL,'NEUILLY SUR SEINE CEDEX',NULL,'FR','PRICEWATERHOUSECOOPERS ADVISORY','FAU41118.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('DERICHEBOURG','SAS','SAS',NULL,'PARIS',NULL,'FR','ELIOR','FAA11219.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('COMMUNICATIONS A.S.',NULL,NULL,NULL,'PRAHA 1',NULL,'CZ','CREST COMMUNICATIONS A.S.','FAT11225.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('EVOLUTION',NULL,NULL,NULL,'SAINT LAURENT DU PONT',NULL,'FR','CEI','FAA21220.CSV');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('ITALIAN PROPRETE EST',NULL,NULL,NULL,NULL,NULL,NULL,'ITALIAN','20210716_FILE.XLSX');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('NEXITY PROPERTY MANAGEMENT',NULL,NULL,NULL,NULL,NULL,NULL,'NEXITY PROPERTY MANAGEMENT','BOURSORAMA_2022.XLSX');
# MAGIC
# MAGIC INSERT INTO tamr_poc_ref.data_source.supplier_golden_source VALUES ('SOLUTEC',NULL,NULL,NULL,'LYON',NULL,'FR','AUBAY','FAV51225.CSV');
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install Required Packages
# MAGIC %pip install --upgrade googletrans==4.0.0rc1 langdetect sentence-transformers "huggingface-hub>=0.34.0,<1.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Verify Package Installation
# MAGIC %pip install --upgrade googletrans==4.0.0rc1 langdetect sentence-transformers "huggingface-hub>=0.34.0,<1.0" openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Stage3 - Translate to EN
# Import after package installation
try:
    from googletrans import Translator
    from langdetect import detect
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType
    print("✓ Packages imported successfully")
except ImportError as e:
    print(f"⚠ Import error: {e}")
    print("Please restart the kernel (Cmd/Ctrl + Shift + R) and run this cell again")
    raise

def translate_batch(iterator):
    translator = Translator()
    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            corp_name = row['corporate_name']
            trade_name = row['trade_name']
            # Translate corporate name
            if corp_name and isinstance(corp_name, str) and corp_name.strip():
                try:
                    corp_lang = detect(corp_name)
                    if corp_lang != 'en':
                        corp_trans = translator.translate(corp_name, src=corp_lang, dest='en').text
                    else:
                        corp_trans = corp_name
                except Exception as e:
                    corp_trans = corp_name
            else:
                corp_trans = corp_name
            # Translate trade name
            if trade_name and isinstance(trade_name, str) and trade_name.strip():
                try:
                    trade_lang = detect(trade_name)
                    if trade_lang != 'en':
                        trade_trans = translator.translate(trade_name, src=trade_lang, dest='en').text
                    else:
                        trade_trans = trade_name
                except Exception as e:
                    trade_trans = trade_name
            else:
                trade_trans = trade_name
            # Add all original columns plus translated columns
            result_row = row.to_dict()
            result_row['corporate_name_en'] = corp_trans
            result_row['trade_name_en'] = trade_trans
            results.append(result_row)
        yield pd.DataFrame(results)

# Read table as Spark DataFrame
df = spark.table("tamr_poc_ref.data_source.supplier_input_to_tamr")

# Define output schema (all original columns + translated columns at end)
output_schema = df.schema \
    .add(StructField("corporate_name_en", StringType(), True)) \
    .add(StructField("trade_name_en", StringType(), True))

# Apply translation using mapInPandas (serverless-compatible)
translated = df.mapInPandas(translate_batch, schema=output_schema)

display(translated)

# COMMAND ----------

# DBTITLE 1,Stage4 - Match using SBERT
import numpy as np
import pandas as pd
import os
from pyspark.sql.types import StructType, StructField, StringType

# Set writable cache directory for HuggingFace models
os.environ['HF_HOME'] = '/tmp/huggingface_cache'
os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface_cache'

# Read golden source table as Spark DataFrame
golden_df = spark.table("tamr_poc_ref.data_source.supplier_golden_source")

# Collect golden source corporate names and parent company names
golden_pd = golden_df.select("corporate_name", "parent_company_name").toPandas()
golden_names = golden_pd["corporate_name"].fillna("").tolist()
golden_parents = golden_pd["parent_company_name"].fillna("NA").tolist()

def match_parent_company(iterator):
    # Set cache directory inside UDF
    import os
    os.environ['HF_HOME'] = '/tmp/huggingface_cache'
    os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface_cache'
    
    # Load model inside UDF to avoid serialization
    from sentence_transformers import SentenceTransformer
    import numpy as np
    
    model = SentenceTransformer('all-MiniLM-L6-v2')
    # Use golden_names and golden_parents from outer scope
    golden_embeddings = model.encode(golden_names, show_progress_bar=False)
    
    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            # Use translated name if available, else original
            query_name = row.get("corporate_name_en") or row.get("corporate_name") or ""
            query_emb = model.encode([query_name])[0]
            # Compute cosine similarities
            sims = np.dot(golden_embeddings, query_emb) / (
                np.linalg.norm(golden_embeddings, axis=1) * np.linalg.norm(query_emb) + 1e-10
            )
            # Find best match above threshold
            best_idx = np.argmax(sims)
            best_score = sims[best_idx]
            threshold = 0.85
            if best_score >= threshold:
                parent_company = golden_parents[best_idx]
                match_status = "matched"
            else:
                parent_company = "NA"
                match_status = "not_matched"
            result_row = row.to_dict()
            result_row["parent_company_name"] = parent_company
            result_row["match_status"] = match_status
            results.append(result_row)
        yield pd.DataFrame(results)

# Add parent_company_name and match_status using mapInPandas
output_schema = translated.schema.add("parent_company_name", "string").add("match_status", "string")
matched_all = translated.mapInPandas(match_parent_company, schema=output_schema)

# Split into matched and not matched DataFrames
matched_df = matched_all.filter("match_status = 'matched'")
not_matched_df = matched_all.filter("match_status = 'not_matched'")

print(f"✓ Matched: {matched_df.count()} records")
print(f"✓ Not matched: {not_matched_df.count()} records")

# Drop and create temp tables for matched and not matched
spark.sql("DROP TABLE IF EXISTS tamr_poc_ref.data_source.temp_match")
matched_df.write.mode("overwrite").saveAsTable("tamr_poc_ref.data_source.temp_match")

spark.sql("DROP TABLE IF EXISTS tamr_poc_ref.data_source.temp_not_match")
not_matched_df.write.mode("overwrite").saveAsTable("tamr_poc_ref.data_source.temp_not_match")

display(matched_df)
display(not_matched_df)

# COMMAND ----------

# TEMPORARY - Replace with your actual key for testing
openai.api_key = "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"

# COMMAND ----------

import openai

openai.api_base = "https://openrouter.ai/api/v1"
openai.api_key = openai.api_key  # Replace with your OpenRouter key

corp_name = "BCD TRAVEL HONG KONG LIMITED"
city = "HONG KONG"
country = "HK"

query = f"""Find the top 5 most likely parent companies for '{corp_name}' located in {city}, {country}.

For each parent company, provide:
1. The parent company name
2. A confidence score between 0.0 and 1.0

Format your response as:
1. ParentCompanyName1 (score: 0.X)
2. ParentCompanyName2 (score: 0.X)
..."""

try:
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": query}],
        max_tokens=300,
        temperature=0.3
    )
    print("API call successful!")
    print(f"Response: {response.choices[0].message.content}")
except Exception as e:
    print(f"API call failed: {type(e).__name__}: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Cell 9
# Read matched and not matched data from temp tables
matched_df = spark.table("tamr_poc_ref.data_source.temp_match")
not_matched_df = spark.table("tamr_poc_ref.data_source.temp_not_match")

import pandas as pd
import re
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

API_KEY = "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"

def gpt_search_parent_company_denorm(iterator):
    import openai
    import re
    openai.api_base = "https://openrouter.ai/api/v1"
    openai.api_key = API_KEY
    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            corp_name = row['corporate_name']
            city = row.get('name_of_the_city', '') or ''
            country = row.get('country', '') or ''
            query = f"""Find the top 5 most likely parent companies for '{corp_name}' located in {city}, {country}.
For each parent company, provide:
1. The parent company name
2. A confidence score between 0.0 and 1.0
Format your response as:
1. ParentCompanyName1 (score: 0.X)
2. ParentCompanyName2 (score: 0.X)
..."""
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": query}],
                    max_tokens=300,
                    temperature=0.3
                )
                text = response.choices[0].message.content
                matches = []
                pattern = r'(?:\d+\.)?\s*([^(\n]+?)\s*(?:\(score:\s*|[-:]\s*)?([0-1]\.[0-9]+|[0-1])'
                for match in re.finditer(pattern, text, re.IGNORECASE):
                    company = match.group(1).strip()
                    score_str = match.group(2)
                    try:
                        score = float(score_str)
                        matches.append({
                            "parent_company_name": company,
                            "confidence_score": score
                        })
                    except:
                        continue
                if not matches:
                    for line in text.split('\n'):
                        line = line.strip()
                        if line and len(line) > 3:
                            clean_line = re.sub(r'^\d+\.\s*', '', line)
                            if clean_line:
                                matches.append({
                                    "parent_company_name": clean_line[:100],
                                    "confidence_score": 0.5
                                })
                matches = matches[:5]
                while len(matches) < 5:
                    matches.append({"parent_company_name": "NA", "confidence_score": 0.0})
            except Exception as e:
                matches = [{"parent_company_name": "NA", "confidence_score": 0.0}] * 5
            # Denormalize: yield 5 rows per supplier, rank 1 (best) to 5 (least)
            for idx, m in enumerate(matches):
                result_row = row.to_dict()
                # Update parent_company_name instead of adding it
                result_row["parent_company_name"] = m["parent_company_name"]
                result_row["confidence_score"] = m["confidence_score"]
                result_row["rank"] = idx + 1
                results.append(result_row)
        yield pd.DataFrame(results)

# Denormalize matched_df: 1 row per supplier, rank=1, confidence_score=1.0
def denorm_matched(iterator):
    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            result_row = row.to_dict()
            result_row["confidence_score"] = 1.0
            result_row["rank"] = 1
            results.append(result_row)
        yield pd.DataFrame(results)

# Define schema for denormalized output - only add NEW columns
denorm_schema = matched_df.schema \
    .add(StructField("confidence_score", FloatType(), True)) \
    .add(StructField("rank", IntegerType(), True))

search_schema = not_matched_df.schema \
    .add(StructField("confidence_score", FloatType(), True)) \
    .add(StructField("rank", IntegerType(), True))

# Process matched and not matched separately
matched_denorm = matched_df.mapInPandas(denorm_matched, schema=denorm_schema)
searched_denorm = not_matched_df.mapInPandas(gpt_search_parent_company_denorm, schema=search_schema)

# Combine
final_denorm = matched_denorm.unionByName(searched_denorm)

# Store
final_denorm.write.mode("overwrite").saveAsTable("tamr_poc_ref.data_source.supplier_matched_with_ai_denorm")

print(f"✓ Total rows (denormalized): {final_denorm.count()}")
display(final_denorm)
