%md
# Supplier Parent Company Matching System - Technical Documentation

## Overview
This POC implements an automated supplier-to-parent-company matching system using a **hybrid AI approach** combining semantic similarity and generative AI.

---

## Architecture & Workflow

```
┌─────────────────────┐
│ Stage 1: Input Data │ → Supplier records from various sources
└──────────┬──────────┘
           │
┌──────────▼──────────────┐
│ Stage 2: Golden Source  │ → Reference data with known parent companies
└──────────┬──────────────┘
           │
┌──────────▼──────────────┐
│ Stage 3: Translation    │ → Translate to English using Google Translate
│  (langdetect + googletrans) │
└──────────┬──────────────┘
           │
┌──────────▼──────────────────────┐
│ Stage 4: Semantic Matching      │ → SBERT (Sentence-BERT) embeddings
│  Model: all-MiniLM-L6-v2        │ → Cosine similarity matching
│  Threshold: 0.85                │
└──────────┬──────────────────────┘
           │
      ┌────▼────┐
      │ Matched? │
      └─┬────┬──┘
        │    │
     YES│    │NO
        │    │
        │    └─────────────────────────┐
        │                              │
        │                     ┌────────▼─────────────────┐
        │                     │ Stage 5: GPT-3.5 Search  │
        │                     │  via OpenRouter API      │
        │                     │  Top 5 candidates        │
        │                     └────────┬─────────────────┘
        │                              │
        └──────────┬───────────────────┘
                   │
         ┌─────────▼──────────┐
         │  Final Output      │
         │  (Denormalized)    │
         └────────────────────┘
```

---

## Technical Components

### 1. **Data Sources**

**Input Table:** `tamr_poc_ref.data_source.supplier_input_to_tamr`
- Contains supplier records without parent company assignments
- Fields: corporate_name, trade_name, legal_form, city, country, etc.

**Golden Source Table:** `tamr_poc_ref.data_source.supplier_golden_source`
- Reference dataset with known parent companies
- Used as ground truth for semantic matching

```sql
-- Example structure
CREATE TABLE supplier_input_to_tamr (
    corporate_name VARCHAR(255),
    trade_name VARCHAR(255),
    name_of_the_city VARCHAR(255),
    country VARCHAR(255),
    ...
);
```

---

## ML & AI Models Used

### **Model 1: Language Detection (langdetect)**
- **Purpose:** Detect source language before translation
- **Method:** Statistical analysis using character n-grams
- **Output:** ISO 639-1 language code (e.g., 'en', 'fr', 'de')

```python
from langdetect import detect

lang = detect("Société Générale")  # Returns: 'fr'
```

---

### **Model 2: Google Translate API (googletrans)**
- **Purpose:** Translate non-English supplier names to English
- **API:** Unofficial Google Translate API wrapper
- **Batch Processing:** Processed via Spark `mapInPandas` for scalability

```python
from googletrans import Translator

translator = Translator()
result = translator.translate("Société Générale", src='fr', dest='en')
print(result.text)  # Output: "General Society"
```

**Why Translation?**
- Standardizes names across languages
- Improves embedding quality for non-English text
- Reduces false negatives in matching

---

### **Model 3: Sentence-BERT (all-MiniLM-L6-v2)**

**Type:** Semantic Similarity Model (Transformer-based)

**Architecture:**
- Base: MiniLM (distilled BERT variant)
- Training: Siamese/Triplet networks for sentence embeddings
- Output: 384-dimensional dense vectors

**Technical Specifications:**
- Parameters: \~23 million
- Speed: \~2,800 sentences/second (CPU)
- Use Case: Semantic search, duplicate detection, clustering

**How It Works:**
```python
from sentence_transformers import SentenceTransformer
import numpy as np

model = SentenceTransformer('all-MiniLM-L6-v2')

# Generate embeddings
name1 = "BCD TRAVEL HONG KONG LIMITED"
name2 = "BCD TRAVEL ASIA PACIFIC PTE LTD"

emb1 = model.encode([name1])[0]  # Shape: (384,)
emb2 = model.encode([name2])[0]

# Cosine similarity
similarity = np.dot(emb1, emb2) / (np.linalg.norm(emb1) * np.linalg.norm(emb2))
print(f"Similarity: {similarity:.3f}")  # Output: 0.923
```

**Matching Logic:**
```python
threshold = 0.85
if similarity >= threshold:
    match_status = "matched"
    parent_company = golden_source_parent[best_match_idx]
else:
    match_status = "not_matched"
    # Send to GPT for fallback
```

**Why SBERT?**
- Captures semantic meaning ("BCD Travel" ≈ "BCD Meetings")
- Fast computation (pre-computed golden source embeddings)
- Language-agnostic after translation

---

### **Model 4: GPT-3.5-Turbo (via OpenRouter)**

**Type:** Large Language Model (LLM) - Generative AI

**Purpose:** Fallback for unmatched suppliers
- Leverages world knowledge about companies
- Infers parent-subsidiary relationships
- Provides confidence scores

**API Configuration:**
```python
import openai

openai.api_base = "https://openrouter.ai/api/v1"
openai.api_key = "your-api-key"

response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[{"role": "user", "content": query}],
    max_tokens=300,
    temperature=0.3  # Lower = more deterministic
)
```

**Prompt Engineering:**
```python
query = f"""
Find the top 5 most likely parent companies for '{corp_name}' 
located in {city}, {country}.

For each parent company, provide:
1. The parent company name
2. A confidence score between 0.0 and 1.0

Format your response as:
1. ParentCompanyName1 (score: 0.X)
2. ParentCompanyName2 (score: 0.X)
...
"""
```

**Output Processing:**
- Regex parsing to extract company names and scores
- Returns top 5 candidates per supplier
- Denormalized format: 5 rows per supplier (rank 1-5)

**Why GPT?**
- Handles cases where golden source lacks reference data
- Uses contextual information (city, country)
- Provides ranked alternatives with confidence

---

## Key Technical Terms

### **Embeddings**
Dense vector representations of text that capture semantic meaning. Similar texts have similar embeddings in high-dimensional space.

### **Cosine Similarity**
Measures similarity between two vectors based on the cosine of the angle between them:
```
cos(θ) = (A · B) / (||A|| × ||B||)
```
Range: -1 (opposite) to 1 (identical)

### **mapInPandas (Spark)**
Distributed processing method that applies a Pandas UDF to partitions of a Spark DataFrame:
```python
df.mapInPandas(function, schema=output_schema)
```
- Enables Python library usage (googletrans, sentence-transformers) in distributed context
- Serverless-compatible (vs. mapPartitions)

### **Denormalization**
Data structure where one input row produces multiple output rows:
- **Matched:** 1 supplier → 1 row (rank=1, score=1.0)
- **Unmatched:** 1 supplier → 5 rows (rank=1-5, varying scores)

### **Temperature (LLM Parameter)**
Controls randomness in GPT responses:
- **0.0:** Deterministic (always same output)
- **0.3:** Low variability (used here for consistency)
- **1.0:** High creativity

---

## Performance Considerations

### **SBERT Processing**
- Golden source embeddings computed once (\~50 records)
- Input embeddings computed per batch
- Cosine similarity: O(n) per query

### **GPT API Calls**
- Rate limits apply (OpenRouter tier-dependent)
- Batched processing via `mapInPandas`
- Timeout/retry logic recommended for production

### **Spark Optimization**
```python
# Cache intermediate results
translated.cache()

# Repartition for parallel processing
df.repartition(10).mapInPandas(...)
```

---

## Output Schema

**Final Table:** `tamr_poc_ref.data_source.supplier_matched_with_ai_denorm`

| Column | Type | Description |
|--------|------|-------------|
| corporate_name | STRING | Original supplier name |
| corporate_name_en | STRING | Translated name |
| parent_company_name | STRING | Matched parent company |
| confidence_score | FLOAT | Match confidence (0.0-1.0) |
| rank | INT | Ranking (1=best, 5=worst) |
| match_status | STRING | 'matched' or 'not_matched' |
| name_of_the_city | STRING | Location data |
| country | STRING | Country code |

---

## Threshold Tuning

**Current:** `threshold = 0.85` (strict matching)

**Impact:**
- **Higher (0.90+):** Fewer false positives, more GPT calls
- **Lower (0.75):** More SBERT matches, risk of false positives

**Recommended Approach:**
```python
# Precision-Recall analysis
for threshold in [0.75, 0.80, 0.85, 0.90]:
    evaluate_matches(threshold)
```

---

## Error Handling

```python
try:
    response = openai.ChatCompletion.create(...)
except openai.error.RateLimitError:
    # Implement exponential backoff
    time.sleep(2 ** retry_count)
except Exception as e:
    # Fallback: return NA with score 0.0
    matches = [{"parent_company_name": "NA", "confidence_score": 0.0}] * 5
```

---

## References

- **Sentence-BERT Paper:** [arxiv.org/abs/1908.10084](https://arxiv.org/abs/1908.10084)
- **all-MiniLM-L6-v2:** HuggingFace Model Card
- **GPT-3.5:** OpenAI Documentation
- **OpenRouter:** [openrouter.ai/docs](https://openrouter.ai/docs)


%md
# Step-by-Step Implementation Guide

## Stage 1: Create Input Data Table

**Objective:** Set up the source data table with supplier records

```sql
DROP TABLE IF EXISTS tamr_poc_ref.data_source.supplier_input_to_tamr;

CREATE TABLE tamr_poc_ref.data_source.supplier_input_to_tamr (
    corporate_name VARCHAR(255),
    trade_name VARCHAR(255),
    legal_form VARCHAR(255),
    box_zip_code VARCHAR(50),
    name_of_the_city VARCHAR(255),
    state_region VARCHAR(255),
    country VARCHAR(255),
    address_1 VARCHAR(255),
    tech_file_name VARCHAR(255)
);

INSERT INTO tamr_poc_ref.data_source.supplier_input_to_tamr 
VALUES 
    ('BCD TRAVEL HONG KONG LIMITED', NULL, NULL, NULL, 'HONG KONG', NULL, 'HK', NULL, 'FCA81225.CSV'),
    ('OBJETRAMA', 'SAS', 'SAS', NULL, 'MUNDOLSHEIM CEDEX', NULL, 'FR', NULL, 'FAA11219.CSV'),
    -- Add more records...
```

**Key Points:**
- Stores raw supplier data from multiple file sources
- No parent company information initially
- Country codes follow ISO 3166-1 alpha-2 standard

---

## Stage 2: Create Golden Source Table

**Objective:** Build reference dataset with known parent companies

```sql
CREATE TABLE tamr_poc_ref.data_source.supplier_golden_source (
    corporate_name VARCHAR(255),
    trade_name VARCHAR(255),
    legal_form VARCHAR(255),
    box_zip_code VARCHAR(50),
    name_of_the_city VARCHAR(255),
    state_region VARCHAR(255),
    country VARCHAR(255),
    parent_company_name VARCHAR(255),  -- Key addition!
    tech_file_name VARCHAR(255)
);

INSERT INTO tamr_poc_ref.data_source.supplier_golden_source 
VALUES 
    ('BCD TRAVEL HONG KONG LIMITED', NULL, NULL, NULL, 'HONG KONG', NULL, 'HK', 'BCD TRAVEL', 'FCA81225.CSV'),
    ('OBJETRAMA', 'SAS', 'SAS', NULL, 'MUNDOLSHEIM CEDEX', NULL, 'FR', 'OBJETRAMA', 'FAA11219.CSV'),
    -- Add more records...
```

**Key Points:**
- Contains `parent_company_name` column (the ground truth)
- Used as the reference corpus for SBERT matching
- Should cover major parent companies in your domain

---

## Stage 3: Install Python Packages

**Objective:** Install required libraries for translation and embeddings

```python
%pip install --upgrade \
    googletrans==4.0.0rc1 \
    langdetect \
    sentence-transformers \
    "huggingface-hub>=0.34.0,<1.0" \
    openai

dbutils.library.restartPython()  # Restart to load new packages
```

**Package Breakdown:**
- **googletrans:** Google Translate API wrapper
- **langdetect:** Language detection library
- **sentence-transformers:** SBERT models from HuggingFace
- **huggingface-hub:** Download and cache transformer models
- **openai:** OpenAI/OpenRouter API client

**Important:** Always restart Python after pip install in Databricks

---

## Stage 4: Translate to English

**Objective:** Normalize all supplier names to English for consistent embedding

```python
from googletrans import Translator
from langdetect import detect
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

def translate_batch(iterator):
    """
    Pandas UDF to translate corporate and trade names to English
    Runs in parallel across Spark partitions
    """
    translator = Translator()
    
    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            corp_name = row['corporate_name']
            trade_name = row['trade_name']
            
            # Translate corporate name
            if corp_name and isinstance(corp_name, str) and corp_name.strip():
                try:
                    corp_lang = detect(corp_name)  # Detect language
                    if corp_lang != 'en':
                        corp_trans = translator.translate(
                            corp_name, 
                            src=corp_lang, 
                            dest='en'
                        ).text
                    else:
                        corp_trans = corp_name  # Already English
                except Exception as e:
                    corp_trans = corp_name  # Fallback to original
            else:
                corp_trans = corp_name
            
            # Translate trade name (same logic)
            if trade_name and isinstance(trade_name, str) and trade_name.strip():
                try:
                    trade_lang = detect(trade_name)
                    if trade_lang != 'en':
                        trade_trans = translator.translate(
                            trade_name, 
                            src=trade_lang, 
                            dest='en'
                        ).text
                    else:
                        trade_trans = trade_name
                except Exception as e:
                    trade_trans = trade_name
            else:
                trade_trans = trade_name
            
            # Build result row with new columns
            result_row = row.to_dict()
            result_row['corporate_name_en'] = corp_trans
            result_row['trade_name_en'] = trade_trans
            results.append(result_row)
        
        yield pd.DataFrame(results)

# Read input data
df = spark.table("tamr_poc_ref.data_source.supplier_input_to_tamr")

# Define output schema (original + 2 new columns)
output_schema = df.schema \
    .add(StructField("corporate_name_en", StringType(), True)) \
    .add(StructField("trade_name_en", StringType(), True))

# Apply translation in parallel
translated = df.mapInPandas(translate_batch, schema=output_schema)

display(translated)
```

**Technical Details:**
- **mapInPandas:** Distributes translation across Spark workers
- **Language Detection:** Avoids unnecessary translation for English text
- **Error Handling:** Falls back to original text if translation fails
- **Batch Processing:** Processes DataFrame partitions in parallel

---

## Stage 5: Semantic Matching with SBERT

**Objective:** Match suppliers to parent companies using sentence embeddings

```python
import numpy as np
import pandas as pd
import os
from pyspark.sql.types import StructType, StructField, StringType
from sentence_transformers import SentenceTransformer

# Set cache directory for model downloads
os.environ['HF_HOME'] = '/tmp/huggingface_cache'
os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface_cache'

# Load golden source as Pandas DataFrame
golden_df = spark.table("tamr_poc_ref.data_source.supplier_golden_source")
golden_pd = golden_df.select("corporate_name", "parent_company_name").toPandas()

golden_names = golden_pd["corporate_name"].fillna("").tolist()
golden_parents = golden_pd["parent_company_name"].fillna("NA").tolist()

def match_parent_company(iterator):
    """
    Pandas UDF to find best matching parent company using SBERT
    """
    # Set cache inside UDF
    import os
    os.environ['HF_HOME'] = '/tmp/huggingface_cache'
    os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface_cache'
    
    from sentence_transformers import SentenceTransformer
    import numpy as np
    
    # Load SBERT model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Pre-compute golden source embeddings (once per partition)
    golden_embeddings = model.encode(golden_names, show_progress_bar=False)
    
    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            # Use translated name if available
            query_name = row.get("corporate_name_en") or row.get("corporate_name") or ""
            
            # Generate query embedding
            query_emb = model.encode([query_name])[0]
            
            # Compute cosine similarities
            sims = np.dot(golden_embeddings, query_emb) / (
                np.linalg.norm(golden_embeddings, axis=1) * np.linalg.norm(query_emb) + 1e-10
            )
            
            # Find best match
            best_idx = np.argmax(sims)
            best_score = sims[best_idx]
            
            # Apply threshold
            threshold = 0.85
            if best_score >= threshold:
                parent_company = golden_parents[best_idx]
                match_status = "matched"
            else:
                parent_company = "NA"
                match_status = "not_matched"
            
            # Build result
            result_row = row.to_dict()
            result_row["parent_company_name"] = parent_company
            result_row["match_status"] = match_status
            results.append(result_row)
        
        yield pd.DataFrame(results)

# Apply matching
output_schema = translated.schema \
    .add("parent_company_name", "string") \
    .add("match_status", "string")

matched_all = translated.mapInPandas(match_parent_company, schema=output_schema)

# Split results
matched_df = matched_all.filter("match_status = 'matched'")
not_matched_df = matched_all.filter("match_status = 'not_matched'")

print(f"✓ Matched: {matched_df.count()} records")
print(f"✓ Not matched: {not_matched_df.count()} records")

# Save to temp tables
matched_df.write.mode("overwrite").saveAsTable("tamr_poc_ref.data_source.temp_match")
not_matched_df.write.mode("overwrite").saveAsTable("tamr_poc_ref.data_source.temp_not_match")
```

**Mathematical Foundation:**

**Cosine Similarity Formula:**
```
similarity = (A · B) / (||A|| × ||B||)

Where:
  A · B = dot product of embeddings
  ||A|| = L2 norm (magnitude) of vector A
  ||B|| = L2 norm of vector B
```

**Example Similarity Scores:**
- "BCD TRAVEL HONG KONG" vs "BCD TRAVEL ASIA PACIFIC": 0.92 ✓ (matched)
- "OBJETRAMA" vs "DERICHEBOURG": 0.31 ✗ (not matched)
- "PWC ENTERPRISES" vs "PRICEWATERHOUSECOOPERS": 0.88 ✓ (matched)

**Threshold Selection:**
- **0.85:** Balances precision and recall
- **Higher:** Fewer false positives, more GPT fallback
- **Lower:** More SBERT matches, risk of incorrect matches

---

## Stage 6: GPT-3.5 Fallback for Unmatched Records

**Objective:** Use LLM to find parent companies for records that failed SBERT matching

### Step 6a: Configure API

```python
import openai

openai.api_base = "https://openrouter.ai/api/v1"
openai.api_key = "sk-or-v1-YOUR-API-KEY-HERE"  # Replace with your key
```

### Step 6b: Test API Call (Single Record)

```python
corp_name = "BCD TRAVEL HONG KONG LIMITED"
city = "HONG KONG"
country = "HK"

query = f"""
Find the top 5 most likely parent companies for '{corp_name}' 
located in {city}, {country}.

For each parent company, provide:
1. The parent company name
2. A confidence score between 0.0 and 1.0

Format your response as:
1. ParentCompanyName1 (score: 0.X)
2. ParentCompanyName2 (score: 0.X)
...
"""

try:
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": query}],
        max_tokens=300,
        temperature=0.3  # Lower = more consistent
    )
    print("API call successful!")
    print(f"Response: {response.choices[0].message.content}")
except Exception as e:
    print(f"API call failed: {type(e).__name__}: {str(e)}")
```

**Expected Output:**
```
API call successful!
Response: 
1. BCD Travel (score: 0.95)
2. BCD Meetings & Events (score: 0.85)
3. Advito (score: 0.75)
4. TripActions (score: 0.40)
5. American Express Global Business Travel (score: 0.35)
```

### Step 6c: Batch Processing with Denormalization

```python
import pandas as pd
import re
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

API_KEY = "sk-or-v1-YOUR-API-KEY-HERE"

def gpt_search_parent_company_denorm(iterator):
    """
    Pandas UDF to query GPT for parent companies
    Returns 5 rows per supplier (denormalized)
    """
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
            
            # Build query
            query = f"""
Find the top 5 most likely parent companies for '{corp_name}' 
located in {city}, {country}.

For each parent company, provide:
1. The parent company name
2. A confidence score between 0.0 and 1.0

Format your response as:
1. ParentCompanyName1 (score: 0.X)
2. ParentCompanyName2 (score: 0.X)
...
"""
            
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": query}],
                    max_tokens=300,
                    temperature=0.3
                )
                text = response.choices[0].message.content
                
                # Parse response with regex
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
                
                # Ensure 5 results
                matches = matches[:5]
                while len(matches) < 5:
                    matches.append({
                        "parent_company_name": "NA", 
                        "confidence_score": 0.0
                    })
                
            except Exception as e:
                # Fallback on error
                matches = [
                    {"parent_company_name": "NA", "confidence_score": 0.0}
                ] * 5
            
            # Denormalize: create 5 rows per supplier
            for idx, m in enumerate(matches):
                result_row = row.to_dict()
                result_row["parent_company_name"] = m["parent_company_name"]
                result_row["confidence_score"] = m["confidence_score"]
                result_row["rank"] = idx + 1  # 1 = best, 5 = worst
                results.append(result_row)
        
        yield pd.DataFrame(results)

# Load temp tables
matched_df = spark.table("tamr_poc_ref.data_source.temp_match")
not_matched_df = spark.table("tamr_poc_ref.data_source.temp_not_match")

# Process matched records (add confidence_score=1.0, rank=1)
def denorm_matched(iterator):
    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            result_row = row.to_dict()
            result_row["confidence_score"] = 1.0
            result_row["rank"] = 1
            results.append(result_row)
        yield pd.DataFrame(results)

# Define schemas
denorm_schema = matched_df.schema \
    .add(StructField("confidence_score", FloatType(), True)) \
    .add(StructField("rank", IntegerType(), True))

search_schema = not_matched_df.schema \
    .add(StructField("confidence_score", FloatType(), True)) \
    .add(StructField("rank", IntegerType(), True))

# Process both sets
matched_denorm = matched_df.mapInPandas(denorm_matched, schema=denorm_schema)
searched_denorm = not_matched_df.mapInPandas(gpt_search_parent_company_denorm, schema=search_schema)

# Combine
final_denorm = matched_denorm.unionByName(searched_denorm)

# Save final output
final_denorm.write.mode("overwrite").saveAsTable(
    "tamr_poc_ref.data_source.supplier_matched_with_ai_denorm"
)

print(f"✓ Total rows (denormalized): {final_denorm.count()}")
display(final_denorm)
```

**Output Structure (Denormalized):**

| corporate_name | parent_company_name | confidence_score | rank | match_status |
|----------------|---------------------|------------------|------|---------------|
| FLORAFLORE | FLORAFLORE | 1.0 | 1 | matched |
| TROPPER DATA | Tropper AG | 0.92 | 1 | not_matched |
| TROPPER DATA | Tropper Group | 0.78 | 2 | not_matched |
| TROPPER DATA | Data Services Inc | 0.45 | 3 | not_matched |
| TROPPER DATA | NA | 0.0 | 4 | not_matched |
| TROPPER DATA | NA | 0.0 | 5 | not_matched |

**Denormalization Benefits:**
- Shows alternative matches for manual review
- Enables confidence-based filtering
- Supports ranking/sorting by score

---

## Complete Pipeline Summary

```python
# 1. Load data
input_df = spark.table("tamr_poc_ref.data_source.supplier_input_to_tamr")
golden_df = spark.table("tamr_poc_ref.data_source.supplier_golden_source")

# 2. Translate
translated = input_df.mapInPandas(translate_batch, schema=translation_schema)

# 3. SBERT matching
matched_all = translated.mapInPandas(match_parent_company, schema=match_schema)
matched = matched_all.filter("match_status = 'matched'")
not_matched = matched_all.filter("match_status = 'not_matched'")

# 4. GPT fallback for unmatched
matched_final = matched.mapInPandas(denorm_matched, schema=final_schema)
searched_final = not_matched.mapInPandas(gpt_search_parent_company_denorm, schema=final_schema)

# 5. Combine and save
final = matched_final.unionByName(searched_final)
final.write.mode("overwrite").saveAsTable("supplier_matched_with_ai_denorm")
```

---

## Query Examples for Analysis

### Get Best Matches Only
```sql
SELECT 
    corporate_name,
    parent_company_name,
    confidence_score,
    match_status
FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
WHERE rank = 1  -- Best match only
ORDER BY confidence_score DESC;
```

### High-Confidence Matches
```sql
SELECT *
FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
WHERE confidence_score >= 0.85
AND rank = 1;
```

### Review Low-Confidence Matches (Manual Review Needed)
```sql
SELECT 
    corporate_name,
    parent_company_name,
    confidence_score,
    rank
FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
WHERE confidence_score < 0.70
AND match_status = 'not_matched'
ORDER BY corporate_name, rank;
```

### Match Rate Analysis
```sql
SELECT 
    match_status,
    COUNT(DISTINCT corporate_name) as supplier_count,
    ROUND(COUNT(DISTINCT corporate_name) * 100.0 / SUM(COUNT(DISTINCT corporate_name)) OVER(), 2) as percentage
FROM tamr_poc_ref.data_source.supplier_matched_with_ai_denorm
WHERE rank = 1
GROUP BY match_status;
```

---

## Production Recommendations

### 1. **Add Retry Logic**
```python
import time

def call_gpt_with_retry(query, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = openai.ChatCompletion.create(...)
            return response
        except openai.error.RateLimitError:
            time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            if attempt == max_retries - 1:
                raise
    return None
```

### 2. **Cache Golden Source Embeddings**
```python
# Compute once, save to Delta table
golden_embeddings = model.encode(golden_names)
spark.createDataFrame(
    pd.DataFrame({
        'corporate_name': golden_names,
        'embedding': golden_embeddings.tolist()
    })
).write.mode("overwrite").saveAsTable("golden_embeddings_cache")
```

### 3. **Monitor API Costs**
```python
# Track GPT calls
total_api_calls = not_matched_df.count()
estimated_cost = total_api_calls * 0.0015  # GPT-3.5 pricing
print(f"Estimated OpenRouter cost: ${estimated_cost:.2f}")
```

### 4. **Add Logging**
```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Processing batch of {len(pdf)} records")
logger.warning(f"API call failed for {corp_name}: {str(e)}")
```

---

## Next Steps

1. **Tune Threshold:** Experiment with SBERT threshold values
2. **Enrich Golden Source:** Add more reference records
3. **Evaluate Accuracy:** Manual review of sample matches
4. **Optimize Performance:** Cache embeddings, batch GPT calls
5. **Add Monitoring:** Track match rates, API costs, latency