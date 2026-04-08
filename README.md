Invoice Classification System - Technical Documentation
Executive Summary
This system implements an automated invoice classification pipeline using a two-tier AI/ML approach:

Tier 1: Semantic similarity matching using sentence transformers (NLP embeddings)
Tier 2: LLM-based classification using GPT-3.5-Turbo for unmatched records
System Architecture
Architecture Flow
Invoice Input Data → Sentence Transformer Matching → Matched Invoices
                                   ↓
                           Not Matched Invoices → GPT-3.5 Classification → Final Categorization
Data Tables
invoice_input: Raw invoice data to be classified
invoice_golden_source: Pre-classified reference invoices with known categories
category_mapping: Hierarchical taxonomy (Domain → Category → Subcategory)
invoice_not_matched: Invoices not matched by semantic similarity
invoice_not_matched_categorized: Final LLM-classified results
Step-by-Step Implementation
STEP 1: Data Preparation
1.1 Create Golden Source Table
Purpose: Reference dataset of pre-classified invoices for similarity matching

Technical Details:

20 sample invoices with confirmed classifications
Fields: sg_tech_id, source, description, invoice_label, supplier, suggested_classification_id, suggested_classification_path
CREATE TABLE tamr_poc_ref.data_source.invoice_golden_source (
    sg_tech_id STRING,
    classification_score INT,
    source STRING,
    description STRING,
    invoice_label STRING,
    supplier STRING,
    suggested_classification_id INT,
    suggested_classification_path STRING
);
1.2 Create Category Taxonomy
Purpose: Hierarchical classification structure (3 levels: Domain → Category → Subcategory)

Domains:

ICT (Information & Communications Technology)
BPS (Business Process Services)
Outside sourcing division scope
Example Categories: AIRLINES, FOOD SERVICES, UTILITIES, SECURITY SERVICES, SOFTWARE, TELECOM - VOICE

CREATE TABLE tamr_poc_ref.data_source.category_mapping (
    domain STRING,
    category STRING,
    subcategory STRING
);
1.3 Load Input Invoices
Purpose: New invoices requiring classification

CREATE TABLE tamr_poc_ref.data_source.invoice_input (
    sg_tech_id STRING,
    source STRING,
    description STRING,
    invoice_label STRING,
    supplier STRING
);
STEP 2: Install ML Dependencies
%pip install sentence-transformers
Package: sentence-transformers

Purpose: Provides pre-trained transformer models for semantic text embeddings
Use Case: Convert text to high-dimensional vectors for similarity comparison
STEP 3: Semantic Similarity Matching (Tier 1)
3.1 Technical Approach
Algorithm: Cosine Similarity on Sentence Embeddings

Model: all-MiniLM-L6-v2

Type: Sentence-BERT (Bidirectional Encoder Representations from Transformers)
Architecture: MiniLM (distilled from BERT)
Dimensions: 384-dimensional dense vectors
Training: Contrastive learning on sentence pairs
Speed: ~14,200 sentences/second
Use Case: Lightweight semantic similarity for production
3.2 Implementation
from sentence_transformers import SentenceTransformer, util
from pyspark.sql import functions as F

# Load pre-trained model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Concatenate invoice fields for rich semantic representation
def concat_fields(df):
    return df.withColumn(
        "concat_text",
        F.lower(F.concat_ws(" ", 
            F.col("source"), 
            F.col("description"), 
            F.col("invoice_label"), 
            F.col("supplier")
        ))
    )

input_df = concat_fields(input_df)
golden_df = concat_fields(golden_df)
3.3 Embedding Generation
# Convert text to embeddings (384-dimensional vectors)
input_texts = [row.concat_text for row in input_df.collect()]
golden_texts = [row.concat_text for row in golden_df.collect()]

input_embeddings = model.encode(input_texts, convert_to_tensor=True)
golden_embeddings = model.encode(golden_texts, convert_to_tensor=True)
Technical Terms:

Embedding: Dense vector representation of text in continuous space
Tensor: Multi-dimensional array optimized for GPU computation
Semantic Space: Latent space where similar meanings have similar vectors
3.4 Similarity Computation
# Compute cosine similarity matrix
similarities = util.cos_sim(input_embeddings, golden_embeddings)

# Match each input to most similar golden record
for i, input_row in enumerate(input_df.collect()):
    best_match_idx = similarities[i].argmax().item()
    best_score = similarities[i][best_match_idx].item()
    
    if best_score > 0.8:  # Threshold: 80% similarity
        # Transfer classification from golden source
        ...
Cosine Similarity Formula:

similarity = (A · B) / (||A|| * ||B||)
Range: [-1, 1] (typically [0, 1] for text)
0.8 threshold: High confidence match (80% semantic overlap)
3.5 Classification Transfer
if best_score > 0.8:
    golden_row = golden_df.collect()[best_match_idx]
    matches.append({
        "sg_tech_id": input_row.sg_tech_id,
        "classification_score": int(best_score * 100),
        "suggested_classification_id": golden_row.suggested_classification_id,
        "suggested_classification_path": golden_row.suggested_classification_path
    })
else:
    not_matched.append(input_row)  # Send to Tier 2
STEP 4: LLM Classification (Tier 2)
4.1 Technical Approach
Model: GPT-3.5-Turbo via OpenRouter API

Provider: OpenAI (via OpenRouter proxy)
Model Type: Generative Pre-trained Transformer
Parameters: 175B (estimated)
Context Window: 4,096 tokens
Temperature: 0.2 (low = deterministic, high = creative)
Why GPT for Unmatched Records?

Reasoning: Can analyze supplier names to infer business type
Contextual Understanding: Handles ambiguous descriptions
Few-shot Learning: Works with category list in prompt
4.2 Distributed Processing with PySpark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def gpt_category_mapping(iterator):
    from openai import OpenAI
    
    # Initialize client
    client = OpenAI(
        api_key=API_KEY,
        base_url="https://openrouter.ai/api/v1"
    )
    
    for pdf in iterator:  # Process pandas DataFrame batches
        results = []
        for idx, row in pdf.iterrows():
            # Build prompt with invoice context
            query = f"""
            Given the following invoice details:
            Description: {row['description']}
            Invoice Label: {row['invoice_label']}
            Supplier: {row['supplier']}
            
            Available categories:
            {categories_text}
            
            Select the best matching category.
            """
            
            # Call GPT-3.5-Turbo
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": query}],
                max_tokens=150,
                temperature=0.2
            )
            
            # Parse structured response
            text = response.choices[0].message.content.strip()
            # Extract: id, category, subcategory, confidence
            ...
Technical Terms:

mapInPandas: PySpark function for distributed UDF processing
Iterator Pattern: Processes data in batches for efficiency
Temperature: Controls randomness (0=deterministic, 1=creative)
Max Tokens: Limits response length (cost optimization)
4.3 Response Parsing
for line in text.split('\n'):
    if ':' in line:
        key, value = line.split(':', 1)
        key = key.strip().lower()
        value = value.strip()
        
        if key == 'id':
            cat_id = int(re.search(r'\d+', value).group())
        elif key == 'category':
            cat = value
        elif key == 'subcategory':
            subcat = value
        elif key == 'confidence':
            score = int(re.search(r'\d+', value).group())

row["suggested_classification_path"] = f"{cat} > {subcat}"
row["suggested_classification_id"] = cat_id
row["classification_score"] = score
4.4 Save Results
final_df = not_matched_df.mapInPandas(gpt_category_mapping, schema=output_schema)
final_df.write.mode("overwrite").saveAsTable(
    "tamr_poc_ref.data_source.invoice_not_matched_categorized"
)
Key Technical Terms Glossary
Machine Learning
Sentence Transformers: Neural network models that map sentences to fixed-length vectors
Embedding: Dense numerical vector representing text in semantic space
Cosine Similarity: Measure of similarity between two vectors (angle-based)
Semantic Search: Finding similar items based on meaning, not keywords
Transfer Learning: Using pre-trained models for new tasks
Deep Learning Models
BERT (Bidirectional Encoder Representations from Transformers): Base architecture for understanding context
MiniLM: Distilled (compressed) version of BERT for efficiency
GPT (Generative Pre-trained Transformer): Large language model for text generation
Transformer: Attention-based neural network architecture
Natural Language Processing
Tokenization: Breaking text into units (words, subwords)
Contrastive Learning: Training approach that pulls similar items together
Zero-shot Classification: Classification without training examples
Few-shot Learning: Learning from few examples in prompt
API & Infrastructure
OpenRouter: API gateway for multiple LLM providers
Temperature: Sampling parameter controlling output randomness
Context Window: Maximum input text length (measured in tokens)
Token: Unit of text (~4 characters in English)
PySpark
mapInPandas: Apply pandas UDF across partitions
DataFrame Partitioning: Distributing data across cluster nodes
Lazy Evaluation: Operations execute only when action is called
Performance Characteristics
Tier 1 (Sentence Transformers)
Throughput: ~14,200 sentences/second (CPU)
Latency: <1ms per invoice
Accuracy: High for similar invoices (80%+ threshold)
Cost: $0 (local inference)
Tier 2 (GPT-3.5-Turbo)
Throughput: ~20 requests/second (API limit)
Latency: ~500ms per invoice
Accuracy: Higher for ambiguous cases
Cost: $0.0015 per 1K input tokens, $0.002 per 1K output tokens
Error Handling & Edge Cases
Low Similarity Scores: Records below 0.8 threshold → sent to Tier 2
API Failures: Gracefully caught with try/except → null classification
Parsing Errors: Regex extraction with fallbacks
Token Limits: Category list truncated to first 50 items
Future Enhancements
Active Learning: Add misclassified records to golden source
Fine-tuning: Train custom classifier on historical data
Confidence Calibration: Adjust thresholds based on validation
Batch Optimization: Cache embeddings, batch API calls
Monitoring: Track accuracy, latency, cost metrics
