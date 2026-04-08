# Databricks notebook source
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
