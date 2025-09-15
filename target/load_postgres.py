import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
from datetime import datetime

# -------------------------
# Config
# -------------------------
S3_BUCKET = "your-bucket-name"
ENTITIES_KEY = "entities.csv"
DOMAINS_KEY = "domains.csv"
SCORED_KEY = "scored_links.csv"

DB_CONFIG = {
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypass",
    "host": "localhost",
    "port": 5432
}

# -------------------------
# Connect to Postgres
# -------------------------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# -------------------------
# Initialize S3 client
# -------------------------
s3 = boto3.client("s3")

# -------------------------
# Helper to read CSV in chunks
# -------------------------
def read_csv_s3(bucket, key, chunksize=50000):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'], chunksize=chunksize)

# -------------------------
# 1️⃣ Load au_entities + trading_names
# -------------------------
for chunk in read_csv_s3(S3_BUCKET, ENTITIES_KEY):
    # Entities
    entities_tuples = [
        (
            int(row["ABN"]),
            row["Entity_Name"],
            row["Entity_Type"],
            row["Entity_Type_Code"],
            row["ABN_Status"],
            row["ABN_Status_From"] if pd.notna(row["ABN_Status_From"]) else None,
            int(row["ASIC_Number"]) if pd.notna(row["ASIC_Number"]) else None,
            row["GST_Status"],
            row["GST_From"] if pd.notna(row["GST_From"]) else None,
            row["State"],
            row["Postcode"],
            row["Record_Last_Updated"] if pd.notna(row["Record_Last_Updated"]) else datetime.now()
        )
        for _, row in chunk.iterrows()
    ]

    execute_values(cur, """
        INSERT INTO au_entities(
            abn, entity_name, entity_type, entity_type_code, abn_status, abn_status_from,
            asic_number, gst_status, gst_from, state, postcode, record_last_updated
        )
        VALUES %s
        ON CONFLICT (abn) DO UPDATE SET
            entity_name = EXCLUDED.entity_name,
            record_last_updated = now()
    """, entities_tuples)

    # Trading names
    trading_tuples = []
    for _, row in chunk.iterrows():
        if pd.notna(row["Trading_Names"]):
            for tn in str(row["Trading_Names"]).split(";"):
                trading_tuples.append((int(row["ABN"]), tn.strip()))
    if trading_tuples:
        execute_values(cur, """
            INSERT INTO au_entity_trading_names (abn, trading_name)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, trading_tuples)

conn.commit()

# -------------------------
# 2️⃣ Load au_entity_domains + metadata + social_links
# -------------------------
for chunk in read_csv_s3(S3_BUCKET, DOMAINS_KEY):
    # Domains
    domain_tuples = [(row["domain"], int(row["abn"]), datetime.now()) for _, row in chunk.iterrows()]
    execute_values(cur, """
        INSERT INTO au_entity_domains(domain, abn, record_last_updated)
        VALUES %s
        ON CONFLICT (domain) DO UPDATE SET
            abn = EXCLUDED.abn,
            record_last_updated = now()
        RETURNING id, domain
    """, domain_tuples)

    # Map domain -> domain_id
    cur.execute("SELECT id, domain FROM au_entity_domains")
    domain_map = {row[1]: row[0] for row in cur.fetchall()}

    # Metadata
    metadata_tuples = []
    social_tuples = []
    for _, row in chunk.iterrows():
        domain_id = domain_map[row["domain"]]
        meta = json.loads(row["meta"].replace("'", '"')) if pd.notna(row["meta"]) else {}

        metadata_tuples.append((
            domain_id,
            row["url"],
            meta.get("title"),
            meta.get("description"),
            meta.get("keywords"),
            meta.get("og_title"),
            meta.get("og_description"),
            meta.get("og_site_name"),
            meta.get("twitter_title"),
            meta.get("twitter_description"),
            meta.get("canonical"),
            meta.get("h1"),
            meta.get("language"),
            datetime.now()
        ))

        # Social links
        for platform in ["linkedin", "facebook", "twitter", "instagram", "youtube"]:
            if meta.get(platform):
                social_tuples.append((domain_id, platform, meta[platform], datetime.now()))

    if metadata_tuples:
        execute_values(cur, """
            INSERT INTO au_domain_metadata(
                domain_id, url, title, description, keywords, og_title,
                og_description, og_site_name, twitter_title, twitter_description,
                canonical, h1, language, record_last_updated
            )
            VALUES %s
            ON CONFLICT (domain_id, url) DO UPDATE SET
                record_last_updated = now()
        """, metadata_tuples)

    if social_tuples:
        execute_values(cur, """
            INSERT INTO au_entity_social_links(domain_id, platform, url, record_last_updated)
            VALUES %s
            ON CONFLICT (domain_id, platform) DO UPDATE SET
                url = EXCLUDED.url,
                record_last_updated = now()
        """, social_tuples)

conn.commit()

# -------------------------
# 3️⃣ Load scored_links -> associate domains with trading names / ABNs
# -------------------------
for chunk in read_csv_s3(S3_BUCKET, SCORED_KEY):
    # Ensure domains exist
    for domain in chunk["domain"].unique():
        cur.execute("""
            INSERT INTO au_entity_domains(domain, abn, record_last_updated)
            VALUES (%s, %s, now())
            ON CONFLICT (domain) DO NOTHING
        """, (domain, int(chunk[chunk["domain"]==domain]["abn"].iloc[0])))

    # Map domain -> domain_id
    cur.execute("SELECT id, domain FROM au_entity_domains")
    domain_map = {row[1]: row[0] for row in cur.fetchall()}

    # Optional: insert into domain metadata / scores table if needed
    # For now we just update record_last_updated
    for _, row in chunk.iterrows():
        domain_id = domain_map[row["domain"]]
        cur.execute("""
            UPDATE au_domain_metadata
            SET record_last_updated = now()
            WHERE domain_id = %s AND url = %s
        """, (domain_id, row["url"]))

conn.commit()
cur.close()
conn.close()
