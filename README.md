The project extracts business data from data.gov.au and matches it with business domains from the Common Crawl March 2025 index, using RapidFuzz for fuzzy matching. Processed CSVs can be loaded into a target Postgres database.

### Data Extraction

- **Business Data Source**: Download bulk datasets like the "ABN Bulk Extract" (in XML format) from data.gov.au's business dataset section.
- **Domain Data Source**: Retrieve the Common Crawl March 2025 domain-level index and Web Graph data (host and domain mappings are available in CC-MAIN-2025-13 via AWS S3 or Common Crawl's releases)

### Matching Process

- Clean and normalize business names to maximize matching accuracy.
- Use **RapidFuzz** in Python for scalable fuzzy matching between business names and crawled domains, leveraging vectorized operations or multiprocessing if handling millions of entries.
- Fine-tune match scores (e.g., partial_ratio, token_sort_ratio) with a flexible threshold to balance precision and recall per dataset.[7][3]

### Loading and Processing

- Output matched records as CSV files, made available in an S3 bucket for downstream ETL.
- Load processed business-domain mappings directly into a Postgres DB, using batch import utilities (e.g., `COPY`, `psycopg2`, or `SQLAlchemy` bulk inserts) for efficient performance. 

### Overall Workflow

1. **Extract**: Get business records (ABN, company name, etc.) and domain records (from Common Crawl).
2. **Transform**: Clean, match, and enrich using RapidFuzz.
3. **Load**: Write CSVs to S3, then import results to Postgres.

## DAG Workflow: Extract → Process → Domain Match → Load

```text
       Extract Phase (Parallel)
┌───────────────────────┐   ┌────────────────────┐
│ extract_common_crawl   │   │ extract_au_abr     │
└─────────────┬─────────┘   └─────────────┬──────┘
              │                         │
              └─────────────┬───────────┘
                            │
       Processing Phase (Parallel)
┌──────────────────────────┐   ┌────────────────────┐
│ process_common_crawl      │   │ process_au_abr     │
└─────────────┬────────────┘   └─────────────┬──────┘
              │                         │
              └─────────────┬───────────┘
                            │
              Domain Match Phase
┌──────────────────────────┐
│ domain_match.py          │
└─────────────┬────────────┘
              │
                Load Phase
┌──────────────────────────┐
│ load_postgres.py         │
└──────────────────────────┘


```

This approach ensures a scalable, reproducible workflow for linking Australian business entities with their possible web domains using open government and web-scale data.

