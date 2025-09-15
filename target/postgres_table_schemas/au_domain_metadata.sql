CREATE TABLE au_domain_metadata (
    id SERIAL PRIMARY KEY,
    domain_id BIGINT NOT NULL REFERENCES au_entity_domains(id) ON DELETE CASCADE,  
    url TEXT NOT NULL,                 
    title TEXT,
    description TEXT,
    keywords TEXT,
    og_title TEXT,
    og_description TEXT,
    og_site_name TEXT,
    twitter_title TEXT,
    twitter_description TEXT,
    canonical TEXT,
    h1 TEXT,
    language VARCHAR(10),             
    record_last_updated TIMESTAMP DEFAULT now()
);

-- Indexes
-- Fast lookup by domain
CREATE INDEX idx_domain_metadata_domain_id ON au_domain_metadata (domain_id);

-- Prevent duplicate URLs per domain
CREATE UNIQUE INDEX idx_domain_metadata_domain_url ON au_domain_metadata (domain_id, url);


-- Optional: trigram search on URL (partial matches)
CREATE INDEX idx_domain_metadata_url_trgm ON au_domain_metadata USING gin (url gin_trgm_ops);

-- Cluster metadata by domain_id for better locality
CLUSTER au_domain_metadata USING idx_domain_metadata_domain_id;
