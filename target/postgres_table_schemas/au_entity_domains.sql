
CREATE TABLE au_entity_domains (
    id SERIAL PRIMARY KEY,
    domain TEXT NOT NULL UNIQUE,
    abn BIGINT NOT NULL REFERENCES au_entities(abn) ON DELETE CASCADE,
    record_last_updated TIMESTAMP DEFAULT now()
);

-- Fast lookup by ABN (join/filter)
CREATE INDEX idx_domains_abn ON au_entity_domains (abn);

-- Optional: substring or fuzzy search on URL
CREATE INDEX idx_domains_url_trgm ON au_entity_domains USING gin (url gin_trgm_ops);
