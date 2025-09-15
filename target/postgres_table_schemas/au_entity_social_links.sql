CREATE TABLE au_entity_social_links (
    id SERIAL PRIMARY KEY,
    domain_id BIGINT NOT NULL REFERENCES au_entity_domains(id) ON DELETE CASCADE,  -- match domains.id type
    platform VARCHAR(50) NOT NULL,   -- controlled string size, e.g. 'linkedin', 'facebook', 'twitter'
    url TEXT NOT NULL,
    record_last_updated TIMESTAMP DEFAULT now()
);

-- Index for fast joins
CREATE INDEX idx_social_links_domain_id ON au_entity_social_links (domain_id);

-- Prevent duplicate platform per domain
CREATE UNIQUE INDEX idx_social_links_domain_platform ON au_entity_social_links (domain_id, platform);

-- Optional: trigram index for partial URL searches
CREATE INDEX idx_social_links_url_trgm ON au_entity_social_links USING gin (url gin_trgm_ops);

-- Cluster by domain_id so all social links for a domain sit together
CLUSTER au_entity_social_links USING idx_social_links_domain_id;
