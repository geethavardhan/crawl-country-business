CREATE TABLE trading_names (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(20) NOT NULL REFERENCES au_entities(abn) ON DELETE CASCADE,
    trading_name TEXT NOT NULL
);
