CREATE TABLE IF NOT EXISTS crypto_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    price DECIMAL,
    volume_24h DECIMAL,
    change_24h DECIMAL,
    bid_price DECIMAL,
    ask_price DECIMAL,
    high_24h DECIMAL,
    low_24h DECIMAL,
    quote_asset_volume_24h DECIMAL,
    number_of_trades_24h INT,
    retrieval_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Time of retrieval
);
