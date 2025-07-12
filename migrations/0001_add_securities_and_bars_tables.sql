CREATE TABLE securities (
    id UUID PRIMARY KEY,
    symbol VARCHAR(4) UNIQUE NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE bars (
    symbol_id VARCHAR(4) NOT NULL REFERENCES securities(symbol),
    ts TIMESTAMPTZ NOT NULL,
    o NUMERIC(10,4) NOT NULL,
    h NUMERIC(10,4) NOT NULL,
    l NUMERIC(10,4) NOT NULL,
    c NUMERIC(10,4) NOT NULL,
    v NUMERIC(10,4) NOT NULL,
    txns INTEGER NOT NULL,
    PRIMARY KEY (symbol_id, ts)
);
