CREATE TABLE IF NOT EXISTS agustinahermelo_coderhouse.stocksinfo (
                Ticker VARCHAR(10),
                date DATE,
                minute TIME,
                label VARCHAR(10),
                high DECIMAL(10, 3),
                low DECIMAL(10, 3),
                open_price DECIMAL(10, 3),
                close_price DECIMAL(10, 3),
                average DECIMAL(10, 3),
                volume DECIMAL(15, 1),
                notional DECIMAL(20, 1),
                numberOfTrades DECIMAL(15, 1)
            )
            DISTKEY (Ticker)
            SORTKEY (date, minute);

CREATE TABLE IF NOT EXISTS agustinahermelo_coderhouse.stocksinfo_stg (
                Ticker VARCHAR(10),
                date DATE,
                minute TIME,
                label VARCHAR(10),
                high DECIMAL(10, 3),
                low DECIMAL(10, 3),
                open_price DECIMAL(10, 3),
                close_price DECIMAL(10, 3),
                average DECIMAL(10, 3),
                volume DECIMAL(15, 1),
                notional DECIMAL(20, 1),
                numberOfTrades DECIMAL(15, 1)
            )
            DISTKEY (Ticker)
            SORTKEY (date, minute);