CREATE SCHEMA cityride_metadata;
CREATE TABLE cityride_metadata.file_metadata (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'MISSING',
    last_checked TIMESTAMP,
    uploaded_at TIMESTAMP,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

ALTER TABLE cityride_metadata.file_metadata
ADD CONSTRAINT file_metadata_unique UNIQUE (year, month, file_type);


-- for snowflake 
CREATE OR REPLACE TABLE cityride_metadata.file_metadata (
    id INT AUTOINCREMENT PRIMARY KEY,  -- SERIAL in Postgres → AUTOINCREMENT in Snowflake
    year INT NOT NULL,
    month INT NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'MISSING',
    last_checked TIMESTAMP_NTZ,        -- Snowflake uses TIMESTAMP_NTZ/TZ/LTZ (no timezone by default)
    uploaded_at TIMESTAMP_NTZ,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,  -- now() → CURRENT_TIMESTAMP
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Unique constraint
ALTER TABLE cityride_metadata.file_metadata
ADD CONSTRAINT file_metadata_unique UNIQUE (year, month, file_type);