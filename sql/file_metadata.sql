CREATE SCHEMA cityride_analytics;
CREATE TABLE cityride_analytics.file_metadata (
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

ALTER TABLE cityride_analytics.file_metadata
ADD CONSTRAINT file_metadata_unique UNIQUE (year, month, file_type);