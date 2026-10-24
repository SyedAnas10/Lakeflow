BEGIN TRANSACTION;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

GO

-- Running upgrade  -> 0001

CREATE TABLE ingestion_job (
    id VARCHAR(36) NOT NULL, 
    content_hash VARCHAR(64) NOT NULL, 
    source VARCHAR(100) NOT NULL, 
    filename NVARCHAR(255) NOT NULL, 
    landing_key NVARCHAR(1024) NOT NULL, 
    size_bytes BIGINT NOT NULL, 
    status VARCHAR(20) NOT NULL, 
    stage VARCHAR(50) NULL, 
    progress_pct INTEGER NOT NULL, 
    attempts INTEGER NOT NULL, 
    lease_until DATETIME NULL, 
    file_format VARCHAR(20) NULL, 
    encoding VARCHAR(40) NULL, 
    delimiter VARCHAR(4) NULL, 
    rows_raw INTEGER NULL, 
    rows_exact_duplicate INTEGER NULL, 
    rows_rejected INTEGER NULL, 
    rows_key_duplicate INTEGER NULL, 
    rows_valid INTEGER NULL, 
    replaced_chars INTEGER NULL, 
    error NVARCHAR(max) NULL, 
    created_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    updated_at DATETIMEOFFSET NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (id), 
    UNIQUE (content_hash)
);

GO

CREATE INDEX ix_ingestion_job_status ON ingestion_job (status);

GO

CREATE TABLE column_profile (
    id INTEGER NOT NULL IDENTITY, 
    job_id VARCHAR(36) NOT NULL, 
    column_name NVARCHAR(255) NOT NULL, 
    position INTEGER NOT NULL, 
    row_count INTEGER NOT NULL, 
    null_count INTEGER NOT NULL, 
    null_rate FLOAT NOT NULL, 
    distinct_approx INTEGER NOT NULL, 
    min_length INTEGER NULL, 
    max_length INTEGER NULL, 
    inferred_type VARCHAR(20) NOT NULL, 
    top_values NVARCHAR(max) NOT NULL, 
    top_patterns NVARCHAR(max) NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(job_id) REFERENCES ingestion_job (id) ON DELETE CASCADE, 
    CONSTRAINT uq_profile_job_column UNIQUE (job_id, column_name)
);

GO

CREATE INDEX ix_column_profile_job_id ON column_profile (job_id);

GO

INSERT INTO alembic_version (version_num) OUTPUT inserted.version_num VALUES ('0001');

GO

-- Running upgrade 0001 -> 0002

ALTER TABLE ingestion_job ADD declared_encoding VARCHAR(40) NULL;

GO

UPDATE alembic_version SET version_num='0002' WHERE alembic_version.version_num = '0001';

GO

COMMIT;

GO

