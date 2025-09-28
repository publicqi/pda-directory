-- Two databases: pda-directory-blue and pda-directory-green
CREATE TABLE pda_registry (
    pda BLOB PRIMARY KEY,
    program_id BLOB NOT NULL,
    seed_count INTEGER NOT NULL,
    seed_bytes BLOB NOT NULL
) WITHOUT ROWID;

CREATE INDEX 
IF NOT EXISTS idx_pda_registry_program_pda
ON pda_registry(program_id, pda);

CREATE TABLE _table_counts (
    name TEXT PRIMARY KEY,
    n INTEGER NOT NULL,
    last_insert_ts INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TRIGGER pda_registry_ai AFTER INSERT ON pda_registry
BEGIN
  UPDATE _table_counts
  SET n = n + 1,
      last_insert_ts = CAST(strftime('%s','now') AS INTEGER)
  WHERE name = 'pda_registry';
END;

CREATE TRIGGER pda_registry_ad AFTER DELETE ON pda_registry
BEGIN
  UPDATE _table_counts
  SET n = n - 1
  WHERE name = 'pda_registry';
END;