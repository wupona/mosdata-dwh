-- sql/010_create_stg_sm_stock_move_line_raw.sql
-- Staging RAW (append-only) partitionné par JOUR (retention 90 jours)

-- Optionnel si tu veux UUID default
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS core.stg_sm_stock_move_line CASCADE;

CREATE TABLE core.stg_sm_stock_move_line (
    sm_row_id              BIGSERIAL NOT NULL,
    -- partition key
    sm_part_day                DATE NOT NULL,

    -- Odoo identifiers
    sm_odoo_move_line_id       INT NOT NULL,
    sm_odoo_move_id            INT,
    sm_odoo_picking_id         INT,

    -- Odoo dates / state
    sm_odoo_date               TIMESTAMP,
    sm_odoo_write_date         TIMESTAMP NOT NULL,
    sm_odoo_state              TEXT,

    -- business fields
    sm_product_id_odoo         INT,
    sm_location_id_odoo        INT,
    sm_location_dest_id_odoo   INT,
    sm_qty_done                NUMERIC(16,4),
    sm_uom_name                TEXT,

    -- duplication control (no updates; set at insert time)
    -- Dup key based on 4 fields (stable + sufficient):
    -- move_line_id + write_date + qty_done + src/dst locations
    sm_dup_key                 TEXT NOT NULL,
    sm_dup_flag                SMALLINT NOT NULL DEFAULT 0,  -- 0=normal, 1=duplicate

    -- extract audit
    sm_extract_ts              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sm_source_file             TEXT,
    sm_payload                 JSONB,

    PRIMARY KEY (sm_row_id, sm_part_day)
) PARTITION BY RANGE (sm_part_day);

COMMENT ON TABLE core.stg_sm_stock_move_line IS
'RAW append-only staging for stock.move.line (done filter handled upstream). Partitioned by day. dup_flag is set at insert time (no UPDATE).';

COMMENT ON COLUMN core.stg_sm_stock_move_line.sm_dup_key IS
'Hash key for duplicate detection (loader checks recent partitions, sets dup_flag=1 if already seen).';

-- -------------------------------------------------------------------
-- Partition helper: create daily partitions for a date range
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.sp_crt_partitions_stg_stockmov(p_from date, p_to date)
LANGUAGE plpgsql
AS $$
DECLARE
    d date;
    p_name text;
BEGIN
    d := p_from;
    WHILE d <= p_to LOOP
        p_name := format('stg_sm_stock_move_line_%s', to_char(d, 'YYYYMMDD'));

        EXECUTE format($f$
            CREATE TABLE IF NOT EXISTS core.%I
            PARTITION OF core.stg_sm_stock_move_line
            FOR VALUES FROM (%L) TO (%L);
        $f$, p_name, d, d + 1);

        -- Indexes per partition (fast lookup in recent days)
        EXECUTE format($f$
            CREATE INDEX IF NOT EXISTS %I ON core.%I (sm_odoo_write_date);
        $f$, 'ix_'||p_name||'_write_date', p_name);

        EXECUTE format($f$
            CREATE INDEX IF NOT EXISTS %I ON core.%I (sm_dup_key);
        $f$, 'ix_'||p_name||'_dup_key', p_name);

        EXECUTE format($f$
            CREATE INDEX IF NOT EXISTS %I ON core.%I (sm_odoo_move_line_id);
        $f$, 'ix_'||p_name||'_move_line', p_name);

        d := d + 1;
    END LOOP;
END;
$$;

-- -------------------------------------------------------------------
-- Retention helper: drop partitions older than N days (default 90)
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.sp_drop_partitions_stg_stockmv(p_keep_days int DEFAULT 90)
LANGUAGE plpgsql
AS $$
DECLARE
    cutoff date := current_date - p_keep_days;
    r record;
BEGIN
    FOR r IN
        SELECT inhrelid::regclass AS part_name
        FROM pg_inherits
        WHERE inhparent = 'core.stg_sm_stock_move_line'::regclass
    LOOP
        -- Partition naming convention includes date suffix; we also rely on partition bounds by querying pg_class/pg_constraint is heavy.
        -- Simple approach: drop partitions whose name date <= cutoff (requires consistent naming).
        -- Example: stg_sm_stock_move_line_raw_20260103
        IF r.part_name::text ~ 'stg_sm_stock_move_line_[0-9]{8}$' THEN
            IF to_date(right(r.part_name::text, 8), 'YYYYMMDD') < cutoff THEN
                EXECUTE format('DROP TABLE IF EXISTS %s CASCADE;', r.part_name);
            END IF;
        END IF;
    END LOOP;
END;
$$;

-- -------------------------------------------------------------------
-- Create partitions for the next 14 days (adjust as you prefer)
-- -------------------------------------------------------------------
CALL core.sp_crt_partitions_stg_stockmov(current_date - 7, current_date + 14);
