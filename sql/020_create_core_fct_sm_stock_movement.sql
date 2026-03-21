-- sql/020_create_core_fct_sm_stock_movement.sql
-- FACT Stock Movement (SM) - partition mensuelle - retention 12 mois

CREATE SCHEMA IF NOT EXISTS core;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS core.fct_sm_stock_movement CASCADE;

CREATE TABLE core.fct_sm_stock_movement (
    -- surrogate key
    sm_fct_id                 UUID NOT NULL DEFAULT gen_random_uuid(),

    -- partition key
    sm_movement_day           DATE NOT NULL,

    -- Odoo traceability
    sm_odoo_move_line_id      INT NOT NULL,
    sm_odoo_move_id           INT,
    sm_odoo_picking_id        INT,
    sm_odoo_write_date        TIMESTAMP,

    -- dimensions (Odoo IDs)
    sm_product_id_odoo        INT NOT NULL,
    sm_location_id_odoo       INT NOT NULL,
    sm_date_key               INT NOT NULL,   -- YYYYMMDD

    -- measures
    sm_qty                    NUMERIC(16,4) NOT NULL,
    sm_signed_qty             NUMERIC(16,4) NOT NULL,
    sm_uom_name               TEXT,

    -- qualifiers
    sm_movement_side          TEXT NOT NULL,  -- 'SRC' | 'DST'
    sm_location_usage         TEXT,
    sm_is_internal_location   BOOLEAN NOT NULL DEFAULT FALSE,

    -- technical
    sm_etl_loaded_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (sm_fct_id, sm_movement_day)
)
PARTITION BY RANGE (sm_movement_day);

COMMENT ON TABLE core.fct_sm_stock_movement IS
'FACT Stock Movement (SM): built from stock.move.line (state=done). 2 rows per move line (SRC/DST). Partitioned monthly.';

-- ===================================================================
-- Partition creation procedure (monthly)
-- ===================================================================
CREATE OR REPLACE PROCEDURE core.sp_create_part_fct_sm_stock_movement(
    p_from DATE,
    p_to   DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    d           DATE;
    month_start DATE;
    next_month  DATE;
    p_name      TEXT;
BEGIN
    d := date_trunc('month', p_from)::DATE;

    WHILE d <= p_to LOOP
        month_start := date_trunc('month', d)::DATE;
        next_month  := (date_trunc('month', d) + INTERVAL '1 month')::DATE;
        p_name      := format('fct_sm_stock_movement_%s', to_char(month_start, 'YYYYMM'));

        EXECUTE format($f$
            CREATE TABLE IF NOT EXISTS core.%I
            PARTITION OF core.fct_sm_stock_movement
            FOR VALUES FROM (%L) TO (%L);
        $f$, p_name, month_start, next_month);

        -- Natural key (idempotent load)
        EXECUTE format($f$
            CREATE UNIQUE INDEX IF NOT EXISTS %I
            ON core.%I (sm_odoo_move_line_id, sm_movement_side, sm_location_id_odoo);
        $f$, 'ux_'||p_name||'_natkey', p_name);

        -- Performance indexes
        EXECUTE format($f$
            CREATE INDEX IF NOT EXISTS %I
            ON core.%I (sm_product_id_odoo, sm_location_id_odoo, sm_date_key);
        $f$, 'ix_'||p_name||'_prod_loc_date', p_name);

        EXECUTE format($f$
            CREATE INDEX IF NOT EXISTS %I
            ON core.%I (sm_odoo_write_date);
        $f$, 'ix_'||p_name||'_write_date', p_name);

        d := next_month;
    END LOOP;
END;
$$;

-- ===================================================================
-- Retention procedure (drop partitions older than N months)
-- ===================================================================
CREATE OR REPLACE PROCEDURE core.sp_drop_part_fct_sm_stock_movement(
    p_keep_months INT DEFAULT 12
)
LANGUAGE plpgsql
AS $$
DECLARE
    cutoff DATE := (date_trunc('month', CURRENT_DATE)
                   - (p_keep_months || ' months')::INTERVAL)::DATE;
    r RECORD;
BEGIN
    FOR r IN
        SELECT inhrelid::regclass AS part_name
        FROM pg_inherits
        WHERE inhparent = 'core.fct_sm_stock_movement'::regclass
    LOOP
        IF r.part_name::TEXT ~ 'fct_sm_stock_movement_[0-9]{6}$' THEN
            IF to_date(right(r.part_name::TEXT, 6) || '01', 'YYYYMMDD') < cutoff THEN
                EXECUTE format('DROP TABLE IF EXISTS %s CASCADE;', r.part_name);
            END IF;
        END IF;
    END LOOP;
END;
$$;

-- ===================================================================
-- Create partitions: from last month to +12 months
-- ===================================================================
CALL core.sp_create_part_fct_sm_stock_movement(
    (date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')::DATE,
    (date_trunc('month', CURRENT_DATE) + INTERVAL '12 months')::DATE
);
