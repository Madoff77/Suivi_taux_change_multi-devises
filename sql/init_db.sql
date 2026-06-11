-- =============================================================================
-- init_db.sql
-- Base de donnees metier "exchange_rates"
-- Cree automatiquement au premier demarrage du conteneur postgres-exchange
-- (monte dans /docker-entrypoint-initdb.d/).
--
-- Contient :
--   1. exchange_rate_raw_responses   -> reponses API brutes (tracabilite)
--   2. exchange_rates                -> taux structures (table cible idempotente)
--   3. exchange_rate_rejected_rows   -> table cimetiere (lignes invalides)
--   4. exchange_rate_alerts          -> alertes de variation
--   5. exchange_rate_ingestion_logs  -> journal d'execution
--   6. 4 vues KPI exploitables dans Metabase
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Reponses brutes de l'API (avant toute transformation metier)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exchange_rate_raw_responses (
    id                  BIGSERIAL PRIMARY KEY,
    base_currency       VARCHAR(8),
    target_currency     VARCHAR(8),
    api_url             TEXT,
    http_status         INTEGER,
    raw_payload         JSONB,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    dag_run_id          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_responses_pair
    ON exchange_rate_raw_responses (base_currency, target_currency);
CREATE INDEX IF NOT EXISTS idx_raw_responses_ingested
    ON exchange_rate_raw_responses (ingestion_timestamp);

-- -----------------------------------------------------------------------------
-- 2. Taux structures (table cible) - idempotente grace a la contrainte UNIQUE
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exchange_rates (
    id                  BIGSERIAL PRIMARY KEY,
    rate_date           DATE        NOT NULL,
    base_currency       VARCHAR(8)  NOT NULL,
    target_currency     VARCHAR(8)  NOT NULL,
    exchange_rate       NUMERIC(20, 8) NOT NULL,
    source              TEXT        NOT NULL DEFAULT 'frankfurter',
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    dag_run_id          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_exchange_rates_pair_date
        UNIQUE (rate_date, base_currency, target_currency)
);

CREATE INDEX IF NOT EXISTS idx_exchange_rates_pair_date
    ON exchange_rates (base_currency, target_currency, rate_date DESC);

-- -----------------------------------------------------------------------------
-- 3. Table cimetiere : lignes rejetees par le controle qualite
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exchange_rate_rejected_rows (
    id                  BIGSERIAL PRIMARY KEY,
    base_currency       VARCHAR(8),
    target_currency     VARCHAR(8),
    raw_payload         JSONB,
    rejection_reason    TEXT        NOT NULL,
    quality_dimension   TEXT        NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    dag_run_id          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rejected_dimension
    ON exchange_rate_rejected_rows (quality_dimension);

-- -----------------------------------------------------------------------------
-- 4. Alertes de variation au-dela du seuil configurable
--    Contrainte UNIQUE -> evite les alertes en double si on relance le run.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exchange_rate_alerts (
    id                  BIGSERIAL PRIMARY KEY,
    base_currency       VARCHAR(8)  NOT NULL,
    target_currency     VARCHAR(8)  NOT NULL,
    previous_rate       NUMERIC(20, 8),
    current_rate        NUMERIC(20, 8),
    variation_absolute  NUMERIC(20, 8),
    variation_percent   NUMERIC(12, 4),
    threshold_percent   NUMERIC(12, 4),
    rate_date           DATE        NOT NULL,
    dag_run_id          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_alerts_pair_date
        UNIQUE (base_currency, target_currency, rate_date)
);

CREATE INDEX IF NOT EXISTS idx_alerts_date
    ON exchange_rate_alerts (rate_date DESC);

-- -----------------------------------------------------------------------------
-- 5. Journal d'execution du pipeline (un enregistrement par run)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exchange_rate_ingestion_logs (
    id                  BIGSERIAL PRIMARY KEY,
    dag_id              TEXT        NOT NULL,
    dag_run_id          TEXT        NOT NULL,
    execution_timestamp TIMESTAMPTZ NOT NULL,
    status              TEXT        NOT NULL,   -- SUCCESS | FAILED
    rows_received       INTEGER     NOT NULL DEFAULT 0,
    rows_valid          INTEGER     NOT NULL DEFAULT 0,
    rows_rejected       INTEGER     NOT NULL DEFAULT 0,
    rows_inserted       INTEGER     NOT NULL DEFAULT 0,
    alerts_created      INTEGER     NOT NULL DEFAULT 0,
    message             TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_logs_run
    ON exchange_rate_ingestion_logs (dag_run_id);

-- =============================================================================
-- VUES KPI POUR METABASE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- KPI 1 - Derniers taux disponibles
-- Objectif : le dernier taux connu pour chaque paire de devises.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_latest_exchange_rates AS
SELECT DISTINCT ON (base_currency, target_currency)
       base_currency,
       target_currency,
       exchange_rate,
       rate_date,
       source,
       ingestion_timestamp
FROM exchange_rates
ORDER BY base_currency, target_currency, rate_date DESC, ingestion_timestamp DESC;

-- -----------------------------------------------------------------------------
-- KPI 2 - Variations recentes
-- Objectif : variation entre deux observations consecutives par paire,
-- triee par amplitude pour identifier les mouvements les plus marquants.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_recent_rate_variations AS
WITH ordered AS (
    SELECT base_currency,
           target_currency,
           rate_date,
           exchange_rate,
           LAG(exchange_rate) OVER (
               PARTITION BY base_currency, target_currency
               ORDER BY rate_date
           ) AS previous_rate
    FROM exchange_rates
)
SELECT base_currency,
       target_currency,
       rate_date,
       previous_rate,
       exchange_rate AS current_rate,
       (exchange_rate - previous_rate) AS variation_absolute,
       CASE
           WHEN previous_rate IS NOT NULL AND previous_rate <> 0
           THEN ROUND(((exchange_rate - previous_rate) / previous_rate) * 100, 4)
       END AS variation_percent
FROM ordered
WHERE previous_rate IS NOT NULL
ORDER BY rate_date DESC, ABS(exchange_rate - previous_rate) DESC;

-- -----------------------------------------------------------------------------
-- KPI supplementaire 1 - Volatilite moyenne sur 7 jours
-- Objectif : paires les plus instables sur 7 jours (moyenne des variations).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_7d_average_volatility AS
WITH ordered AS (
    SELECT base_currency,
           target_currency,
           rate_date,
           exchange_rate,
           LAG(exchange_rate) OVER (
               PARTITION BY base_currency, target_currency
               ORDER BY rate_date
           ) AS previous_rate
    FROM exchange_rates
    WHERE rate_date >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT base_currency,
       target_currency,
       COUNT(*) FILTER (WHERE previous_rate IS NOT NULL) AS observations,
       ROUND(AVG(ABS(exchange_rate - previous_rate))
             FILTER (WHERE previous_rate IS NOT NULL), 8) AS avg_abs_variation,
       ROUND(AVG(ABS((exchange_rate - previous_rate) / NULLIF(previous_rate, 0)) * 100)
             FILTER (WHERE previous_rate IS NOT NULL), 4) AS avg_pct_variation
FROM ordered
GROUP BY base_currency, target_currency
ORDER BY avg_pct_variation DESC NULLS LAST;

-- -----------------------------------------------------------------------------
-- KPI supplementaire 2 - Qualite d'ingestion
-- Objectif : suivre recues / valides / rejetees et le taux de rejet par run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_ingestion_quality_summary AS
SELECT dag_run_id,
       dag_id,
       execution_timestamp,
       status,
       rows_received,
       rows_valid,
       rows_rejected,
       rows_inserted,
       alerts_created,
       CASE
           WHEN rows_received > 0
           THEN ROUND((rows_rejected::numeric / rows_received) * 100, 2)
           ELSE 0
       END AS rejection_rate_pct
FROM exchange_rate_ingestion_logs
ORDER BY execution_timestamp DESC;
