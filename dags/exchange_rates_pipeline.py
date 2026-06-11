"""
exchange_rates_pipeline
========================

Pipeline d'orchestration Airflow (TaskFlow API) pour le suivi des taux de
change multi-devises a partir de l'API Frankfurter.

Chemin nominal :
    load_pipeline_config
    -> extract_exchange_rates
    -> store_raw_responses
    -> transform_exchange_rates
    -> quality_check_exchange_rates
    -> load_invalid_rows_to_cemetery
    -> load_valid_rates
    -> detect_rate_variations
    -> write_ingestion_log
    -> nominal_path_summary

Chemin d'echec :
    une tache critique echoue -> retries Airflow -> failure_path_log (FAILED)

Tous les parametres metier viennent des Variables Airflow ; la connexion
PostgreSQL metier passe par le Connection ID `exchange_postgres`.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

# -----------------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------------
POSTGRES_CONN_ID = "exchange_postgres"
FRANKFURTER_BASE_URL = "https://api.frankfurter.app/latest"
HTTP_TIMEOUT_SECONDS = 15
SOURCE_NAME = "frankfurter"

# Valeurs par defaut si les Variables Airflow ne sont pas definies.
DEFAULT_PAIRS = [
    {"base": "EUR", "target": "USD"},
    {"base": "EUR", "target": "GBP"},
    {"base": "EUR", "target": "JPY"},
    {"base": "USD", "target": "CHF"},
    {"base": "GBP", "target": "USD"},
]
DEFAULT_ALERT_THRESHOLD_PCT = 1.5
DEFAULT_FRESHNESS_THRESHOLD_HOURS = 24

EXPECTED_API_KEYS = {"amount", "base", "date", "rates"}


# -----------------------------------------------------------------------------
# Helpers (hors taches : reutilisables et testables)
# -----------------------------------------------------------------------------
def _get_hook() -> PostgresHook:
    """Retourne un PostgresHook base sur le Connection ID Airflow."""
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def _write_log_row(status: str, counts: dict, message: str, run_id: str) -> None:
    """Insere une ligne dans la table de journal d'execution."""
    hook = _get_hook()
    hook.run(
        """
        INSERT INTO exchange_rate_ingestion_logs
            (dag_id, dag_run_id, execution_timestamp, status,
             rows_received, rows_valid, rows_rejected, rows_inserted,
             alerts_created, message)
        VALUES
            (%(dag_id)s, %(dag_run_id)s, %(execution_timestamp)s, %(status)s,
             %(rows_received)s, %(rows_valid)s, %(rows_rejected)s,
             %(rows_inserted)s, %(alerts_created)s, %(message)s)
        """,
        parameters={
            "dag_id": "exchange_rates_pipeline",
            "dag_run_id": run_id,
            "execution_timestamp": datetime.now(timezone.utc),
            "status": status,
            "rows_received": counts.get("rows_received", 0),
            "rows_valid": counts.get("rows_valid", 0),
            "rows_rejected": counts.get("rows_rejected", 0),
            "rows_inserted": counts.get("rows_inserted", 0),
            "alerts_created": counts.get("alerts_created", 0),
            "message": message,
        },
    )


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
@dag(
    dag_id="exchange_rates_pipeline",
    description="Ingestion, qualite et analyse des taux de change Frankfurter.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["exchange-rates", "frankfurter", "kpi", "quality"],
)
def exchange_rates_pipeline():

    # -------------------------------------------------------------------------
    # 1. Configuration depuis les Variables Airflow
    # -------------------------------------------------------------------------
    @task(execution_timeout=timedelta(minutes=2))
    def load_pipeline_config() -> dict:
        """Charge et valide la configuration depuis les Variables Airflow."""
        pairs = Variable.get(
            "exchange_rate_pairs",
            default_var=json.dumps(DEFAULT_PAIRS),
            deserialize_json=True,
        )
        alert_threshold = float(
            Variable.get(
                "exchange_rate_alert_threshold_pct",
                default_var=DEFAULT_ALERT_THRESHOLD_PCT,
            )
        )
        freshness_hours = int(
            Variable.get(
                "exchange_rate_freshness_threshold_hours",
                default_var=DEFAULT_FRESHNESS_THRESHOLD_HOURS,
            )
        )

        # Validation de structure de la configuration.
        if not isinstance(pairs, list) or not pairs:
            raise ValueError("exchange_rate_pairs doit etre une liste non vide.")
        for pair in pairs:
            if not isinstance(pair, dict) or "base" not in pair or "target" not in pair:
                raise ValueError(f"Paire invalide (base/target attendus) : {pair}")
        if alert_threshold <= 0:
            raise ValueError("exchange_rate_alert_threshold_pct doit etre > 0.")
        if freshness_hours <= 0:
            raise ValueError("exchange_rate_freshness_threshold_hours doit etre > 0.")

        config = {
            "pairs": pairs,
            "alert_threshold_pct": alert_threshold,
            "freshness_threshold_hours": freshness_hours,
        }
        print(f"[config] {len(pairs)} paires, seuil alerte={alert_threshold}%, "
              f"fraicheur={freshness_hours}h")
        return config

    # -------------------------------------------------------------------------
    # 2. Extraction depuis l'API Frankfurter
    # -------------------------------------------------------------------------
    @task(retries=3, retry_delay=timedelta(minutes=1),
          execution_timeout=timedelta(minutes=5))
    def extract_exchange_rates(config: dict) -> list[dict]:
        """Appelle l'API Frankfurter pour chaque paire. Pas de transfo metier."""
        results: list[dict] = []
        for pair in config["pairs"]:
            base, target = pair["base"], pair["target"]
            url = f"{FRANKFURTER_BASE_URL}?from={base}&to={target}"
            record = {
                "base_currency": base,
                "target_currency": target,
                "api_url": url,
                "http_status": None,
                "raw_payload": None,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                resp = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
                record["http_status"] = resp.status_code
                try:
                    record["raw_payload"] = resp.json()
                except ValueError:
                    record["raw_payload"] = {"_error": "reponse non-JSON",
                                             "_text": resp.text[:500]}
                print(f"[extract] {base}->{target} HTTP {resp.status_code}")
            except requests.RequestException as exc:
                # On trace l'echec reseau sans interrompre les autres paires.
                record["http_status"] = -1
                record["raw_payload"] = {"_error": str(exc)}
                print(f"[extract] ERREUR reseau {base}->{target} : {exc}")
            results.append(record)
        return results

    # -------------------------------------------------------------------------
    # 3. Stockage des reponses brutes (passe-plat -> garantit l'ordre amont)
    # -------------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=1),
          execution_timeout=timedelta(minutes=3))
    def store_raw_responses(raw_records: list[dict], **context) -> list[dict]:
        """Stocke chaque reponse brute (meme invalide) avec horodatage."""
        run_id = context["run_id"]
        ingested_at = datetime.now(timezone.utc)
        hook = _get_hook()
        rows = [
            (
                rec["base_currency"],
                rec["target_currency"],
                rec["api_url"],
                rec["http_status"],
                json.dumps(rec["raw_payload"]),
                ingested_at,
                run_id,
            )
            for rec in raw_records
        ]
        hook.insert_rows(
            table="exchange_rate_raw_responses",
            rows=rows,
            target_fields=[
                "base_currency", "target_currency", "api_url", "http_status",
                "raw_payload", "ingestion_timestamp", "dag_run_id",
            ],
            commit_every=100,
        )
        print(f"[store_raw] {len(rows)} reponses brutes stockees.")
        return raw_records  # passe-plat vers la transformation

    # -------------------------------------------------------------------------
    # 4. Transformation : 1 ligne par paire et par date
    # -------------------------------------------------------------------------
    @task(execution_timeout=timedelta(minutes=3))
    def transform_exchange_rates(raw_records: list[dict], **context) -> list[dict]:
        """Transforme les reponses API en lignes exploitables."""
        run_id = context["run_id"]
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        transformed: list[dict] = []
        for rec in raw_records:
            payload = rec.get("raw_payload") or {}
            target = rec.get("target_currency")
            rates = payload.get("rates") if isinstance(payload, dict) else None
            transformed.append({
                "rate_date": payload.get("date") if isinstance(payload, dict) else None,
                "base_currency": rec.get("base_currency"),
                "target_currency": target,
                "exchange_rate": (rates or {}).get(target) if rates else None,
                "source": SOURCE_NAME,
                "ingestion_timestamp": ingestion_ts,
                "dag_run_id": run_id,
                "http_status": rec.get("http_status"),
                "raw_payload": payload,
            })
        print(f"[transform] {len(transformed)} lignes produites.")
        return transformed

    # -------------------------------------------------------------------------
    # 5. Controle qualite : completude / coherence / fraicheur / unicite / structure
    # -------------------------------------------------------------------------
    @task(execution_timeout=timedelta(minutes=3))
    def quality_check_exchange_rates(rows: list[dict], config: dict) -> dict:
        """Separe les lignes valides des lignes invalides selon 5 dimensions."""
        freshness_hours = config["freshness_threshold_hours"]
        now = datetime.now(timezone.utc)
        valid: list[dict] = []
        invalid: list[dict] = []
        seen_keys: set[tuple] = set()

        def reject(row: dict, dimension: str, reason: str) -> None:
            invalid.append({
                "base_currency": row.get("base_currency"),
                "target_currency": row.get("target_currency"),
                "raw_payload": row.get("raw_payload"),
                "rejection_reason": reason,
                "quality_dimension": dimension,
            })

        for row in rows:
            base = row.get("base_currency")
            target = row.get("target_currency")
            rate_date = row.get("rate_date")
            rate = row.get("exchange_rate")
            payload = row.get("raw_payload") or {}

            # --- 5. Structure : cles attendues + devise cible presente ---
            if not isinstance(payload, dict) or not EXPECTED_API_KEYS.issubset(payload):
                reject(row, "structure",
                       f"Cles API manquantes (attendu {sorted(EXPECTED_API_KEYS)})")
                continue
            if target not in (payload.get("rates") or {}):
                reject(row, "structure",
                       f"Devise cible {target} absente de 'rates'")
                continue

            # --- 1. Completude ---
            if not base or not target or not rate_date or rate is None:
                reject(row, "completude",
                       "Champ obligatoire manquant (base/target/date/rate)")
                continue

            # --- 2. Coherence ---
            if len(str(base)) != 3 or len(str(target)) != 3:
                reject(row, "coherence", "Code devise non conforme (3 lettres)")
                continue
            if base == target:
                reject(row, "coherence", "base_currency == target_currency")
                continue
            try:
                rate_value = float(rate)
            except (TypeError, ValueError):
                reject(row, "coherence", f"Taux non numerique : {rate}")
                continue
            if rate_value <= 0:
                reject(row, "coherence", f"Taux non strictement positif : {rate_value}")
                continue

            # --- 3. Fraicheur ---
            try:
                parsed_date = datetime.strptime(rate_date, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc)
            except (TypeError, ValueError):
                reject(row, "structure", f"Date illisible : {rate_date}")
                continue
            age_hours = (now - parsed_date).total_seconds() / 3600.0
            if age_hours > freshness_hours:
                reject(row, "fraicheur",
                       f"Donnee trop ancienne ({age_hours:.1f}h > {freshness_hours}h)")
                continue

            # --- 4. Unicite (dans le batch) ---
            key = (rate_date, base, target)
            if key in seen_keys:
                reject(row, "unicite", f"Doublon dans le batch : {key}")
                continue
            seen_keys.add(key)

            valid.append({
                "rate_date": rate_date,
                "base_currency": base,
                "target_currency": target,
                "exchange_rate": rate_value,
                "source": row.get("source", SOURCE_NAME),
                "ingestion_timestamp": row.get("ingestion_timestamp"),
                "dag_run_id": row.get("dag_run_id"),
            })

        print(f"[quality] {len(valid)} valides / {len(invalid)} rejetees "
              f"sur {len(rows)} recues.")
        return {"valid": valid, "invalid": invalid, "received": len(rows)}

    # -------------------------------------------------------------------------
    # 6. Chargement des lignes invalides dans la table cimetiere
    # -------------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=1),
          execution_timeout=timedelta(minutes=3))
    def load_invalid_rows_to_cemetery(qc_result: dict, **context) -> int:
        """Charge les lignes invalides dans exchange_rate_rejected_rows."""
        run_id = context["run_id"]
        invalid = qc_result.get("invalid", [])
        if not invalid:
            print("[cemetery] aucune ligne invalide.")
            return 0
        ingested_at = datetime.now(timezone.utc)
        hook = _get_hook()
        rows = [
            (
                row.get("base_currency"),
                row.get("target_currency"),
                json.dumps(row.get("raw_payload")),
                row.get("rejection_reason"),
                row.get("quality_dimension"),
                ingested_at,
                run_id,
            )
            for row in invalid
        ]
        hook.insert_rows(
            table="exchange_rate_rejected_rows",
            rows=rows,
            target_fields=[
                "base_currency", "target_currency", "raw_payload",
                "rejection_reason", "quality_dimension",
                "ingestion_timestamp", "dag_run_id",
            ],
            commit_every=100,
        )
        print(f"[cemetery] {len(rows)} lignes invalides chargees.")
        return len(rows)

    # -------------------------------------------------------------------------
    # 7. Chargement des lignes valides (idempotent via ON CONFLICT)
    # -------------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=1),
          execution_timeout=timedelta(minutes=3))
    def load_valid_rates(qc_result: dict) -> int:
        """Charge les lignes valides dans exchange_rates (UPSERT idempotent)."""
        valid = qc_result.get("valid", [])
        if not valid:
            print("[load_valid] aucune ligne valide a charger.")
            return 0
        hook = _get_hook()
        conn = hook.get_conn()
        inserted = 0
        try:
            with conn.cursor() as cur:
                for row in valid:
                    cur.execute(
                        """
                        INSERT INTO exchange_rates
                            (rate_date, base_currency, target_currency,
                             exchange_rate, source, ingestion_timestamp, dag_run_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (rate_date, base_currency, target_currency)
                        DO UPDATE SET
                            exchange_rate = EXCLUDED.exchange_rate,
                            source = EXCLUDED.source,
                            ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                            dag_run_id = EXCLUDED.dag_run_id
                        """,
                        (
                            row["rate_date"], row["base_currency"],
                            row["target_currency"], row["exchange_rate"],
                            row["source"], row["ingestion_timestamp"],
                            row["dag_run_id"],
                        ),
                    )
                    inserted += cur.rowcount
            conn.commit()
        finally:
            conn.close()
        print(f"[load_valid] {len(valid)} lignes upsert (idempotent).")
        return len(valid)

    # -------------------------------------------------------------------------
    # 8. Detection des variations au-dela du seuil
    # -------------------------------------------------------------------------
    @task(retries=1, execution_timeout=timedelta(minutes=3))
    def detect_rate_variations(qc_result: dict, config: dict, **context) -> int:
        """Compare le taux courant au precedent et cree des alertes si besoin."""
        valid = qc_result.get("valid", [])
        if not valid:
            print("[alerts] aucune ligne valide, pas d'alerte.")
            return 0
        threshold = config["alert_threshold_pct"]
        run_id = context["run_id"]
        hook = _get_hook()
        conn = hook.get_conn()
        alerts_created = 0
        try:
            with conn.cursor() as cur:
                for row in valid:
                    base = row["base_currency"]
                    target = row["target_currency"]
                    rate_date = row["rate_date"]
                    current = float(row["exchange_rate"])

                    # Taux precedent = derniere observation anterieure a rate_date.
                    cur.execute(
                        """
                        SELECT exchange_rate
                        FROM exchange_rates
                        WHERE base_currency = %s AND target_currency = %s
                          AND rate_date < %s
                        ORDER BY rate_date DESC
                        LIMIT 1
                        """,
                        (base, target, rate_date),
                    )
                    prev = cur.fetchone()
                    if not prev or prev[0] is None or float(prev[0]) == 0:
                        continue
                    previous = float(prev[0])
                    variation_abs = current - previous
                    variation_pct = (variation_abs / previous) * 100.0

                    if abs(variation_pct) >= threshold:
                        cur.execute(
                            """
                            INSERT INTO exchange_rate_alerts
                                (base_currency, target_currency, previous_rate,
                                 current_rate, variation_absolute, variation_percent,
                                 threshold_percent, rate_date, dag_run_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (base_currency, target_currency, rate_date)
                            DO NOTHING
                            """,
                            (base, target, previous, current, variation_abs,
                             round(variation_pct, 4), threshold, rate_date, run_id),
                        )
                        alerts_created += cur.rowcount
                        print(f"[alerts] {base}->{target} {variation_pct:.2f}% "
                              f"(seuil {threshold}%)")
            conn.commit()
        finally:
            conn.close()
        print(f"[alerts] {alerts_created} alerte(s) creee(s).")
        return alerts_created

    # -------------------------------------------------------------------------
    # 9. Journal d'execution (succes)
    # -------------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=1),
          execution_timeout=timedelta(minutes=2))
    def write_ingestion_log(qc_result: dict, rows_inserted: int,
                            rows_rejected: int, alerts_created: int,
                            **context) -> dict:
        """Ecrit une entree SUCCESS dans la table de logs."""
        counts = {
            "rows_received": qc_result.get("received", 0),
            "rows_valid": len(qc_result.get("valid", [])),
            "rows_rejected": rows_rejected,
            "rows_inserted": rows_inserted,
            "alerts_created": alerts_created,
        }
        _write_log_row(
            status="SUCCESS",
            counts=counts,
            message="Pipeline execute avec succes.",
            run_id=context["run_id"],
        )
        print(f"[log] SUCCESS {counts}")
        return counts

    # -------------------------------------------------------------------------
    # 10. Resume lisible du chemin nominal
    # -------------------------------------------------------------------------
    @task(execution_timeout=timedelta(minutes=2))
    def nominal_path_summary(counts: dict) -> None:
        """Affiche un resume lisible du run dans les logs Airflow."""
        print("=" * 60)
        print("RESUME DU CHEMIN NOMINAL - exchange_rates_pipeline")
        print("=" * 60)
        print(f"  Lignes recues   : {counts.get('rows_received', 0)}")
        print(f"  Lignes valides  : {counts.get('rows_valid', 0)}")
        print(f"  Lignes rejetees : {counts.get('rows_rejected', 0)}")
        print(f"  Lignes inserees : {counts.get('rows_inserted', 0)}")
        print(f"  Alertes creees  : {counts.get('alerts_created', 0)}")
        print("=" * 60)

    # -------------------------------------------------------------------------
    # 11. Chemin d'echec : trace un statut FAILED si une tache amont echoue
    # -------------------------------------------------------------------------
    @task(trigger_rule=TriggerRule.ONE_FAILED,
          execution_timeout=timedelta(minutes=2))
    def failure_path_log(**context) -> None:
        """Ecrit une entree FAILED dans la table de logs (chemin d'echec)."""
        run_id = context["run_id"]
        _write_log_row(
            status="FAILED",
            counts={},
            message="Une tache critique a echoue apres epuisement des retries.",
            run_id=run_id,
        )
        print(f"[failure] entree FAILED ecrite pour run_id={run_id}")

    # -------------------------------------------------------------------------
    # Orchestration / dependances
    # -------------------------------------------------------------------------
    config = load_pipeline_config()
    raw = extract_exchange_rates(config)
    stored = store_raw_responses(raw)
    transformed = transform_exchange_rates(stored)
    qc = quality_check_exchange_rates(transformed, config)

    rejected_count = load_invalid_rows_to_cemetery(qc)
    inserted_count = load_valid_rates(qc)
    alerts_count = detect_rate_variations(qc, config)

    # detect_rate_variations doit lire les taux apres chargement -> dependance.
    inserted_count >> alerts_count

    log_counts = write_ingestion_log(qc, inserted_count, rejected_count, alerts_count)
    nominal_path_summary(log_counts)

    # Chemin d'echec : declenche si n'importe quelle tache amont echoue.
    failure = failure_path_log()
    [config, raw, stored, transformed, qc, rejected_count,
     inserted_count, alerts_count, log_counts] >> failure


exchange_rates_pipeline()
