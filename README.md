# Projet Final — Suivi des taux de change multi-devises

Plateforme d'orchestration **Airflow** pour l'ingestion, le stockage, la
transformation, le controle qualite et l'analyse des taux de change
multi-devises a partir de l'**API Frankfurter**, avec visualisation des KPI
dans **Metabase**. Le tout est **dockerise** et lancable en une commande.

---

## 1. Presentation du projet
Le pipeline interroge quotidiennement l'API Frankfurter pour au moins 5 paires
de devises, stocke les reponses brutes, transforme les donnees, applique 5
controles qualite, charge les lignes valides de maniere **idempotente**, isole
les lignes invalides dans une **table cimetiere**, detecte les **variations**
au-dela d'un seuil configurable, journalise chaque execution et expose 4 **vues
KPI** pour Metabase.

API utilisee — exemple : `https://api.frankfurter.app/latest?from=EUR&to=USD`

## 2. Architecture Docker
```
                +-------------------+
                |   API Frankfurter |
                +---------+---------+
                          |
        +-----------------v------------------+
        |  airflow-scheduler / webserver     |
        |  DAG: exchange_rates_pipeline      |
        +----+-------------------------+-----+
             | metadata                | conn: exchange_postgres
   +---------v---------+      +--------v-----------+      +-----------+
   | postgres-airflow  |      | postgres-exchange  |<-----| metabase  |
   +-------------------+      +--------------------+      +-----------+
                                  (KPI views)            http://localhost:3000
```

## 3. Liste des services Docker
| Service             | Role                                    | Port hote |
|---------------------|-----------------------------------------|-----------|
| `postgres-airflow`  | Metadonnees Airflow                     | interne   |
| `airflow-init`      | Migration DB + creation admin           | -         |
| `airflow-webserver` | UI Airflow                              | **8080**  |
| `airflow-scheduler` | Ordonnanceur                            | interne   |
| `postgres-exchange` | Base metier des taux                    | **5433**  |
| `metabase`          | Visualisation des KPI                   | **3000**  |

## 4. Prerequis
- Docker + Docker Compose v2
- ~4 Go de RAM disponibles
- Ports 8080, 3000 et 5433 libres

## 5. Commandes de lancement
```bash
docker compose up -d
```
```bash
docker compose ps
```
```bash
docker compose logs -f airflow-scheduler
```

Acces :
- **Airflow** : http://localhost:8080 — login `airflow` / password `airflow`
- **Metabase** : http://localhost:3000

## 6. Initialisation Airflow
Le service `airflow-init` execute automatiquement la migration de la base et la
creation de l'utilisateur admin. La connexion metier et les Variables sont
injectees via les variables d'environnement (fichier `.env`) — **aucune action
manuelle** n'est requise.

Pour declencher le DAG : ouvrez l'UI, activez (toggle) le DAG
`exchange_rates_pipeline`, puis cliquez sur **Trigger DAG**.

## 7. Variables Airflow a creer
Injectees automatiquement via `.env` (visibles dans Admin -> Variables) :

| Variable                                  | Valeur par defaut |
|-------------------------------------------|-------------------|
| `exchange_rate_pairs`                     | 5 paires (voir ci-dessous) |
| `exchange_rate_alert_threshold_pct`       | `1.5` |
| `exchange_rate_freshness_threshold_hours` | `24` |

```json
exchange_rate_pairs = [
  {"base": "EUR", "target": "USD"},
  {"base": "EUR", "target": "GBP"},
  {"base": "EUR", "target": "JPY"},
  {"base": "USD", "target": "CHF"},
  {"base": "GBP", "target": "USD"}
]
```

> Pour les modifier sans redeployer : Admin -> Variables dans l'UI Airflow.

## 8. Connection ID PostgreSQL a utiliser
Le DAG utilise le Connection ID **`exchange_postgres`**, injecte via :
```
postgresql://exchange:exchange@postgres-exchange:5432/exchange_rates
```
Aucun identifiant PostgreSQL n'est hardcode dans le DAG.

## 9. Description du DAG
`dags/exchange_rates_pipeline.py` — **TaskFlow API** (`@dag` / `@task`),
schedule `@daily`, `catchup=False`, `max_active_runs=1`.

| # | Tache                          | Role |
|---|--------------------------------|------|
| 1 | `load_pipeline_config`         | Charge/valide les Variables Airflow |
| 2 | `extract_exchange_rates`       | Appelle l'API (timeout, gestion erreurs) |
| 3 | `store_raw_responses`          | Stocke les reponses brutes horodatees |
| 4 | `transform_exchange_rates`     | 1 ligne par paire et par date |
| 5 | `quality_check_exchange_rates` | 5 dimensions qualite, separe valides/invalides |
| 6 | `load_invalid_rows_to_cemetery`| Charge les rejets (raison + dimension) |
| 7 | `load_valid_rates`             | UPSERT idempotent (`ON CONFLICT`) |
| 8 | `detect_rate_variations`       | Alerte si variation >= seuil |
| 9 | `write_ingestion_log`          | Journal SUCCESS du run |
| 10| `nominal_path_summary`         | Resume lisible dans les logs |
| 11| `failure_path_log`             | Journal FAILED (chemin d'echec) |

## 10. Description des tables
Toutes definies dans `sql/init_db.sql` :
- `exchange_rate_raw_responses` — reponses API brutes (JSONB, tracabilite).
- `exchange_rates` — taux structures ; `UNIQUE (rate_date, base, target)`.
- `exchange_rate_rejected_rows` — table cimetiere (raison + dimension qualite).
- `exchange_rate_alerts` — alertes de variation au-dela du seuil.
- `exchange_rate_ingestion_logs` — journal d'execution (SUCCESS/FAILED).

## 11. Description des controles qualite
1. **Completude** : `base`, `target`, `rate_date`, `exchange_rate` non vides.
2. **Coherence** : taux strictement positif ; `base != target` ; codes sur 3 lettres.
3. **Fraicheur** : age <= `exchange_rate_freshness_threshold_hours`.
4. **Unicite** : pas de doublon `(rate_date, base, target)` dans le batch.
5. **Structure** : cles API `amount/base/date/rates` presentes + devise cible dans `rates`.

Les lignes invalides sont envoyees dans `exchange_rate_rejected_rows` avec la
raison, la dimension qualite et le payload.

## 12. Description du chemin nominal
```
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
```

## 13. Description du chemin d'echec
```
task failed -> retries Airflow -> failure_path_log (statut FAILED)
```
La tache `failure_path_log` a un `trigger_rule=one_failed` : si une tache amont
echoue apres epuisement de ses retries, une ligne `FAILED` est ecrite dans
`exchange_rate_ingestion_logs`.

## 14. Description des retries et timeouts
| Tache              | Retries | Justification |
|--------------------|---------|---------------|
| Extraction API     | 3       | API publique parfois indisponible |
| Ecritures Postgres | 2       | Souci de connexion souvent transitoire |
| Transformation     | 0       | Erreur logique : reessayer n'aide pas |
| Qualite            | 0       | Idem |
| Detection variations | 1     | Lecture DB, retry leger |

`execution_timeout` defini par tache (2 a 5 minutes) pour ne pas bloquer le scheduler.

## 15. Description des KPIs
| Vue                            | Objectif metier |
|--------------------------------|-----------------|
| `vw_latest_exchange_rates`     | Dernier taux par paire |
| `vw_recent_rate_variations`    | Variations entre deux observations |
| `vw_7d_average_volatility`     | Volatilite moyenne sur 7 jours |
| `vw_ingestion_quality_summary` | Recues/valides/rejetees + taux de rejet |

## 16. Comment connecter Metabase
1. Ouvrir http://localhost:3000 et creer le compte admin Metabase (premiere visite).
2. **Add a database** -> type **PostgreSQL** :
   - Host : `postgres-exchange`
   - Port : `5432`
   - Database name : `exchange_rates`
   - Username : `exchange`
   - Password : `exchange`
3. Une fois synchronise, les tables et **vues** (`vw_*`) sont disponibles.
4. **Creer les cartes KPI** : New -> Question -> choisir une vue `vw_*` ->
   visualiser (table, barres, ligne) -> **Save**.
5. **Creer un dashboard** : New -> Dashboard -> ajouter les 4 cartes KPI -> Save.

> Note : Metabase utilise depuis le conteneur le port interne **5432** (et non
> 5433 qui est l'exposition vers la machine hote).

## 17. Comment verifier les donnees avec SQL
```bash
docker compose exec postgres-exchange psql -U exchange -d exchange_rates
```
```sql
SELECT * FROM exchange_rate_raw_responses ORDER BY created_at DESC;
SELECT * FROM exchange_rates ORDER BY rate_date DESC, base_currency, target_currency;
SELECT * FROM exchange_rate_rejected_rows ORDER BY created_at DESC;
SELECT * FROM exchange_rate_alerts ORDER BY created_at DESC;
SELECT * FROM exchange_rate_ingestion_logs ORDER BY created_at DESC;
SELECT * FROM vw_latest_exchange_rates;
SELECT * FROM vw_recent_rate_variations;
SELECT * FROM vw_7d_average_volatility;
SELECT * FROM vw_ingestion_quality_summary;
```

## 18. Captures attendues pour le rendu
1. UI Airflow — **Graph View** d'une execution reussie.
2. Tables PostgreSQL apres execution (`SELECT` sur chaque table).
3. Table de logs `exchange_rate_ingestion_logs`.
4. Table d'alertes avec **justification du seuil** retenu.
5. KPIs sur Metabase.
6. Dashboard Metabase (si possible).

## 19. Plan de demo (soutenance, max 15 min)
1. **Architecture Docker** (`docker compose ps`) — 2 min.
2. **DAG Airflow** (Graph View, taches separees) — 2 min.
3. **Execution reussie** (Trigger + Graph vert) — 2 min.
4. **Lecture des logs** (logs scheduler + table `ingestion_logs`) — 1 min.
5. **Verification PostgreSQL** (`psql` + SELECT) — 2 min.
6. **Visualisation Metabase** (dashboard KPI) — 2 min.
7. **Controles qualite** (table cimetiere + dimensions) — 2 min.
8. **Chemin d'echec** (`failure_path_log` + statut FAILED) — 1 min.
9. **Choix de robustesse** (idempotence, retries, seuil) — 1 min.

---

## Demarrage rapide
```bash
docker compose up -d
# attendre ~1 min, puis ouvrir http://localhost:8080 (airflow/airflow)
# activer et declencher le DAG exchange_rates_pipeline
# puis http://localhost:3000 pour Metabase
```

## Limites / actions manuelles restantes
- **Metabase** : la creation du compte admin, la connexion a la base metier et
  la creation du dashboard sont **manuelles** (etapes detaillees en section 16).
- La premiere execution n'a **pas de taux precedent** en base : les alertes
  n'apparaissent qu'a partir de la **2e execution** (ou apres backfill).
- L'API Frankfurter ne renvoie pas de taux les week-ends/jours feries : la
  donnee peut alors etre rejetee par le controle de **fraicheur** selon le seuil.
