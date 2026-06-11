# Robustesse et qualite du pipeline

## Pourquoi un decoupage en plusieurs taches ?
Chaque etape (extraction, stockage brut, transformation, qualite, chargement,
alerting, logs) est isolee dans une tache Airflow distincte. Cela rend le DAG
**lisible**, permet de **rejouer une seule etape** en cas d'echec, donne des
logs cibles par etape, et applique des **retries/timeouts adaptes** a la nature
de chaque operation plutot qu'une politique globale unique.

## Pourquoi stocker les reponses brutes ?
La table `exchange_rate_raw_responses` conserve la reponse API telle quelle
(URL, code HTTP, JSON, horodatage). C'est notre **source de verite** : en cas
de bug de transformation, on peut **rejouer** sans rappeler l'API ; en cas de
litige sur une valeur, on dispose de la preuve d'origine ; et meme une reponse
invalide reste **tracable** pour le diagnostic.

## Pourquoi une table cimetiere ?
Les lignes invalides ne sont pas silencieusement perdues : elles partent dans
`exchange_rate_rejected_rows` avec la **raison** et la **dimension qualite**
concernee. On obtient ainsi une donnee propre en aval (table `exchange_rates`)
sans jamais sacrifier l'**auditabilite** : on sait quoi a ete rejete, et
pourquoi.

## Pourquoi ces controles qualite ?
Les cinq dimensions ciblent les pannes reelles de ce flux : la **completude**
attrape les champs manquants ; la **coherence** (taux > 0, devises differentes,
codes sur 3 lettres) attrape les valeurs absurdes ; la **fraicheur** evite de
charger une donnee perimee (week-ends/jours feries de l'API) ; l'**unicite**
protege la cle metier ; la **structure** valide le contrat de l'API avant toute
exploitation. Ce sont les erreurs les plus probables d'une API publique gratuite.

## Pourquoi l'idempotence ?
La table `exchange_rates` porte une contrainte `UNIQUE (rate_date, base, target)`
et le chargement utilise `ON CONFLICT ... DO UPDATE`. Relancer le meme run ne
cree donc **aucun doublon** ; les alertes utilisent `ON CONFLICT DO NOTHING`
sur la meme cle. Un pipeline rejouable sans effet de bord est indispensable en
production (reprise apres incident, backfill).

## Pourquoi ces retries et timeouts ?
- **Extraction API : 3 retries** car une API publique peut etre indisponible
  temporairement (reseau, rate limit).
- **Ecritures PostgreSQL : 2 retries** car un souci de connexion est souvent
  transitoire.
- **Transformation / qualite : 0-1 retry** car une erreur y est logique
  (donnee mal formee) et reessayer ne la corrigerait pas.
- Des `execution_timeout` evitent qu'une tache bloquee ne fige le scheduler.

## Pourquoi un seuil d'alerte configurable ?
Le seuil (`exchange_rate_alert_threshold_pct`) vit dans une Variable Airflow :
on l'ajuste **sans toucher au code ni redeployer**. Les paires volatiles (JPY)
et stables (EUR/USD) n'ont pas la meme sensibilite ; externaliser le seuil rend
le pipeline adaptable au contexte metier et aux retours des utilisateurs.
