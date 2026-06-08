# brief-rfm-louveteaux-sauvages

## Contexte

Ce projet met en place une pipeline data RFM orchestrée avec Airflow, dockerisée avec Docker Compose, et exposée via Nginx.

L'application est en ligne sur **https://ymfo1nom.com** :

| Chemin | Service |
|--------|---------|
| `/` | Streamlit (viz) |
| `/airflow/` | Airflow (UI + API) |
| `/clickhouse/` | ClickHouse (HTTP) |
| `/deltalake/` | MinIO Console (bucket `lake`, Delta / S3) |
| `/dbt/` | dbt docs (catalogue modèles) |

Ancienne route `/doc/` → redirection vers `/`.

## Architecture technique

Services principaux :
- `postgres` : base de métadonnées Airflow
- `redis` : broker Celery
- `airflow-apiserver`, `airflow-scheduler`, `airflow-worker`, `airflow-triggerer`, `airflow-dag-processor`, `airflow-init`
- `postgres-db` : base applicative RFM
- `clickhouse` : analytics OLAP (couche gold, lecture Delta)
- `minio` + `minio-init` : stockage S3 local (`lake/`)
- `dbt` / `dbt-docs` : transformations SQL versionnées
- `streamlit` : app de restitution
- `nginx` : reverse proxy public

Flux global (lakehouse) :
1. Airflow ingère (API, fichiers, scrape) vers **MinIO** (`lake/`, format Delta/Parquet).
2. **ClickHouse** lit le lake et sert l'analytics ; **dbt** matérialise la couche gold.
3. **postgres-db** garde les données app / métadonnées classiques.
4. **Streamlit** restitue ; **nginx** route tout sur `ymfo1nom.com`.

```bash
# dbt (dev)
docker compose run --rm dbt dbt run
```

## CI/CD (GitHub Actions)

Workflows présents :
- `.github/workflows/deploy.yml` : déploiement complet stack (compose pull + up)
- `.github/workflows/deploy-dags.yml` : déploiement DAGs uniquement
- `.github/workflows/deploy-doc.yml` : déploiement Streamlit + Nginx + compose pour la route `/doc/`

Déclenchement :
- Push sur `develop` (selon les filtres `paths` de chaque workflow)
- Ou lancement manuel (`workflow_dispatch`)

## Variables GitHub Secrets

Secrets d'accès serveur (requis pour tous les workflows de déploiement) :
- `SSH_HOST`
- `SSH_USER`
- `SSH_PRIVATE_KEY`
- `DEPLOY_PATH`

Secrets runtime (principalement utilisés par `deploy.yml`) :
- `AIRFLOW_IMAGE_NAME`
- `AIRFLOW_UID`
- `AIRFLOW_PROJ_DIR`
- `AIRFLOW_DB_USER`
- `AIRFLOW_DB_PASSWORD`
- `AIRFLOW_DB_NAME`
- `APP_DB_USER`
- `APP_DB_PASSWORD`
- `APP_DB_NAME`
- `FERNET_KEY`
- `AIRFLOW__API_AUTH__JWT_SECRET`
- `AIRFLOW__API_AUTH__JWT_ISSUER`
- `AIRFLOW_WWW_USER_USERNAME`
- `AIRFLOW_WWW_USER_PASSWORD`
- `NGINX_PUBLIC_PORT`
- `PIP_ADDITIONAL_REQUIREMENTS`
- `AIRFLOW_WEBSERVER_BASE_URL` (ex. `https://ymfo1nom.com/airflow`)
- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
- `MINIO_BROWSER_REDIRECT_URL` (ex. `https://ymfo1nom.com/deltalake`)

## Sécurité base de données

Deux profils SQL applicatifs sont utilisés sur `postgres-db` :

1) Profil pipeline Airflow (lecture/écriture)
- utilisateur type : `airflow-xxxxx`
- droits : `CONNECT`, `USAGE`, `SELECT`, `INSERT` + droits séquences
- destiné aux étapes ETL de la pipeline

2) Profil Streamlit (lecture seule)
- utilisateur type : `streamlit-xxxxx`
- droits : `CONNECT`, `USAGE`, `SELECT`
- destiné à la mise à disposition des données côté app

Les suffixes `-xxxxx` servent d'obfuscation (forme de "salage" des noms d'utilisateur) pour limiter l'exposition directe de noms de comptes prévisibles.

## Gestion des utilisateurs Airflow

Des profils Airflow applicatifs ont été créés pour les participants du projet, afin d'éviter le partage d'un compte unique et d'améliorer la traçabilité.

## Lancement local

1. Copier `.env.example` vers `.env`
2. Renseigner les variables
3. Lancer :

```bash
docker compose up -d
```

Accès locaux :
- Airflow : `http://localhost:${NGINX_PUBLIC_PORT}/`
- Streamlit : `http://localhost:${NGINX_PUBLIC_PORT}/doc/`

## Environnement de développement (sans Nginx)

Le fichier `docker-compose.dev.yaml` mime le comportement de la stack principale, mais pour un usage local direct :
- pas de reverse proxy Nginx
- ports exposés service par service
- fichier d'environnement dédié : `.env.dev`

Fichiers à utiliser :
- `docker-compose.dev.yaml`
- `.env.dev.example` (modèle à copier)

### Fonctionnement de `.env.dev`

1. Copier `.env.dev.example` vers `.env.dev`
2. Adapter les variables si besoin (mots de passe, ports, clés)
3. Lancer le compose dev avec ce fichier d'env

Commandes :

```bash
cp .env.dev.example .env.dev
docker compose -f docker-compose.dev.yaml --env-file .env.dev up -d
```

Arrêt :

```bash
docker compose -f docker-compose.dev.yaml --env-file .env.dev down
```

Accès locaux (dev) :
- Airflow API/UI : `http://localhost:${AIRFLOW_API_PUBLIC_PORT}`
- Streamlit : `http://localhost:${STREAMLIT_PUBLIC_PORT}`
- PostgreSQL Airflow : `localhost:${AIRFLOW_DB_PUBLIC_PORT}`
- PostgreSQL applicatif : `localhost:${APP_DB_PUBLIC_PORT}`

Variables principales du `.env.dev` :
- `ENV_FILE_PATH` : chemin du fichier d'env injecté dans les services Airflow
- `AIRFLOW_DB_*` : connexion base de métadonnées Airflow
- `APP_DB_*` : connexion base applicative RFM
- `AIRFLOW_API_PUBLIC_PORT`, `STREAMLIT_PUBLIC_PORT` : ports exposés localement
- `FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET` : secrets techniques Airflow (valeurs dev uniquement)

## Notes production

- Ne pas exposer `airflow-apiserver` directement
- Conserver les volumes Docker pour la persistance
- Utiliser des secrets robustes et uniques
- Ajouter TLS en frontal pour une exposition Internet