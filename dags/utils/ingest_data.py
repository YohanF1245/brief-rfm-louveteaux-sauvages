import pandas as pd
import os
from dotenv import load_dotenv
from utils.logger import get_logger
from utils.db_utils import get_connection, execute_query
from psycopg2.extras import execute_values

load_dotenv()

log = get_logger("ingestion")

# ─────────────────────────────────────────────────────────
# PARAMÈTRES DEPUIS .env
# ─────────────────────────────────────────────────────────

DATA_PATH  = os.getenv("DATA_PATH",  "/opt/airflow/data/online_retail_II.xlsx")
SHEET_NAME = os.getenv("EXCEL_SHEET", "Year 2010-2011")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))

# ─────────────────────────────────────────────────────────
# 1. LECTURE DU FICHIER EXCEL
# ─────────────────────────────────────────────────────────

def load_excel(
    path: str = DATA_PATH,
    sheet_name: str = SHEET_NAME,
) -> pd.DataFrame:
    """
    Lit le fichier Excel Online Retail II.

    Args:
        path       : chemin vers le fichier .xlsx  (défaut : DATA_PATH du .env)
        sheet_name : nom de la feuille à lire      (défaut : EXCEL_SHEET du .env)

    Returns:
        pd.DataFrame brut (toutes colonnes en str)
    """
    log.info(f"Lecture du fichier : {path}")
    log.debug(f"Feuille : {sheet_name}")

    if not os.path.exists(path):
        log.error(f"Fichier introuvable : {path}")
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    try:
        df = pd.read_excel(
            path,
            sheet_name=sheet_name,
            dtype=str,
            engine="openpyxl",
        )
        log.info(f"Fichier chargé : {len(df)} lignes, {len(df.columns)} colonnes")
        log.debug(f"Colonnes détectées : {df.columns.tolist()}")
        return df

    except ValueError:
        log.error(f"Feuille '{sheet_name}' introuvable dans le fichier")
        raise


# ─────────────────────────────────────────────────────────
# 2. VALIDATION DES COLONNES
# ─────────────────────────────────────────────────────────

def validate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vérifie que les colonnes attendues sont présentes
    et les renomme en snake_case.

    Args:
        df : DataFrame brut issu de load_excel

    Returns:
        pd.DataFrame avec colonnes standardisées
    """
    expected = {
        "Invoice"     : "invoice",
        "StockCode"   : "stock_code",
        "Description" : "description",
        "Quantity"    : "quantity",
        "InvoiceDate" : "invoice_date",
        "Price"       : "price",
        "Customer ID" : "customer_id",
        "Country"     : "country",
    }

    missing = [col for col in expected if col not in df.columns]
    if missing:
        log.error(f"Colonnes manquantes : {missing}")
        raise KeyError(f"Colonnes manquantes : {missing}")

    df = df.rename(columns=expected)
    log.info("Colonnes validées et renommées")
    return df


# ─────────────────────────────────────────────────────────
# 3. RAPPORT QUALITÉ
# ─────────────────────────────────────────────────────────

def log_data_quality(df: pd.DataFrame):
    """
    Logue un rapport qualité rapide des données brutes.

    Args:
        df : DataFrame après validation des colonnes
    """
    log.info("── Rapport qualité ──────────────────────")
    log.info(f"  Lignes totales       : {len(df)}")
    log.info(f"  Sans customer_id     : {df['customer_id'].isna().sum()}")
    log.info(f"  Lignes dupliquées    : {df.duplicated().sum()}")
    log.info(f"  Pays uniques         : {df['country'].nunique()}")
    log.info(f"  Plage de dates       : {df['invoice_date'].min()} → {df['invoice_date'].max()}")
    log.info("─────────────────────────────────────────")


# ─────────────────────────────────────────────────────────
# 4. CRÉATION DE LA TABLE
# ─────────────────────────────────────────────────────────
def create_raw_table(conn):
    log.info("Création de raw.raw_orders (si inexistante)")
    execute_query(conn, """
        CREATE TABLE IF NOT EXISTS raw.raw_orders (
            invoice      TEXT,
            stock_code   TEXT,
            description  TEXT,
            quantity     TEXT,
            invoice_date TEXT,
            price        TEXT,
            customer_id  TEXT,
            country      TEXT
        );
    """)
    log.debug("Table raw.raw_orders prête")



# ─────────────────────────────────────────────────────────
# 5. INSERTION EN BASE
# ─────────────────────────────────────────────────────────

def insert_raw_data(conn, df: pd.DataFrame, batch_size: int = BATCH_SIZE):
    rows  = [tuple(row) for row in df.itertuples(index=False)]
    total = len(rows)
    log.info(f"Insertion de {total} lignes dans raw.raw_orders")

    with conn.cursor() as cur:
        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            execute_values(
                cur,
                "INSERT INTO raw.raw_orders VALUES %s",
                batch,
            )
            log.debug(f"  Batch {i} → {min(i + batch_size, total)} inséré")

    conn.commit()
    log.info(f"✅ {total} lignes insérées dans raw.raw_orders")


# ─────────────────────────────────────────────────────────
# 6. POINT D'ENTRÉE (appelé par le DAG)
# ─────────────────────────────────────────────────────────

def run_ingestion():
    """
    Orchestre l'ingestion complète :
    lecture → validation → qualité → insertion BDD.
    Tous les paramètres sont lus depuis le .env
    """
    log.info("═══ DÉBUT INGESTION ═══")
    log.info(f"  Fichier    : {DATA_PATH}")
    log.info(f"  Feuille    : {SHEET_NAME}")
    log.info(f"  Batch size : {BATCH_SIZE}")

    try:
        df   = load_excel()
        df   = validate_columns(df)
        log_data_quality(df)

        conn = get_connection()
        create_raw_table(conn)
        insert_raw_data(conn, df)
        conn.close()

        log.info("═══ INGESTION TERMINÉE AVEC SUCCÈS ═══")

    except Exception as e:
        log.error(f"Échec de l'ingestion : {e}", exc_info=True)
        raise