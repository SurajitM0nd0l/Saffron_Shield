import psycopg

from databricks.sdk import WorkspaceClient

# Lakebase connection details
PGHOST = "ep-dark-flower-d8qg3me7.database.us-east-2.cloud.databricks.com"
PGPORT = "5432"
PGDATABASE = "databricks_postgres"
PGUSER = "surajitm0nd0l12@gmail.com"

# Copy the exact resource name from the Lakebase Connect details.
ENDPOINT_NAME = (
    "projects/saffron-shield-memory/branches/production/endpoints/primary"
)

workspace_client = WorkspaceClient()


def get_lakebase_connection():
    # Generate a fresh OAuth credential.
    credential = (
        workspace_client.postgres
        .generate_database_credential(endpoint=ENDPOINT_NAME)
    )

    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=credential.token,
        sslmode="require"
    )