from dotenv import load_dotenv
from pydantic import BaseModel 
from google.cloud import secretmanager
from google.oauth2 import service_account
from dlt.common import pendulum
from dlt.sources.credentials import ConnectionStringCredentials
from sql_database import sql_database, sql_table, Table 
from typing import Mapping
import os 
import datetime
import sqlalchemy as sa 
import dlt 


class Profile(BaseModel):
    id: int
    username: str
    created_at: datetime.datetime
    gender: str
    is_human: bool
    

load_dotenv()

def get_secret_data():
    credentials = service_account.Credentials.from_service_account_file(os.environ["GOOGLE_SECRETS__CREDENTIALS"])
    secrets_prefix = os.environ["GCP_SECRET_MANAGER_PREFIX"]
    client = secretmanager.SecretManagerServiceClient(credentials= credentials)
    return {
        "db": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_DB/versions/main").payload.data.decode("UTF-8"),
        "host": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_HOST/versions/main").payload.data.decode("UTF-8"),
        "pass": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_PASS/versions/main").payload.data.decode("UTF-8"),
        "user": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_USER/versions/main").payload.data.decode("UTF-8"),
        "port": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_PORT/versions/main").payload.data.decode("UTF-8")
    }

def load_table_from_database(cred_mapping: Mapping[str,str]) -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    This example sources data from the public Rfam MySQL database.
    """
    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="testing", destination='bigquery', dataset_name="dev"
    )

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    credentials = ConnectionStringCredentials(
        f"postgresql+psycopg2://{cred_mapping.get('user')}:{cred_mapping.get('pass')}@{cred_mapping.get('host')}:{cred_mapping.get('port')}/{cred_mapping.get('db')}"
    )
    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source = sql_database(credentials, table_names=['profile']).with_resources("profile")
    source.profile.apply_hints(incremental=dlt.sources.incremental("created_at"))

    info = pipeline.run(source, write_disposition="append")
    print(info)

if __name__ == "__main__":
    print(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    print(os.environ['GOOGLE_SECRETS__CREDENTIALS'])
    # print(get_secret_data())
    # load_table_from_database(get_secret_data())