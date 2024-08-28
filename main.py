from dotenv import load_dotenv
from pydantic import BaseModel 
from google.cloud import secretmanager
from google.oauth2 import service_account
from dlt.common import pendulum
from dlt.sources.credentials import ConnectionStringCredentials
from sql_database import sql_database, sql_table, Table 
from typing import Mapping, Union, Any, Generator
import os 
import datetime
import sqlalchemy as sa 
import dlt
import pandas as pd 
from mimesis import Field, Fieldset, Schema
from mimesis.enums import Gender, TimestampFormat
from mimesis.locales import Locale
from dlt.destinations.impl.filesystem.factory import filesystem as FS
import pytz
from pendulum.datetime import DateTime
import json
from dlt.common.schema.typing import (
    TColumnSchema,
)



field = Field(Locale.EN, seed=0xff)
fieldset = Fieldset(Locale.EN, seed=0xff)


class UserProfile(BaseModel):
    id: int
    name: str
    created_at: Union[datetime.datetime, str]
    updated_at: datetime.datetime

class OrderHistory(BaseModel):
    order_id: int 
    shipping_address: str 
    payment_method: str 
    created_at: Union[datetime.datetime, str] 
    updated_at: Union[datetime.datetime, str]

class OrderHistoryStaging(BaseModel): # Filtered out unlisted data
    payload: str
    order_id: int 
    processed_at: Union[datetime.datetime, str] 

    

load_dotenv()
credentials = service_account.Credentials.from_service_account_file(os.environ["GOOGLE_SECRETS__CREDENTIALS"])

def get_secret_data():
    secrets_prefix = os.environ["GCP_SECRET_MANAGER_PREFIX"]
    client = secretmanager.SecretManagerServiceClient(credentials= credentials)
    return {
        "db": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_DB/versions/local").payload.data.decode("UTF-8"),
        "host": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_HOST/versions/main").payload.data.decode("UTF-8"),
        "pass": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_PASS/versions/main").payload.data.decode("UTF-8"),
        "user": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_USER/versions/local").payload.data.decode("UTF-8"),
        "port": client.access_secret_version(name=f"{secrets_prefix}/DEV_PG_PORT/versions/main").payload.data.decode("UTF-8")
    }


@dlt.resource
def users():
    schema_definition = lambda: {
    "pk": field("increment"),
    "uid": field("uuid"),
    "name": field("text.word"),
    "version": field("version"),
    "timestamp": field("timestamp", fmt=TimestampFormat.POSIX),
    "owner": {
        "email": field("person.email", domains=["mimesis.name"]),
        "creator": field("full_name", gender=Gender.FEMALE),
    },
    "apiKeys": fieldset("token_hex", key=lambda s: s[:16], i=3),
    }

    schema = Schema(schema=schema_definition, iterations=10)
    schema.create()
    yield schema


@dlt.resource(max_table_nesting=0, columns=OrderHistoryStaging) # Save raw nested data and prevent auto-breakdown into child tables
def order_history():
    cred_mapping = get_secret_data()
    print(f"Cred Mapping:",  cred_mapping)
    credentials = ConnectionStringCredentials(
        f"postgresql+psycopg2://{cred_mapping.get('user')}:{cred_mapping.get('pass')}@{cred_mapping.get('host')}:{cred_mapping.get('port')}/{cred_mapping.get('db')}"
    )
    conn = sa.create_engine(credentials.to_url())
    # query = sa.text('SELECT * FROM public.order_history')
    query = 'SELECT * FROM public.order_history'
    # with conn.connect() as connection:
    #     data = connection.execute(
    #         query
    #     ).fetchall()
    for data in pd.read_sql_query(query, conn, chunksize= 100):    
        # print("Data sample, ", data.head(1))
        # print("Data Length :", len(data))
        # yield data.assign(
        # **data.select_dtypes(['datetime']).astype(str).to_dict('list')
        # ).to_dict('records')
        data['created_at'] = data['created_at'].map(str)
        data['updated_at'] = data['updated_at'].map(str)
        records = data.to_dict('records')
        yield [{"payload": json.dumps(datum, indent= 4),
                "order_id": datum.get("order_id"),
                "processed_at": str(datetime.datetime.now())
                } for datum in records]
        # yield records

# @dlt.destination(batch_size=1000, loader_file_format='parquet', name='custom_gcs_')


# @dlt.transformer(max_table_nesting=0, write_disposition= 'replace')
# def nest_data(data:Mapping[str, Any]) -> Generator[Mapping[str,Any], None, None]:
#     print("Data Length in transformer :", len(data))
#     yield [{
#         "payload": datum
#     } for datum in data] 




def load_table_from_database(cred_mapping: Mapping[str,str]) -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    This example sources data from the public Rfam MySQL database.
    """
    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="load_many_data_from_pg_2", 
        destination='bigquery', 
        dataset_name="dev_testing_load_2",
        staging='filesystem',  # add this to activate staging, 
        # staging = FS(bucket_url=dlt.config.get("destination.filesystem.bucket_url", ''),
        #                layout= "{table_name}/{prefix_id}_{load_id}.{file_id}.{ext}",
        #                extra_placeholders= {"prefix_id": f"nested_loading_{Field(Locale.EN, seed=0xff)('uuid')}"},
        #                current_datetime= DateTime.now(tz=pytz.timezone('Asia/Jakarta'))
        #                ),
        # pipelines_dir= os.path.join(os.getcwd(), "pipelines"),
        progress=dlt.progress.tqdm(colour="yellow"),
        # dev_mode= True,
        export_schema_path="schemas/export",
    )

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    credentials = ConnectionStringCredentials(
        f"postgresql+psycopg2://{cred_mapping.get('user')}:{cred_mapping.get('pass')}@{cred_mapping.get('host')}:{cred_mapping.get('port')}/{cred_mapping.get('db')}"
    )
    # conn = sa.create_engine(credentials.to_url())
    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source = sql_database(credentials, chunk_size= 1000,).with_resources("order_history")
    source.order_history.apply_hints(table_name="order_trx_bulk" , 
                                     columns=OrderHistory,
                                     incremental=dlt.sources.incremental("created_at"))
    # source.user_profile.apply_hints(table_name= "user_profile", columns=UserProfile, table_format='delta')
    # print(credentials.to_url())
    # source = order_history(cred_mapping).add_yield_map(nest_data)
    # source = order_history(cred_mapping)
    
    # info = pipeline.run(
    #                     source,
    #                     write_disposition="append",
    #                     refresh="drop_sources",
    #                     dev_mode=True
    #                     )
    info = pipeline.run(
                    # data = order_history,
                    data = source,
                    table_name='order_trx_bulk', 
                    refresh="drop_resources",
                    write_disposition= 'append',
                    primary_key= 'order_id',
                    schema_contract="evolve",
                    # columns=[
                    #     TColumnSchema(
                    #         name='payload', 
                    #         nullable=False, 
                    #         description="Contains data paylod in JSON dumps",
                    #                   ),
                    #     TColumnSchema(
                    #         name='order_id', 
                    #         nullable=False, 
                    #         description="Primary Key (Serial-Generated Incremented)",
                    #         primary_key= True,
                    #         unique=True,
                    #         cluster=True 
                    #                   ),
                    #     TColumnSchema(
                    #         name='processed_at', 
                    #         nullable=False, 
                    #         description="Processing time in extraction phase (UTC)",
                    #         partition=True,
                    #                   ),

                    # ]
                    columns=[
                        TColumnSchema(
                            name='order_id', 
                            nullable=False, 
                            description="Primary Key (Serial-Generated Incremented)",
                            primary_key= True,
                            unique=True,
                            cluster=True 
                                      ),
                        TColumnSchema(
                            name='shipping_address', 
                            nullable=False, 
                            description="Shipping Address",
                            primary_key= False,
                            unique=False,
                            cluster=False 
                                      ),
                        TColumnSchema(
                            name='payment_method', 
                            nullable=False, 
                            description="Payment Method",
                            primary_key= False,
                            unique=False,
                            cluster=True 
                                      ),
                        TColumnSchema(
                            name='created_at', 
                            nullable=False, 
                            description="Processing time in extraction phase (UTC)",
                            partition=True,
                                      ),
                        TColumnSchema(
                            name='updated_at', 
                            nullable=False, 
                            description="Processing time in extraction phase (UTC)",
                            partition=False,
                                      ),

                    ]
                    )
    
    print(info)

if __name__ == "__main__":
    # print(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    # print(os.environ['GOOGLE_SECRETS__CREDENTIALS'])
    # print(get_secret_data())
    # print(dlt.config.value)
    # print(dlt.config.values)
    load_table_from_database(get_secret_data())