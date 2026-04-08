import os
from dotenv import load_dotenv

load_dotenv()


def get_connection_params() -> dict:
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ["SNOWFLAKE_ROLE"],
    }


def get_session():
    from snowflake.snowpark import Session
    return Session.builder.configs(get_connection_params()).create()
