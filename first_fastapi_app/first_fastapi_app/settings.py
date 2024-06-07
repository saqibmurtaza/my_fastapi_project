from starlette.config import Config
from starlette.datastructures import Secret

env_file_path = r'F:\my_fastapi_project\.env'

try:
    config = Config(env_file=env_file_path)
except FileNotFoundError:
    config = Config()

DATABASE_URL = config("DATABASE_URL", cast=Secret)
BOOTSTRAP_SERVER = config("BOOTSTRAP_SERVER", cast=str)
KAFKA_TOPIC = config("KAFKA_TOPIC", cast=str)
KAFKA_CONSUMER_GROUP_ID = config("KAFKA_CONSUMER_GROUP_ID", cast=str)

TEST_DATABASE_URL = config("TEST_DATABASE_URL", cast=Secret)