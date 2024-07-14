from sqlalchemy import create_engine

import utils.config as settings

log = settings.log


class Connections:
    def connect_to_postgres(self):

        try:
            log.info("=====> Connecting to postgres ====>")
            engine = create_engine(
                f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}")
            conn = engine.connect()

            log.info("==> Connected to postgres")
            return conn

        except Exception as e:
            log.error("==> Error connecting to postgres")
            raise e


if __name__ == '__main__':
    conn = Connections()
    conn.connect_to_postgres()
