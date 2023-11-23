import pandas as pd
from sqlalchemy import create_engine
from .private import DB_CFG


# DB_ENGINE
def get_db_engine():
    db_url = (
        f"mysql+pymysql://"
        f"{DB_CFG['db_id']}:{DB_CFG['db_pw']}@"
        f"{DB_CFG['db_host']}:{DB_CFG['db_port']}/"
        f"{DB_CFG['db_name']}"
    )
    db_engine = create_engine(db_url)
    return db_engine


class DB_CONTROLLER:
    def __init__(self, db_engine) -> None:
        self.db_engine = db_engine

    def get_dart_fundamental_df(self):
        sql = "select * from dart_fundamental_df"
        dart_fundamental_df = self.read_query(sql, self.db_engine)
        return dart_fundamental_df

    def get_dart_info_df(self):
        sql = "select * from dart_info_df"
        dart_info_df = self.read_query(sql, self.db_engine)
        return dart_info_df

    def get_fdr_info_df(self):
        sql = "select * from fdr_info_df"
        fdr_info_df = self.read_query(sql, self.db_engine)
        return fdr_info_df

    def get_fdr_ohlcv_df(self):
        sql = "select * from fdr_ohlcv_df"
        fdr_ohlcv_df = self.read_query(sql, self.db_engine)
        return fdr_ohlcv_df

    def get_pykrx_info_df(self):
        sql = "select * from pykrx_info_df"
        pykrx_info_df = self.read_query(sql, self.db_engine)
        return pykrx_info_df

    def get_krx_info_df(self):
        sql = "select * from krx_info_df"
        krx_info_df = self.read_query(sql, self.db_engine)
        return krx_info_df

    @staticmethod
    def read_query(sql, db):
        sql = sql.replace("\n", " ")
        return pd.read_sql_query(sql, db)


def get_db_controller():
    db_controller = DB_CONTROLLER(get_db_engine())
    return db_controller
