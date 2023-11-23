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
