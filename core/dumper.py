import pandas as pd
from sqlalchemy.types import VARCHAR, Date, DateTime


class Dumper(object):
    dtypes_map = {
        'ts_code': VARCHAR(length=255),
        'trade_date': Date(),
        'trade_time': DateTime(),
    }

    def __init__(self, engine, parser):
        self.engine = engine
        self.parser = parser
        self.trans_map = {}

    def rollback(self):
        for t in self.trans_map.values():
            t.rollback()

    def commit(self):
        for t in self.trans_map.values():
            t.commit()

    def dump(self, df, table):
        conn = self.engine.connect()
        self.trans_map.setdefault(conn, conn.begin())
        
        df.to_sql(table, conn, if_exists='append', index=False)

    def create_table(self, df, table):
        pandas_sql = pd.io.sql.pandasSQL_builder(self.engine.connect())

        sql_table = pd.io.sql.SQLTable(
            table,
            pandas_sql,
            frame=df,
            index=False,
            if_exists=self.parser.if_exists,
            dtype=self.dtypes_map,
            keys=self.parser.keys
        )
        sql_table.create()
