import queue

import pandas as pd
from sqlalchemy.types import VARCHAR, Date, DateTime

from .setting import Setting

class Dumper(object):
    def dtypes_map(self, df):
        mapping = {}
        for field in df.columns:
            if field.endswith('code'):
                mapping[field] = VARCHAR(length=255)
            if field.endswith('date'):
                mapping[field] = Date()
            if field.endswith('time'):
                mapping[field] = DateTime()
        return mapping

    def __init__(self, conn, parser):
        self.conn = conn
        self.parser = parser
        self.queue = queue.Queue(maxsize=Setting['concurrency'])


    def dump(self):
        df, table = self.queue.get()
        df.to_sql(table, self.conn, if_exists='append', index=False)

    def put(self, *df_n_table):
        self.queue.put(df_n_table)

    def create_table(self, df, table):
        # regular table
        pandas_sql = pd.io.sql.pandasSQL_builder(self.conn)

        sql_table = pd.io.sql.SQLTable(
            table,
            pandas_sql,
            frame=df,
            index=False,
            if_exists=self.parser.if_exists,
            dtype=self.dtypes_map(df),
            keys=self.parser.keys
        )

        is_new = not sql_table.exists()
        sql_table.create()
        if is_new:
            if self.parser.cursor:
                # hypertable
                hypertable_query = """
                SELECT create_hypertable(
                '{table}',
                '{time}',
                chunk_time_interval => INTERVAL '1 month'
                );
                """.format(table=table, time=self.parser.cursor)

                # index
                index_query = """
                CREATE INDEX {table}_symbol_time ON {table} (ts_code, {time} DESC);
                """.format(table=table, time=self.parser.cursor)

                # execute
                self.conn.execute(hypertable_query)
                self.conn.execute(index_query)
