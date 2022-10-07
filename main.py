import logging
import sys
import datetime
import traceback

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

import tushare as ts

from core import Fetcher, Dumper, Parser, Setting, SchemaManager


logging.getLogger().setLevel(logging.INFO)

ts.set_token(Setting['tushare_token'])

engine = create_engine(Setting['tsdb'], poolclass=NullPool)

SCHEMA, START, END = 1, 2, 3


def from_argv():
    return dict(
        schema_path=SchemaManager.schemas()[sys.argv[SCHEMA]],
        start_date=datetime.datetime.strptime(sys.argv[START], '%Y%m%d'),
        end_date=datetime.datetime.strptime(sys.argv[END], '%Y%m%d')
    )


def run():
    args = from_argv()

    conn = engine.connect()
    trans = conn.begin()

    parser = Parser(args['schema_path'], conn)
    parser.register('start_date', args['start_date'])
    parser.register('end_date', args['end_date'])

    dumper = Dumper(conn, parser)
    fetcher = Fetcher(parser, dumper)

    error = None

    try:
        fetcher.fetch()
        fetcher.wait()

        stmt = "INSERT INTO schedule(schema, start_date, end_date) VALUES ( %s, %s, %s);"
        conn.execute(stmt, sys.argv[1], sys.argv[2], sys.argv[3])

        trans.commit()
    except Exception as e:
        error = e
        trans.rollback()
    finally:
        engine.pool.dispose()
        if error:
            raise error


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        traceback.print_exc()
        input('error...')
