import logging
import sys
import pathlib
import datetime
import time

import yaml

from sqlalchemy import create_engine
from sqlalchemy.pool import SingletonThreadPool

from core import Fetcher, Dumper, Parser, Scheduler, Setting, SchemaManager


import tushare as ts

logging.getLogger().setLevel(logging.INFO)

DELAY = 10
PROJECT = pathlib.Path(__file__).resolve().parent

file = PROJECT.joinpath('setting.yaml')

ts.set_token(Setting['tushare_token'])
engine = create_engine(Setting['tsdb'], poolclass=SingletonThreadPool, pool_size=Setting['pool_size'])


def run():
    def from_argv():
        return dict(
            schema_path=SchemaManager.all()[sys.argv[1]],
            start_date=datetime.datetime.strptime(sys.argv[2], '%Y%m%d'),
            end_date=datetime.datetime.strptime(sys.argv[3], '%Y%m%d')
        )

    args = from_argv()
    parser = Parser(args['schema_path'], engine)
    parser.register('start_date', args['start_date'])
    parser.register('end_date', args['end_date'])

    dumper = Dumper(engine, parser)
    fetcher = Fetcher(parser, dumper)

    error = None
    try:
        fetcher.fetch()
        fetcher.wait()
        dumper.commit()

        stmt = "INSERT INTO schedule(schema, start_date, end_date) VALUES ( %s, %s, %s);"
        engine.connect().execute(stmt, sys.argv[1], sys.argv[2], sys.argv[3])
    except Exception as e:
        error = e
        dumper.rollback()
        time.sleep(DELAY)
    finally:
        engine.pool.dispose()
        if error:
            raise error


if __name__ == '__main__':
    try:
        run()
        time.sleep(DELAY)
    except Exception as e:
        print(e)
        time.sleep(DELAY)
        raise e
