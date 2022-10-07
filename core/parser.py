from itertools import product

import yaml

from .yaml_ext import magic_loader
from .yaml_ext import R, Q, G


class Parser(object):
    def __init__(self, file, conn):
        self.config = yaml.load(
            open(file, "rb"),
            Loader=magic_loader()
        )
        self.conn = conn
        self.registry = {}

    def __getattr__(self, name):
        return self.config[name]

    @property
    def cursor(self):
        return self.config.get('cursor', None)

    @property
    def keys(self):
        return self.config.get('keys', None)

    @property
    def if_exists(self):
        return self.config.get('if_exists', 'append')

    @property
    def limit(self):
        return self.config.get('limit', float('inf'))

    @property
    def _input(self):
        try:
            self.resolve_g()
            self.resolve_q()
            return self.resolve_r()
        except Exception as e:
            raise e

    def register(self, k, v):
        self.registry[k] = v

    def resolve_r(self):
        rep = {}
        stay = {}

        for k in self.input:
            if isinstance(k, R):
                rep[k] = self.input[k]
            else:
                stay[k] = self.input[k]

        for p in product(*rep.values()):
            rep_input = dict(zip(rep, p))
            rep_input.update(stay)
            yield rep_input

    def resolve_q(self):
        def _select(con, _table, _column):
            from sqlalchemy import select
            from sqlalchemy import table, column

            stmt = select([column(_column)]).select_from(table(_table))
            ret = con.execute(stmt)
            for row in ret:
                for entry in row:
                    yield entry

        for k, v in self.input.items():
            if isinstance(v, Q):
                table, column = v.split('.')
                self.input[k] = _select(self.conn, table, column)

    def resolve_g(self):
        for k, v in self.input.items():
            if isinstance(v, G):
                self.input[k] = self.registry[k].strftime(v)

