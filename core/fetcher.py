import logging
from concurrent import futures

from retry import retry

from .setting import Setting


ts = __import__('tushare', globals(), locals(), [], 0)


class Fetcher(object):
    def __init__(self, parser, dumper):
        self.parser = parser
        self.dumper = dumper

        self.finish = 0
        self.total = 0

        self.futures = []

        self.executor = futures.ThreadPoolExecutor(
            max_workers=Setting['concurrency'])
        self.func_obj = self._func_obj()

    def _func_obj(self):
        origin_func = eval(self.parser.func)
        return self._add_retry(self._add_log(origin_func))

    @staticmethod
    def _add_log(func):
        def wrapped(**kwargs):
            logging.info('{}{}'.format(str(func), kwargs))
            return func(**kwargs)
        return wrapped

    @staticmethod
    def _add_retry(func):
        decorator = retry(delay=Setting['retry_delay'])
        return decorator(func)

    def _fetch(self, kwargs, offset, limit):
        fetched = self.func_obj(**kwargs, offset=offset, limit=limit)
        if fetched is None or not len(fetched):
            self.finish += 1
        else:
            if limit is None or len(fetched) < limit:
                self.finish += 1
                return fetched

            elif len(fetched) == limit:
                offset += limit
                f = self.executor.submit(
                    self._pipelined_fetch, kwargs, offset, limit)
                self.futures.append(f)
                return fetched

    def _pipelined_fetch(self, *args, **kwargs):
        fetched = self._fetch(*args, **kwargs)
        if fetched is not None and len(fetched) > 0:
            self.dumper.put(fetched, self.parser.table)

    def fetch(self):
        table_created = False

        for kwargs in self.parser._input:
            if not table_created:
                self.dumper.create_table(
                    self.func_obj(**kwargs), self.parser.table)
                table_created = True

            f = self.executor.submit(
                self._pipelined_fetch, kwargs, 0, self.parser.limit)
            self.futures.append(f)

            self.total += 1

    def wait(self):
        shutdown = False
        while not (shutdown and self.dumper.queue.empty()):
            print(self.finish, r'/',  self.total,
                  self.dumper.queue.qsize(), self.executor._work_queue.qsize(),
                  ' ', end="\r")

            self.dumper.dump()

            if self.finish == self.total:
                shutdown = True


        for i, f in enumerate(self.futures):
            f.result()

        self.executor.shutdown(wait=True)