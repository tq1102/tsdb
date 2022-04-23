import logging
from concurrent import futures

from retry import retry

from .setting import Setting


ts = __import__('tushare', globals(), locals(), [], 0)


class Fetcher(object):
    def __init__(self, parser, dumper):
        self.parser = parser
        self.dumper = dumper

        self.tracked = list()
        self.executor = futures.ThreadPoolExecutor(
            max_workers=Setting['max_workers'])

    def _func_obj(self):
        func_obj = eval(self.parser.func)
        return self._add_retry(self._add_log(func_obj))

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

    def _fetch(self, func_obj, kwargs, limit, executor):
        fetched = func_obj(**kwargs)
        if fetched and len(fetched):
            if len(fetched) == limit:
                next_end = fetched.iloc[-1][self.parser.cursor]
                kwargs['end_date'] = next_end

                future = executor.submit(
                    self._fetch,
                    func_obj, kwargs, limit, executor)
                self.tracked.append(future)
                return fetched[:-1]
            else:
                assert len(fetched) < limit
                return fetched

    def fetch(self):
        table_created = False
        func_obj = self._func_obj()

        for kwargs in self.parser._input:
            if not table_created:
                table_created = True
                self.dumper.create_table(
                    func_obj(**kwargs), self.parser.table)

            future = self.executor.submit(
                self._pipeline,
                func_obj, kwargs, self.parser.limit, self.executor)
            self.tracked.append(future)

    def _pipeline(self, *args, **kwargs):
        fetched = self._fetch(*args, **kwargs)
        if fetched and len(fetched) > 0:
            self.dumper.dump(fetched, self.parser.table)

    def wait(self):
        shutdown = False
        while not shutdown:
            if (self.executor._work_queue.empty() and
                    len(self.tracked) == 0):
                self.executor.shutdown()
                shutdown = True

            done = [(i, future)
                    for i, future
                    in enumerate(self.tracked)
                    if future.done()]
            idx_desc = sorted(done, key=lambda x: x[0], reverse=True)

            for i, future in idx_desc:
                future.result()
                del self.tracked[i]

            del done