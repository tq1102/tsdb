import sys
import subprocess
from pathlib import Path


from sqlalchemy import String, DateTime, Integer
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base


PROJECT = Path(__file__).resolve().parent.parent

Base = declarative_base()


class SchemaManager(object):
    @staticmethod
    def all():
        root = PROJECT.joinpath('schemas')
        paths = root.rglob('*.yaml')
        return {p.stem: p for p in paths}

    @staticmethod
    def ind(all):
        independent = {}
        for schema, path in all.items():
            with open(path, "r") as f:
                if '!Q' in f.read():
                    continue
                else:
                    independent[schema] = path
        return independent


class ScheduleRecord(Base):
    __tablename__ = "schedule"

    id = Column(Integer, primary_key=True)
    schema = Column(String(30))
    start_date = Column(DateTime())
    end_date = Column(DateTime())

    @staticmethod
    def create_table(engine):
        Base.metadata.create_all(engine)


class Scheduler(object):
    def __init__(self, setting):
        self.setting = setting
        self.engine = create_engine(setting['db'])

        ScheduleRecord.create_table(self.engine)

    def _setting_schemas(self):
        return {k: v for k, v in SchemaManager.all().items()
                if k in self.setting['schemas']}

    def figure_task(self):
        """
        according to db & independent_schema
        """
        pass

    def deliver_task(self):
        """
        TODO: code stub for now
        """
        # return [
        #     [('stock_basic', '10010101', '20220410')],
        #     [('hfq_daily', '10010101', '20220410'), ('one_min_bar', '10010101', '20220410')]
        # ]
        return [
            [('stock_basic', '10010101', '20220409')],
            [('hfq_daily', '20220410', '20220420')]
        ]

    def run(self):
        main = self.setting['subprocess']
        in_tasks, de_tasks = self.deliver_task()

        for schema, start, end in in_tasks:
            cmds = [sys.executable, main, schema, start, end]
            last = subprocess.Popen(cmds,
                                    creationflags=subprocess.CREATE_NEW_CONSOLE)
        last.wait()

        for schema, start, end in de_tasks:
            cmds = [sys.executable, main, schema, start, end]
            subprocess.Popen(cmds,
                             creationflags=subprocess.CREATE_NEW_CONSOLE)


if __name__ == '__main__':
    from .setting import Setting
    s = Scheduler(Setting)
    s.run()