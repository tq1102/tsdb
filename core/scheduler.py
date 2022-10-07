import sys
import subprocess
from pathlib import Path
import datetime


from sqlalchemy import String, DateTime, Integer
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy import select, func


PROJECT = Path(__file__).resolve().parent.parent

Base = declarative_base()


class SchemaManager(object):
    @staticmethod
    def schemas():
        root = PROJECT.joinpath('schemas')
        paths = root.rglob('*.yaml')
        return {p.stem: p for p in paths}

    @staticmethod
    def sorted():
        independent = {}
        dependent = {}
        for schema, path in SchemaManager.schemas().items():
            with open(path, "r") as f:
                if '!Q' in f.read():
                    dependent[schema] = path
                else:
                    independent[schema] = path
        independent.update(dependent)
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
        self.engine = create_engine(setting['tsdb'])

        ScheduleRecord.create_table(self.engine)

    def query(self):
        query = select([
            ScheduleRecord.schema,
            func.max(ScheduleRecord.end_date),
        ]).group_by(ScheduleRecord.schema)

        return self.engine.execute(query).fetchall()

    def update_plan(self):
        intent = {k: v for k, v in SchemaManager.sorted().items()
                     if k in self.setting['schemas']}
        progress = dict(self.query())

        yesterday = datetime.date.today() + datetime.timedelta(-1)
        end = yesterday.strftime('%Y%m%d')

        for schema in intent:
            start = progress[schema].strftime('%Y%m%d') if schema in progress else '10010101'
            yield schema, start, end

    def update(self):
        main = PROJECT.joinpath('main.py')
        tasks = self.update_plan()

        for schema, start, end in tasks:
            cmds = [sys.executable, main, schema, start, end]
            last = subprocess.Popen(cmds,
                                    creationflags=subprocess.CREATE_NEW_CONSOLE)
            last.wait()


if __name__ == '__main__':
    from core.setting import Setting
    s = Scheduler(Setting)
    s.update()
