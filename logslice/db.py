import os
import logging
from contextlib import contextmanager
import enum

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Enum
from sqlalchemy.ext.declarative import declarative_base

from .config import conf


__all__ = ['session_scope', 'LogStat', 'Logfile']


dbpath = conf.get('data', '/data/logslice')
if not os.path.exists(dbpath):
    os.makedirs(dbpath)
dbpath = os.path.join(dbpath, 'logslice.db')
engine = create_engine('sqlite://{}'.format(dbpath), echo=False)
DBSession = sessionmaker(bind=engine)
Base = declarative_base()


class LogStat(enum.Enum):
    ready = 0
    running = 1
    file_deleted = 2
    parse_error = 3
    no_update = 4
    read_file_error = 5


@contextmanager
def session_scope():
    session = DBSession()
    try:
        yield session
        session.commit()
    except Exception as e:
        logging.error('db commit error, rolling back')
        session.rollback()
        raise e
    finally:
        session.close()


class Logfile(Base):
    __tablename__ = 'log'

    filename = Column(String(256), primary_key=True)
    inode = Column(Integer, nullable=False)
    pos = Column(Integer, nullable=False, server_default=0)
    last_update = Column(DateTime, nullable=True)
    status = Column(Enum(LogStat), server_default=LogStat.ready)


Base.metadata.create_all(engine)
