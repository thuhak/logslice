import os
import logging
from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base

from .config import conf


__all__ = ['session_scope', 'Logfile']


dbpath = conf.get('data', '/data/logslice')
if not os.path.exists(dbpath):
    os.makedirs(dbpath)
dbpath = os.path.join(dbpath, 'logslice.db')
engine = create_engine('sqlite://{}'.format(dbpath), echo=False)
DBSession = sessionmaker(bind=engine)
Base = declarative_base()


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
    last_update = Column(DateTime, nullable=False)


Base.metadata.create_all(engine)
