import os
import threading
import logging
from datetime import datetime
from queue import Queue
from time import sleep

from .db import session_scope, Logfile


__version__ = '1.0.0'
__author__ = 'thuhak.zhou@nio.com'


def default_parser(line, context=None):
    print(line)


class LogParser:
    """
    logfile parser
    """
    def __init__(self, filename: str, parser, que: Queue, encoding='utf-8', flush_interval=3, skip_error=False):
        """
        :param filename: path of logfile
        :param que: parsed result will be pushed to this queue
        :param encoding: encoding of logfile
        :param parser: callback function for parsing log
        :param flush_interval: every flush_interval seconds, latest running will be saved to database
        :param skip_error: if skip_error is True, when Exception occurs in parser function, continue parsing next line,
        otherwise stop processing.
        """

        self.filename = filename
        self.inode = os.stat(filename).st_ino
        with session_scope() as sess:
            data = sess.query(Logfile).filter(filename=filename).first()
            if data:
                if self.inode != data.inode:
                    data.inode = self.inode
                    data.pos = self.pos = 0
                    data.last_update = self.last_update = datetime.now()
                else:
                    self.pos = data.pos
                    self.last_update = data.last_update
            else:
                self.pos = 0
                self.last_update = datetime.now()
                data = Logfile(filename=filename,
                               inode=self.inode,
                               pos=self.pos,
                               last_update=self.last_update)
                sess.add(data)
            self._data = data
        self.que = que
        self.skip_error = skip_error
        self.parser = parser
        self.flush_interval = flush_interval
        self.encoding = encoding

        self.running = False
        self.context = None
        self.working_thread = None
        self._flag = threading.Event()
        self.start()

    def start(self):
        """start parser"""
        logging.info('starting logfile parser')
        self.working_thread = threading.Thread(target=self.run, daemon=True)
        self.working_thread.start()
        flush_thread = threading.Thread(target=self.flush, daemon=True)
        flush_thread.start()

    def stop(self):
        """stop parser"""
        logging.info('stopping logfile parser')
        self._flag.set()

    def _flush(self):
        """
        flush running to database
        """
        with session_scope() as sess:
            self._data.pos = self.pos
            self._data.inode = self.inode
            self._data.last_update = datetime.now()
            sess.merge(self._data)

    def flush(self):
        while True:
            if self._flag.is_set():
                if self.working_thread and self.working_thread.is_alive():
                    sleep(0.1)
                    continue
                else:
                    self._flush()
                    self.running = False
                    break
            else:
                sleep(self.flush_interval)
                self._flush()

    def run(self):
        """
        run parser
        """
        try:
            stream = open(self.filename, 'r', encoding=self.encoding)
            stream.seek(self.pos)
            self.running = True
        except Exception as e:
            logging.error('can not open file {}, reason: {}'.format(self.filename, str(e)))
            return
        while True:
            if self._flag.is_set():
                break
            pos = stream.tell()
            line = stream.readline().strip()
            if line:
                try:
                    result = self.parser(line, self.context)
                except Exception as e:
                    logging.error('parser error for line {}, error is {}'.format(line, str(e)))
                    if self.skip_error:
                        self.pos = pos
                        continue
                    else:
                        self.stop()
                else:
                    if result:
                        self.que.put(result)
                    self.pos = stream.tell()
