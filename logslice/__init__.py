"""
simple log parser like logstash
"""
import os
import threading
import logging
import re
from datetime import datetime
from queue import Queue
from time import sleep
from glob import glob
from weakref import WeakValueDictionary

from .db import session_scope, Logfile, LogStat

__version__ = '1.0.0'
__author__ = 'thuhak.zhou@nio.com'


def is_thread_running(thread: threading.Thread) -> bool:
    return thread.is_alive() if thread is not None else False


class LogParser:
    """
    logfile parser
    """
    _instances = WeakValueDictionary()

    def __new__(cls, filename, que, encoding='utf-8', flush_interval=3, stop_time=30):
        """flyweight"""
        if filename in cls._instances:
            instance = cls._instances[filename]
            instance.que = que
            instance.encoding = encoding
            instance.flush_interval = flush_interval
            instance.stop_time = stop_time
        else:
            instance = super().__new__(cls)
            cls._instances[filename] = instance
        return instance

    def __init__(self, filename: str, que: Queue, encoding='utf-8', flush_interval=3, stop_time=30):
        """
        :param filename: path of logfile
        :param que: parsed result will be pushed to this queue
        :param encoding: encoding of logfile
        :param flush_interval: every flush_interval seconds, latest running will be saved to database
        :param stop_time: stop parser when file not changed with stop_time seconds
        """
        self.filename = filename
        self.inode = os.stat(filename).st_ino
        with session_scope() as sess:
            data = sess.query(Logfile).filter_by(filename=filename).first()
            if data:
                if self.inode != data.inode:
                    data.inode = self.inode
                    data.pos = self.pos = 0
                    data.last_update = self.last_update = None
                else:
                    self.pos = data.pos
                    self.last_update = data.last_update
            else:
                self.pos = 0
                self.last_update = None
                data = Logfile(filename=filename, inode=self.inode)
                sess.add(data)
            self._data = data
        self.que = que
        self.flush_interval = flush_interval
        self.stop_time = stop_time
        self.encoding = encoding

        self.running = False
        self.status = LogStat.ready
        self.context = None
        self._working_thread = None
        self._flush_thread = None
        self._flag = threading.Event()
        self.start()

    def parser(self, line):
        """
        default parser, you can define you own parser, override the default one
        """
        return line

    def start(self):
        """start parser"""
        logging.info('starting logfile parser')
        self._working_thread = threading.Thread(target=self.run, daemon=True)
        self._working_thread.start()
        self._flush_thread = threading.Thread(target=self.flush, daemon=True)
        self._flush_thread.start()

    def stop(self):
        """stop parser"""
        logging.info('stopping logfile parser')
        self._flag.set()
        while True:
            if not is_thread_running(self._flush_thread) and not is_thread_running(self._working_thread):
                self.running = False
                break
            sleep(0.1)

    def _flush(self):
        """
        flush running to database
        """
        try:
            with session_scope() as sess:
                self._data.pos = self.pos
                self._data.inode = self.inode
                self._data.last_update = datetime.now()
                self._data.status = self.status
                sess.merge(self._data)
        except Exception as e:
            logging.error('can not save parser status to database, reason {}'.format(str(e)))

    def flush(self):
        """
        run _flush job every self.flush_interval seconds
        """
        while True:
            if not is_thread_running(self._working_thread):
                self._flush()
                self.running = False
                break
            elif self._flag.is_set():
                sleep(0.1)
                continue
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
            self.status = LogStat.read_file_error
            return
        while True:
            if self._flag.is_set():
                break
            line = stream.readline().strip()
            if line:
                try:
                    result = self.parser(line)
                except Exception as e:
                    logging.error('parser error for line {}, error is {}'.format(line, str(e)))
                    break
                else:
                    if result:
                        self.que.put(result)
                    self.status = LogStat.running
                    self.pos = stream.tell()
                    self.last_update = datetime.now()
            else:
                if not os.path.exists(self.filename):
                    logging.info('log file {} no longer exists'.format(self.filename))
                    self.status = LogStat.file_deleted
                    break
                else:
                    stat = os.stat(self.filename)
                    inode, mtime = stat.st_ino, stat.st_mtime
                    if inode != self.inode:
                        stream.close()
                        self.pos = 0
                        self.last_update = None
                        self.inode = inode
                        self.status = LogStat.ready
                        try:
                            stream = open(self.filename, 'r', encoding=self.encoding)
                        except Exception as e:
                            logging.error('can not open file {}, reason: {}'.format(self.filename, str(e)))
                            self.status = LogStat.read_file_error
                            raise e
                    elif self.last_update and \
                            self.last_update.timestamp() - mtime >= self.stop_time:
                        logging.info('{} has no changing over {} seconds,stop parsing'.format(self.filename,
                                                                                              self.stop_time))
                        self.status = LogStat.no_update
                        break
                    else:
                        sleep(1)
        stream.close()


class LogSlice:
    """
    log parser manager
    """

    def __init__(self, path: list, output, parser=LogParser, file_filter=None, encoding='utf-8', recurse=True,
                 flush_interval=3, rescan_interval=10, close_file_time=30):
        """
        :param path: search log files using globbing patterns
        :param output: output object
        :param parser: parser class
        :param file_filter: regex filter
        :param encoding: file encoding
        :param recurse: recurse or not
        :param flush_interval: every flush_interval seconds, latest running will be saved to database
        :param rescan_interval: every rescan_interval seconds, rescan logs
        :param close_file_time: close file which not changed within close_file_time seconds
        """
        self.path = path
        self.output = output
        self.parser = parser
        self.filter = None if file_filter is None else re.compile(file_filter)
        self.encoding = encoding
        self.recurse = recurse
        self.flush_interval = flush_interval
        self.rescan_interval = rescan_interval
        self.close_file_time = close_file_time

        self.que = Queue()
        self.jobs = []
        self._flag = threading.Event()

    def scan_logs(self) -> set:
        """
        search logs in path
        :return: all log files
        """
        all_logs = set()
        for logpath in self.path:
            logs = set(glob(logpath, recursive=self.recurse))
            if self.filter:
                logs = {x for x in logs if self.filter.match(x)}
            all_logs |= logs
        return all_logs

    def start(self):
        ...

    def stop(self):
        self._flag.set()
        for job in self.jobs:
            job.stop()

    def clean_stopped_jobs(self):
        self.jobs = [job for job in self.jobs if job.running]

    def scan(self):
        while True:
            if self._flag.is_set():
                break
            self._scan()
            sleep(self.rescan_interval)

    def _scan(self):
        self.clean_stopped_jobs()
        logs = self.scan_logs()
        no_update_logs = set()
        with session_scope() as sess:
            check_logs = sess.query(Logfile).filter(Logfile.status == LogStat.no_update, Logfile.filename.in_(logs))
            for log in check_logs:
                try:
                    mtime = os.stat(log.filename).st_mtime
                    if mtime < log.last_update.timestamp():
                        no_update_logs.add(log.filename)
                except Exception as e:
                    logging.error('read file {} error, reason {}'.format(log.filename, str(e)))
                    continue
        logs = logs - no_update_logs
        self.jobs = [self.parser(filename=f, que=self.que, encoding=self.encoding, flush_interval=self.flush_interval,
                                 stop_time=self.close_file_time) for f in logs]
