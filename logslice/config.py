from os import environ

from myconf import Conf


__all__ = ['conf']


config_file = environ.get('LOGSLICECONF', '/etc/logslice.yml')
conf = Conf(config_file)
