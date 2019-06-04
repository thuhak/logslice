import sys
from setuptools import setup


if sys.version_info < (3, 6):
    sys.stderr.write('This module requires at least Python 3.6\n')
    sys.exit(1)

require_libs = ['elasticsearch>=6.3.1', 'SQLAlchemy>=1.3.4']


with open('README.md', encoding='utf-8') as f:
    long_description = f.read().strip()


setup(
    name='logslice',
    description='simple log parser like logstash',
    long_description=long_description,
    long_description_content_type='text/markdown',
    version='1.1.0',
    author='thuhak',
    author_email='thuhak.zhou@nio.com',
    keywords='logparser',
    packages =['logslice'],
    url='https://github.com/thuhak/logslice',
    install_requires=require_libs)
