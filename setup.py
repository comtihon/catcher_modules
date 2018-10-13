from setuptools import setup, find_packages

import catcher_modules


def get_requirements() -> list:
    with open('requirements.txt', 'r') as f:
        return f.readlines()


def extras() -> dict:
    modules = {
        'kafka': ["pykafka"],
        'couchbase': ["couchbase"],
        'postgres': ["sqlalchemy", "psycopg2"],
        'mssql': ["cython", "pymssql", "sqlalchemy"],
        'mysql': ["cython", "pymysql", "sqlalchemy"],
        'oracle': ["sqlalchemy", "cx_oracle"],
        'sqlite': ["sqlalchemy"],
        'redis': ["redis"],
        'mongodb': ["pymongo"],
    }
    modules['all'] = list(set([item for sublist in modules.values() for item in sublist]))
    return modules


setup(name=catcher_modules.APPNAME,
      version=catcher_modules.APPVSN,
      description='Additional modules for catcher.',
      author=catcher_modules.APPAUTHOR,
      author_email='valerii.tikhonov@gmail.com',
      url='https://github.com/comtihon/catcher_modules',
      packages=find_packages(),
      install_requires=get_requirements(),
      include_package_data=True,
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development :: Testing'
      ],
      extras_require=extras(),
      tests_require=['mock', 'pytest']
      )
