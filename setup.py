from setuptools import setup, find_packages

import catcher_modules


def get_requirements() -> list:
    with open('requirements.txt', 'r') as f:
        return f.readlines()


def extras() -> dict:
    modules = {
        'kafka': ["pykafka==2.8.*"],
        'couchbase': ["couchbase==2.5.*"],
        'postgres': ["sqlalchemy==1.3.*", "psycopg2==2.8.*"],
        'mssql': ["cython==0.29.*", "pymssql==2.1.*", "sqlalchemy==1.3.*"],
        'mysql': ["cython==0.29.*", "pymysql==0.9.*", "sqlalchemy==1.3.*"],
        'oracle': ["sqlalchemy==1.3.*", "cx_oracle==7.1.*"],
        'sqlite': ["sqlalchemy==1.3.*"],
        'redis': ["redis==3.2.*"],
        'mongodb': ["pymongo==3.8.*"],
        'docker': ["docker==3.7.*"],
        'elastic': ["elasticsearch==7.0.*"],
        's3': ["boto3==1.9.*"],
        'rabbit': ["pika==0.13.1"],
        'email': ["imbox==0.9.*"],
        'marketo': ["marketorestpython==0.5.*"],
        'airflow': ["cryptography==2.8.*"]
    }
    modules['all'] = list(set([item for sublist in modules.values() for item in sublist]))
    # don't try to install couchbase in travis
    modules['travis'] = [m for m in modules['all'] if not m.startswith('couchbase')]
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
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Topic :: Software Development :: Testing'
      ],
      extras_require=extras(),
      tests_require=['mock', 'pytest', 'requests']
      )
