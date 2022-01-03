from setuptools import setup, find_packages

import catcher_modules


def get_requirements() -> list:
    with open('requirements.txt', 'r') as f:
        return f.readlines()


def extras() -> dict:
    modules = {
        'kafka': ["pykafka==2.8.0"],
        'couchbase': ["couchbase==3.2.4"],
        'postgres': ["sqlalchemy==1.4.29", "psycopg2==2.9.3"],
        'mssql': ["pyodbc==4.0.32", "sqlalchemy==1.4.29"],
        'mysql': ["cython==0.29.26", "pymysql==1.0.2", "sqlalchemy==1.4.29"],
        'oracle': ["sqlalchemy==1.4.29", "cx_oracle==8.3.0"],
        'sqlite': ["sqlalchemy==1.4.29"],
        'redis': ["redis==4.1.0"],
        'mongodb': ["pymongo==3.12.3", "sqlalchemy==1.4.29"],
        'docker': ["docker==5.0.3"],
        'elastic': ["elasticsearch==7.16.2"],
        's3': ["boto3==1.20.26"],
        'rabbit': ["pika==1.2.0"],
        'email': ["imbox==0.9.8"],
        'marketo': ["marketorestpython==0.5.14"],
        'airflow': ["cryptography==36.0.1"],
        'selenium': ["selenium==4.1.0"],
        'salesforce': ["simple-salesforce==1.11.4"]
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
      package_data={'catcher_modules': ['resources/*']},
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Topic :: Software Development :: Testing'
      ],
      extras_require=extras(),
      tests_require=['mock', 'pytest', 'requests']
      )
