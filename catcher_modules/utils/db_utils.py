from typing import Union

from catcher.utils.logger import warning

default_ports = {
    'postgresql': 5432,
    'mysql+pymysql': 3306,
    'mssql+pymssql': 1433,
    'mssql+pyodbc': 1433,
    'oracle+cx_oracle': 1521
}


def get_engine(conf: Union[str, dict], dialect: str = 'postgresql', driver: str = None):
    """
    :param conf: there are 3 options for configuring a database connection.
      1. String configuration. All parameters are included in single string:
         postgres:postgres@custom_postgres_1:5432/postgres
      2. String-object configuration. All parameters are included in single string under param `url`. Object contains
         other parameters:
             psql_conf:
                url: 'postgres:postgres@custom_postgres_1:5432/postgres'
                extra: '{"key": "value"}'
                type: 'postgres'
      3. Object configuratoin. All parameters are separated in the object.
         psql_conf:
            dbname: test
            user: user
            password: password
            host: localhost
            port: 5433
            extra: '{"key": "value"}'
            type: 'postgres'
    :param dialect: dialect configuration for sqlalchemy.
    :param driver: driver configuration for pyodbc. Optional
    """

    if not isinstance(conf, str):  # object or string-object representation
        if 'url' not in conf:  # object
            conf_str = __construct_str_configuration(conf, dialect)
        else:  # string-object
            conf_str = __fill_dialect(conf['url'], dialect)
    else:  # string representation
        conf_str = __fill_dialect(conf, dialect)

    if driver is not None and 'driver' not in conf_str:  # pyodbc
        conf_str += '?driver={}'.format(driver.replace(' ', '+'))
    from sqlalchemy import create_engine
    return create_engine(conf_str)


def __construct_str_configuration(conf, dialect):
    port = conf.get('port', default_ports.get(dialect.lower()))
    if port is None:
        port = ''
    else:
        port = ':{}'.format(port)
    return '{}://{}:{}@{}{}/{}'.format(dialect,
                                       conf['user'],
                                       conf['password'],
                                       conf['host'],
                                       port,
                                       conf['dbname'])


def __fill_dialect(url: str, driver: str):
    if '://' not in url:  # simple case - user:password@host:port/database
        return driver + '://' + url
    else:
        if '+' in url or url.startswith('postgresql'):
            return url  # dialect specified - mysql+pymysql://user:password@host:port/database
        else:  # no dialect - need to set up dialect from driver
            driver_used = url.split(':')[0]
            found = [k for k in default_ports.keys() if k.startswith(driver_used)]
            if not found:
                warning('Can\'t find dialect for driver ' + driver_used + '. Will try default ' + driver)
                found = [driver]
            return found[0] + ':' + ':'.join(url.split(':')[1:])
