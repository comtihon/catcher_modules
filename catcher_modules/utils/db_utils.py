from typing import Union

default_ports = {
    'postgresql': 5432,
    'mysql+pymysql': 3306,
    'mssql+pymssql': 1433,
    'oracle+cx_oracle': 1521
}


def get_engine(conf: Union[str, dict], driver: str = 'postgresql'):
    if not isinstance(conf, str):
        conf_str = '{}://{}:{}@{}:{}/{}'.format(driver,
                                                conf['user'],
                                                conf['password'],
                                                conf['host'],
                                                conf.get('port', default_ports.get(driver.lower(), '')),
                                                conf['dbname'])
    else:
        conf_str = driver + '://' + conf

    from sqlalchemy import create_engine
    return create_engine(conf_str)
