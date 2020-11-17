class OldAirflowVersionException(Exception):
    """
    Old versions of airflow doesn't have rest api (or partly have it).
    In case such exception it would be better to do the same via Airflow db (if possible)
    """
    pass
