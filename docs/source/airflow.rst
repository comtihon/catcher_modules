Airflow
=======

Configure connections
---------------------
:meth:`catcher_modules.pipeline.airflow` step allows you to configure airflow connections based on your inventory.
To do this you need to specify **populate_connections** and **fernet_key** in airflow step configuration::

    steps:
          - airflow:
              run:
                config:
                  db_conf: '{{ airflow_db }}'
                  url: '{{ airflow_web }}'
                  populate_connections: true
                  fernet_key: '{{ airflow_fernet }}'
                dag_id: '{{ pipeline }}'
                sync: true
                wait_timeout: 150
              name: 'Trigger pipeline {{ pipeline }}'

| For every connection specified in inventory catcher will try to create the connection with the same
  name in Airflow, if it **does not** already exists.

Then in your inventory you can have::

    psql_conf: 'postgresql://postgres:postgres@custom_postgres_1:5432/postgres'
    airflow_db: 'airflow:airflow@postgres:5432/airflow'
    airflow_web: 'http://webserver:8080'
    airflow_fernet: 'zp8kV516l9tKzqq9pJ2Y6cXbM3bgEWIapGwzQs6jio4='
    s3_config:
        url: http://minio:9000
        key_id: minio
        secret_key: minio123

And in your pipeline::

    postgres_conn_id = 'psql_conf'
    mysql_conn_id = 'mysql_conf'
    aws_conn_id = 's3_config'

    def my_step():
        psql_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

| You can check `Admin -> Connections` for newly created connections. Catcher will create psql_conf, s3_config and
  airflow_web. Airflow_db is skipped, as it was already created before and airflow_fernet is not a connection.
| You can safely use these connections in your pipeline.

| **Docker usage**
| If you run your airflow in docker-compose locally with all the dependencies available via
  **localhost:<docker_forwarded_port>** you probably use local inventory for Catcher with something like::

    psql_conf: 'postgres:postgres@localhost:5432/postgres'

| If you ask Catcher to initialize Airflow's connections based on your local inventory - it will create connection with
  **localhost** host, which won't work in docker, as localhost will point within the container, but not the host.
| Workaround for it is to run Catcher in docker within the same network as your Airflow and have another inventory which
  will point to containers::

    psql_conf: 'postgresql://postgres:postgres@custom_postgres_1:5432/postgres'

Checklist:

* make sure you specify inventory to run
* make sure you set proper fernet key
* make sure you set populate_connections: true
* make sure connections with same names do not exist in airflow