# 4.1.0
## New
* Airflow step
* S3 step has now delete option.

# 4.0.0
## Incompatible changes:
* every step based on sqlalchemy return value is now dict, where keys are column names got from the database. In case of 
write operations rowcount is returned.

# 3.7.0
## New
* Marketo step.

# 3.6.0
## New
* Airflow step.

# 3.5.0
## New
* Email send/receive step.

# 3.4.0
## New
* Prepare - expect step. Supported in all subclasses of `SqlAlchemyDb`

# 3.3.0
## New
* Rabbit step.

# 3.2.0
## New
* S3 step

# 3.1.0
## New
* add ElasticSearch step
## Fix
* fix kafka server url and group_id

# 3.0.0
## Incompatible changes:
* `Redis`: Syntax change. Use command:value instead of list.
* `Redis`: all objects are converted to string before put to redis.
