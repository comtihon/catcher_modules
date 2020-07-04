FROM comtihon/catcher_base:latest

WORKDIR /catcher_modules
COPY catcher_modules catcher_modules
COPY requirements.txt requirements.txt
COPY setup.py setup.py

## database client libraries
# client libraries for postgres, mysql, couchbase
RUN apk update \
  && apk add gcc g++ curl zip bash unixodbc-dev postgresql-dev libcouchbase-dev libffi-dev mariadb-connector-c-dev \
  && pip install Cython
# client library for mssql
RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.5.2.2-1_amd64.apk \
    && apk add --allow-untrusted msodbcsql17_17.5.2.2-1_amd64.apk

## languages for external step support
# install java
RUN apk --no-cache add openjdk11 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community
# install kotlin
RUN curl -s https://get.sdkman.io | bash
RUN bash -c 'source "/root/.sdkman/bin/sdkman-init.sh" && sdk install kotlin'
ENV PATH="/root/.sdkman/candidates/kotlin/current/bin:${PATH}"
# install node js
RUN apk add --update nodejs npm

RUN pip install -e "file://`pwd`#egg=catcher-modules[all]"

WORKDIR /opt/catcher/