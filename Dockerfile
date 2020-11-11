FROM comtihon/catcher_base:latest

WORKDIR /catcher_modules
COPY catcher_modules catcher_modules
COPY requirements.txt requirements.txt
COPY setup.py setup.py

RUN apt-get update \
 && apt-get install -y curl gnupg

## add couchbase repo
RUN curl "https://packages.couchbase.com/clients/c/repos/deb/couchbase.key" | apt-key add  - \
 && echo "deb https://packages.couchbase.com/clients/c/repos/deb/debian10 buster buster/main" > "/etc/apt/sources.list.d/couchbase.list"

### add microsoft odbc repo
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
 && curl "https://packages.microsoft.com/keys/microsoft.asc" | apt-key add -

## database client libraries
# client libraries for postgres, mysql, couchbase
RUN apt-get update \
 && ACCEPT_EULA=Y apt-get install -y unzip zip unixodbc-dev libpq-dev libcouchbase-dev libffi-dev libmariadb-dev msodbcsql17 \
 && pip install Cython

## languages for external step support
# install java
RUN apt-get install -y apt-transport-https ca-certificates wget dirmngr gnupg software-properties-common \
 && curl "https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public" | apt-key add - \
 && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
 && apt-get update \
 && mkdir -p /usr/share/man/man1 \
 && apt-get install -y adoptopenjdk-8-hotspot

ENV JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"
# install kotlin
RUN curl -s https://get.sdkman.io | bash
RUN bash -c 'source "/root/.sdkman/bin/sdkman-init.sh" && sdk install kotlin'
ENV PATH="/root/.sdkman/candidates/kotlin/current/bin:${PATH}"
# install node js
RUN apt install -y nodejs npm

## browsers
RUN apt-get install -y firefox-esr chromium

## selenium drivers (firefox, chrome, opera)
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz \
  && mkdir -p /usr/lib/selenium && tar -xf geckodriver-v0.26.0-linux64.tar.gz -C /usr/bin/ \
  && chmod +x /usr/bin/geckodriver
RUN wget https://chromedriver.storage.googleapis.com/84.0.4147.30/chromedriver_linux64.zip \
  && unzip chromedriver_linux64.zip -d /usr/bin/ \
  && chmod +x /usr/bin/chromedriver
RUN wget https://github.com/operasoftware/operachromiumdriver/releases/download/v.83.0.4103.97/operadriver_linux64.zip \
  && unzip operadriver_linux64.zip -d /usr/bin/ \
  && chmod +x /usr/bin/operadriver_linux64

RUN pip install -e "file://`pwd`#egg=catcher-modules[all]"
WORKDIR /opt/catcher/

## selenium libraries
RUN npm install selenium-webdriver
RUN mkdir -p /usr/share/java \
 && wget https://selenium-release.storage.googleapis.com/3.141/selenium-java-3.141.59.zip \
 && unzip selenium-java-3.141.59.zip -d /usr/share/java \
 && mv /usr/share/java/libs/*.jar /usr/share/java/
RUN wget https://repo1.maven.org/maven2/com/google/auto/service/auto-service-annotations/1.0-rc7/auto-service-annotations-1.0-rc7.jar -P /usr/share/java/