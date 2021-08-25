FROM comtihon/catcher_base:latest

WORKDIR /catcher_modules
COPY catcher_modules catcher_modules
COPY requirements.txt requirements.txt
COPY setup.py setup.py

## database client libraries
# client libraries for postgres, mysql, couchbase
RUN apk update \
  && apk add wget git unzip build-base gcc abuild binutils binutils-doc g++ cmake ninja curl zip bash extra-cmake-modules \
  unixodbc-dev postgresql-dev libcouchbase-dev libffi-dev mariadb-connector-c-dev \
  && pip install Cython docutils
# client library for mssql
RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.5.2.2-1_amd64.apk \
    && apk add --allow-untrusted msodbcsql17_17.5.2.2-1_amd64.apk

## languages for external step support
# install java
RUN apk --no-cache add openjdk8
ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV PATH="$JAVA_HOME/bin:${PATH}"
# install kotlin
RUN curl -s https://get.sdkman.io | bash
RUN bash -c 'source "/root/.sdkman/bin/sdkman-init.sh" && sdk install kotlin'
ENV PATH="/root/.sdkman/candidates/kotlin/current/bin:${PATH}"
# install node js
RUN apk add --update nodejs npm

## browsers
RUN wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub \
 && wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.30-r0/glibc-2.30-r0.apk \
 && wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.30-r0/glibc-bin-2.30-r0.apk
RUN apk add glibc-2.30-r0.apk glibc-bin-2.30-r0.apk
RUN apk add firefox-esr chromium

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