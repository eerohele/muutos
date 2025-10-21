FROM postgres:17

RUN apt-get update && apt-get -y install postgresql-17-cron && rm -rf /var/lib/apt/lists/*
