gen-certs:
  mkcert -key-file dev/certs/server.key -cert-file dev/certs/server.crt localhost

  openssl x509 -inform PEM -outform DER -in "$(mkcert -CAROOT)/rootCA.pem" | \
    keytool -import -alias myserver -keystore dev/jks/dev.jks -storepass muutos

test:
  clj -X:dev kaocha.runner/exec-fn
  clj -X:user:dev user/check!

pgbench-init:
  env PGPASSWORD=postgres pgbench --initialize --scale=10 --host=localhost --port=5432 --username=postgres --dbname=postgres
  env PGPASSWORD=postgres pgbench --initialize --scale=10 --host=localhost --port=5433 --username=postgres --dbname=postgres

pgbench-run:
  env PGPASSWORD=postgres pgbench --client=10 --jobs=2 --time=5 --host=localhost --port=5432 --username=postgres --dbname=postgres

pgbench: pgbench-init pgbench-run

pagila:
  @curl -sL https://raw.githubusercontent.com/devrimgunduz/pagila/refs/heads/master/pagila-schema.sql -o dev/sql/1-pagila-schema.sql
  @curl -sL https://raw.githubusercontent.com/devrimgunduz/pagila/refs/heads/master/pagila-schema-jsonb.sql -o dev/sql/2-pagila-schema-jsonb.sql
  @curl -sL https://raw.githubusercontent.com/devrimgunduz/pagila/refs/heads/master/pagila-data.sql -o dev/sql/3-pagila-data.sql

recvlogical:
  @pg_recvlogical -d postgres --slot my_slot --start --option=proto_version=2 --option=publication_names=p --host=localhost --port=5432 --username=postgres --password --file=-
