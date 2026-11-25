#!/usr/bin/bash

# install development environment for PyGreSQL

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get -y upgrade

# install base utilities and configure time zone

ln -fs /usr/share/zoneinfo/UTC /etc/localtime
apt-get install -y apt-utils software-properties-common
apt-get install -y tzdata
dpkg-reconfigure --frontend noninteractive tzdata

apt-get install -y rpm wget zip

# install all supported Python versions
# (Python 3.12 is the system Python, the others come from deadsnakes)

add-apt-repository -y ppa:deadsnakes/ppa
apt-get update

for v in 3.10 3.11 3.12 3.13 3.14 3.15
do
    apt-get install -y python$v python$v-dev python$v-venv
done

# install build and testing tools
# (tox creates its own virtual environments for each Python version)

apt-get install -y pipx clang-format

export PIPX_HOME=/opt/pipx
export PIPX_BIN_DIR=/usr/local/bin

pipx install tox
pipx install ruff

# install the latest PostgreSQL client tools and libpq from PGDG

apt-get install -y postgresql-common
/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
apt-get install -y postgresql-client libpq-dev

for pghost in pg12 pg13 pg14 pg15 pg16 pg17 pg18 pg19
do
    export PGHOST=$pghost
    export PGDATABASE=postgres
    export PGUSER=postgres
    export PGPASSWORD=postgres

    createdb -E UTF8 -T template0 test
    createdb -E SQL_ASCII -T template0 test_ascii
    createdb -E LATIN1 -l C -T template0 test_latin1
    createdb -E LATIN9 -l C -T template0 test_latin9
    createdb -E ISO_8859_5 -l C -T template0 test_cyrillic

    # createrole for the tests of change_password() with a temporary role
    psql -c "create user test with createrole password 'test'"

    psql -c "grant create on database test to test"
    psql -c "grant create on database test_ascii to test"
    psql -c "grant create on database test_latin1 to test"
    psql -c "grant create on database test_latin9 to test"
    psql -c "grant create on database test_cyrillic to test"

    psql -c "grant create on schema public to test" test
    psql -c "grant create on schema public to test" test_ascii
    psql -c "grant create on schema public to test" test_latin1
    psql -c "grant create on schema public to test" test_latin9
    psql -c "grant create on schema public to test" test_cyrillic

    psql -c "create extension hstore" test
    psql -c "create extension hstore" test_ascii
    psql -c "create extension hstore" test_latin1
    psql -c "create extension hstore" test_latin9
    psql -c "create extension hstore" test_cyrillic

    # enable SSL with the self-signed certificate of the image, so that the
    # tests can also cover SSL connections (they request SSL explicitly)
    psql -c "alter system set ssl_cert_file = '/etc/ssl/certs/ssl-cert-snakeoil.pem'"
    psql -c "alter system set ssl_key_file = '/etc/ssl/private/ssl-cert-snakeoil.key'"
    psql -c "alter system set ssl = on"
    psql -c "select pg_reload_conf()"
done

export PGDATABASE=test
export PGUSER=test
export PGPASSWORD=test
