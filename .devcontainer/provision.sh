#!/usr/bin/bash

# install development environment for PyGreSQL

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt-get -y upgrade

# install base utilities and configure time zone

sudo ln -fs /usr/share/zoneinfo/UTC /etc/localtime
sudo apt-get install -y apt-utils software-properties-common
sudo apt-get install -y tzdata
sudo dpkg-reconfigure --frontend noninteractive tzdata

sudo apt-get install -y rpm wget zip

# install all supported Python versions

sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update

sudo apt-get install -y python3.7 python3.7-dev python3.7-distutils
sudo apt-get install -y python3.8 python3.8-dev python3.8-distutils
sudo apt-get install -y python3.9 python3.9-dev python3.9-distutils
sudo apt-get install -y python3.10 python3.10-dev python3.10-distutils
sudo apt-get install -y python3.11 python3.11-dev python3.11-distutils
sudo apt-get install -y python3.12 python3.12-dev python3.12-distutils

# install build and testing tool

python3.7 -m pip install build
python3.8 -m pip install build
python3.9 -m pip install build
python3.10 -m pip install build
python3.11 -m pip install build

pip install ruff

sudo apt-get install -y tox clang-format

# install PostgreSQL client tools

sudo apt-get install -y postgresql libpq-dev

for pghost in pg10 pg12 pg14 pg15 pg16
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

    psql -c "create user test with password 'test'"

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
done

export PGDATABASE=test
export PGUSER=test
export PGPASSWORD=test
