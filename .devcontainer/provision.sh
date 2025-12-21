#!/usr/bin/bash

# install development environment for PyGreSQL

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get -y upgrade

# install base utilities and configure time zone

ln -fs /usr/share/zoneinfo/UTC /etc/localtime
apt-get install -y apt-utils software-properties-common
ap-get install -y tzdata
dpkg-reconfigure --frontend noninteractive tzdata

apt-get install -y rpm wget zip

# install all supported Python versions

add-apt-repository -y ppa:deadsnakes/ppa
apt-get update

apt-get install -y python3.8 python3.8-dev python3.8-distutils
apt-get install -y python3.9 python3.9-dev python3.9-distutils
apt-get install -y python3.10 python3.10-dev python3.10-distutils
apt-get install -y python3.11 python3.11-dev python3.11-distutils 
apt-get install -y python3.12 python3.12-dev python3.12-venv
apt-get install -y python3.13 python3.13-dev python3.13-venv
apt-get install -y python3.14 python3.14-dev python3.14-venv

# install build and testing tool

python3.12 -m ensurepip --upgrade --default-pip
python3.13 -m ensurepip --upgrade --default-pip
python3.14 -m ensurepip --upgrade --default-pip

python3.8 -m pip install -U pip setuptools wheel build
python3.9 -m pip install -U pip setuptools wheel build
python3.10 -m pip install -U pip setuptools wheel build
python3.11 -m pip install -U pip setuptools wheel build
python3.12 -m pip install -U pip setuptools wheel build
python3.13 -m pip install -U pip setuptools wheel build
python3.14 -m pip install -U pip setuptools wheel build

pip install ruff

apt-get install -y tox clang-format
pip install -U tox

# install PostgreSQL client tools

apt-get install -y postgresql libpq-dev

for pghost in pg12 pg13 pg14 pg15 pg16 pg17 pg18
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
