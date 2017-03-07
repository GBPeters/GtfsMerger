#!/usr/bin/env bash

export PGPASSWORD=postgres

# Install postgres and postgis
echo "Installing PostgreSQL and PostGIS extension..."
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt xenial-pgdg main" >> /etc/apt/sources.list'
wget --quiet -O - http://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y postgresql-9.5-postgis-2.2 pgadmin3 postgresql-contrib-9.5 postgresql-9.5-postgis-2.2-scripts postgresql-server-dev-9.5 libpq-dev

# Install python and other dependencies
echo "Installing Python and dependencies..."
sudo apt-get install python python-pip
sudo pip install -y --upgrade pip
sudo pip install psycopg2

# Create database
echo "Configuring database..."
sudo -u postgres psql -d postgres -c "CREATE DATABASE gtfs"
sudo -u postgres psql -d gtfs -c "CREATE EXTENSION postgis;"

# Clone git
mkdir git
cd git
git clone http://github.com/GBPeters/GtfsMerger
