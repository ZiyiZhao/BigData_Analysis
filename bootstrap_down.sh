#!/bin/bash
S3_BASE="s3://serp-spark/spark_bootstrap"
BASE="s3://serp-spark/"

aws s3 rm --recursive $BASE/temp/

# change timezone of instances to PST
sudo ln -sf /usr/share/zoneinfo/America/Los_Angeles /etc/localtime

# point default python to python2.7
sudo rm /usr/bin/python
sudo ln -s /usr/bin/python2.7 /usr/bin/python
sudo rm /usr/bin/pip
sudo ln -s /usr/bin/pip-2.7 /usr/bin/pip

aws s3 rm --recursive s3n://serp-spark/temp/

# COPY all bootstrap files to /home/hadoop
aws s3 cp --recursive $S3_BASE ~/
aws s3 cp --recursive $S3_BASE/jars ~/jars

# install deps
sudo yum install -y postgresql-devel # for pre-req for psycopg2
#sudo pip install -r /home/hadoop/requirements.txt
sudo pip install psycopg2==2.6.1
# sudo pip install pandas==0.17.1
