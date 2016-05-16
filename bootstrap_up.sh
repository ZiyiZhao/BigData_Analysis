#!/bin/bash
BASE="s3://serp-spark/spark_bootstrap"

export AWS_ACCESS_KEY_ID=AKIAJPJ5OBYD74NHT3TA
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-east-1

aws s3 rm --recursive $BASE

aws s3 cp ./bootstrap_down.sh $BASE/bootstrap_down.sh
aws s3 cp --recursive ./jars $BASE/jars
aws s3 cp ./RedShift_into_Spark.py $BASE/RedShift_into_Spark.py

