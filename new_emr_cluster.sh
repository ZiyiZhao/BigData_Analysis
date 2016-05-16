export AWS_ACCESS_KEY_ID=AKIAJPJ5OBYD74NHT3TA
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-east-1

CLUSTER_NAME='serp-spark'
S3_BOOTSTRAP_URI='s3://serp-spark/spark_bootstrap/bootstrap_down.sh'
EMR_VERSION='emr-4.5.0'
LOG_URI='s3n://serp-spark/logs'
BID_PRICE='0.12'
INSTANCE_TYPE='m3.2xlarge'
INSTANCE_COUNT='20'
NUM_EXEC='20'
MEM='20G'
EXEC_CORES='8'

BOOTSTRAP="[{\"Path\":\"$S3_BOOTSTRAP_URI\",\"Name\":\"$CLUSTER_NAME\"}]"
EC2_ATTRS="{\"KeyName\":\"anthonyj\",\"InstanceProfile\":\"EMR_EC2_DefaultRole\"}"
INSTANCE_GROUPS="[{\"InstanceCount\":$INSTANCE_COUNT,\"BidPrice\":\"$BID_PRICE\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"$INSTANCE_TYPE\",\"Name\":\"Cores\"},{\"InstanceCount\":1,\"BidPrice\":\"$BID_PRICE\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"$INSTANCE_TYPE\",\"Name\":\"Master\"}]"

#https://forums.aws.amazon.com/thread.jspa?threadID=224489
EXTRA_CLASSES='/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*'

REMOTE_HOME='/home/hadoop'
STEP_NAME='RSToSpark Step'
JARS="$REMOTE_HOME/jars/RedshiftJDBC41-1.1.13.1013.jar,$REMOTE_HOME/jars/aws-java-sdk-1.10.55.jar,$REMOTE_HOME/jars/aws-java-sdk-core-1.10.55.jar,$REMOTE_HOME/jars/aws-java-sdk-s3-1.10.55.jar,$REMOTE_HOME/jars/google-collections-0.9.jar,$REMOTE_HOME/jars/hadoop-aws-2.7.1.jar,$REMOTE_HOME/jars/spark-redshift_2.10-0.6.0.jar,$REMOTE_HOME/jars/spark-avro_2.10-2.0.1.jar"

FILES=""
MAIN="$REMOTE_HOME/RedShift_into_Spark.py"
STEPS="[{\"Args\":[\"spark-submit\",\"--deploy-mode\",\"client\",\"--master\",\"yarn\",\"--driver-class-path\",\"$JARS\",\"--jars\",\"$JARS\",\"--conf\",\"spark.driver.extraClassPath=$EXTRA_CLASSES\",\"--conf\",\"spark.executor.extraClassPath=$EXTRA_CLASSES\",\"--num-executors\",\"$NUM_EXEC\",\"--executor-cores\",\"$EXEC_CORES\",\"--driver-memory\",\"$MEM\",\"--executor-memory\",\"$MEM\",\"$MAIN\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"CONTINUE\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"$STEP_NAME\"}]"

aws emr create-cluster --applications Name=Spark --bootstrap-actions "$BOOTSTRAP" \
  --ec2-attributes "$EC2_ATTRS" --service-role EMR_DefaultRole --release-label "$EMR_VERSION" \
  --log-uri "$LOG_URI" --name "$CLUSTER_NAME" --instance-groups "$INSTANCE_GROUPS" --steps "$STEPS" #--auto-terminate #--profile siteops+sandbox


# STEPS="[{\"Args\":[\"spark-submit\",\"--deploy-mode\",\"client\",\"--master\",\"yarn\",\"--py-files\",\"/home/hadoop/repscore.zip\",\"--jars\",\"$JARS\",\"--files\",\"$FILES\",\"--num-executors\",\"$NUM_EXEC\",\"--executor-cores\",\"$EXEC_CORES\",\"--driver-memory\",\"$MEM\",\"--executor-memory\",\"$MEM\",\"--conf\",\"spark.driver.extraClassPath=$EXTRA_CLASSES\",\"--conf\",\"spark.executor.extraClassPath=$EXTRA_CLASSES\",\"$MAIN\",\"$TENANT_IDS\"],\"Type\":\"CUSTOM_JAR\",\"ActionOnFailure\":\"CONTINUE\",\"Jar\":\"command-runner.jar\",\"Properties\":\"\",\"Name\":\"$STEP_NAME\"}]"