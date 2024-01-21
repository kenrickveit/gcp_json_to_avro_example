#!/usr/bin/env python

# Based upon the below:
# https://github.com/GoogleCloudPlatform/reliable-task-scheduling-compute-engine-sample/blob/master/gce/test_executor.py



import logging
import os
import sys

from cloud_handler import CloudLoggingHandler
from cron_executor import Executor

PROJECT = 'big-data-challenge-162202' 
TOPIC = 'json_to_avro_convert'

# Separated args from script_path, as os.path.abspath was removed the double forward slash from the bucket URLs
script_path = os.path.abspath(os.path.join(os.getcwd(), 'create_cluster_and_submit_job.py'))
script_args = ' --project_id=big-data-challenge-162202 --zone=us-central1-a --cluster_name=cluster-1 --init_actions=gs://bdc_source/install_avro.sh --spark_packages=com.databricks:spark-avro_2.11:3.2.0 --gcs_bucket=bdc_source  --pyspark_file=json_to_avro.py'
script_path = script_path + script_args

sample_task = "python -u %s" % script_path


root_logger = logging.getLogger('cron_executor')
root_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)

cloud_handler = CloudLoggingHandler(on_gce=True, logname="task_runner")
root_logger.addHandler(cloud_handler)

# create the executor that watches the topic, and will run the job task
test_executor = Executor(topic=TOPIC, project=PROJECT, task_cmd=sample_task, subname='watch_json_to_avro_convert_task')

# add a cloud logging handler and stderr logging handler
job_cloud_handler = CloudLoggingHandler(on_gce=True, logname=test_executor.subname)
test_executor.job_log.addHandler(job_cloud_handler)
test_executor.job_log.addHandler(ch)
test_executor.job_log.setLevel(logging.DEBUG)


# watches indefinitely
test_executor.watch_topic()
