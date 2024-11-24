# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("Host", "", "Databricks Workspace URL")
dbutils.widgets.text("AccessToken", "", "Secure Access Token")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
host = dbutils.widgets.get("Host")
token = dbutils.widgets.get("AccessToken")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------

SH = SetupHelper(env)
SH.cleanup()

# COMMAND ----------

#Can create this jobs json through workflows ui. 
job_payload = \
{
        "name": "stream-test",
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [ #Task is to run notebook in defined location in workspace
            {
                "task_key": "stream-test-task",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "/Repos/SBIT/SBIT/07-run",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Job_cluster", #Run on job cluster defined below
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12", 
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.master": "local[*, 4]", #single node cluster
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1
                    },
                    "node_type_id": "Standard_DS4_v2",
                    "driver_node_type_id": "Standard_DS4_v2",
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "format": "MULTI_TASK"
    }

# COMMAND ----------

#create job using databricks jobs/create api
import requests
import json
create_response = requests.post(host + '/api/2.1/jobs/create', data=json.dumps(job_payload), auth=("token", token)) #requests.post to make rest api call in python. Pass the above json. Do json.dumps to convert to proper json file
print(f"Response: {create_response}")
job_id = json.loads(create_response.content.decode('utf-8'))["job_id"] #take out job id from create_response
print(f"Created Job {job_id}")

# COMMAND ----------

run_payload = {"job_id": job_id, "notebook_params": {"Environment":env, "RunType": "stream", "ProcessingTime": "1 seconds"}}
run_response = requests.post(host + '/api/2.1/jobs/run-now', data=json.dumps(run_payload), auth=("token", token))
run_id = json.loads(run_response.content.decode('utf-8'))["run_id"]
print(f"Started Job run {run_id}")

# COMMAND ----------

#run job using jobs/runs api. Pass the job id and also variales for the run script. (run_payload)
#See response for jobs/runs/get https://docs.databricks.com/api/workspace/jobs/get
#life_cycle_state shows pending or runnung or terminated depending on state of job.
#Wait until job has started before proceeding with processing the test cases below
import time
status_payload = {"run_id": run_id}
job_status="PENDING"
while job_status == "PENDING":
    time.sleep(20)
    status_job_response = requests.get(host + '/api/2.1/jobs/runs/get', data=json.dumps(status_payload), auth=("token", token))
    job_status = json.loads(status_job_response.content.decode('utf-8'))["tasks"][0]["state"]["life_cycle_state"]  
    print(job_status)    

# COMMAND ----------

# MAGIC %run ./03-history-loader

# COMMAND ----------

# MAGIC %run ./10-producer

# COMMAND ----------

# MAGIC %run ./04-bronze

# COMMAND ----------

# MAGIC  %run ./05-silver

# COMMAND ----------

# MAGIC %run ./06-gold

# COMMAND ----------

#Above code will read raw data in landing area. Now pass test data in test direcory through to the landing directory
import time

print("Sleep for 2 minutes and let setup and history loader finish...")
time.sleep(2*60)

#Validate setup and history load
HL = HistoryLoader(env)
PR = Producer()
BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

SH.validate()
HL.validate()

#Produce some incremantal
PR.produce(1)
PR.validate(1)

# COMMAND ----------

print("Sleep for 2 minutes and let microbatch pickup the data...")
time.sleep(2*60)

#Validate bronze, silver and gold layer 
BZ.validate(1)
SL.validate(1)
GL.validate(1)
 

#Produce some incremantal data and wait for micro batch
PR.produce(2)
PR.validate(2)

# COMMAND ----------

print("Sleep for 2 minutes and let microbatch pickup the data...")
time.sleep(2*60)

#Validate bronze, silver and gold layer 
BZ.validate(2)
SL.validate(2)
GL.validate(2)

# COMMAND ----------

#Terminate the streaming Job
cancel_payload = {"run_id": run_id}
cancel_response = requests.post(host + '/api/2.1/jobs/runs/cancel', data=json.dumps(cancel_payload), auth=("token", token))
print(f"Canceled Job run {run_id}. Status {cancel_response}")

# COMMAND ----------

#Delete the Job
delete_job_payload = {"job_id": job_id}
delete_job_response = requests.post(host + '/api/2.1/jobs/delete', data=json.dumps(delete_job_payload), auth=("token", token))
print(f"Canceled Job run {run_id}. Status {delete_job_response}")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
