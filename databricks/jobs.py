import requests
import copy
import time
import random
import json

class OperationFailed(RuntimeError):
  pass

class TimeoutError(RuntimeError):
  pass

class Jobs:
  _api_url = None
  _token = None
  _default_cluster_spec = {
    "cluster_name":"",
    "spark_version":"13.2.x-scala2.12",
    "aws_attributes":{
      "first_on_demand":1,
      "availability":"SPOT_WITH_FALLBACK",
      "zone_id":"auto",
      "spot_bid_price_percent":100,
      "ebs_volume_type":"GENERAL_PURPOSE_SSD",
      "ebs_volume_count":3,
      "ebs_volume_size":100
    },
    "node_type_id":"i3.xlarge",
    "driver_node_type_id":"m5.xlarge",
    "enable_elastic_disk":False,
    "data_security_mode":"NONE",
    "runtime_engine":"STANDARD",
    "autoscale": {
      "min_workers":2,
      "max_workers":8
    },
    "num_workers":8
  }

  def __init__(self, api_url, token):
    self._api_url = api_url
    self._token = token

  def send_job_request(self, action, request_func):
    api_url = self._api_url
    token = self._token
    
    response = request_func(f'{api_url}/api/2.1/jobs/{action}', {"Authorization": f"Bearer {token}"})
    if response.status_code != 200:
      raise Exception("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
    return response.json()

  def find_job_by_name(self, name):
    jobs = self.send_job_request('list', lambda u, h: requests.get(u, headers=h))
    j = [job for job in jobs['jobs'] if job['settings'].get('name', '') == name]
    if len(j) > 0:
      return j[0]
    return None

  def get_job_by_name(self, name):  
    job = self.find_job_by_name(name)
    if job is None:
      raise Exception(f'Job with name {name} not found')

    job_id = job['job_id']

    job = self.send_job_request('get', lambda u, h: requests.get(f'{u}&job_id={job_id}', headers=h))
    return job

  def reset_job_by_name(self, name, job_config):
    j = self.get_job_by_name(name)
    response = self.send_job_request('reset', lambda u, h: requests.post(f'{u}', json=job_config, headers=h))
    return response

  def get_run(self, run_id):
    response = self.send_job_request('runs/get', lambda u, h: requests.get(f'{u}?run_id={run_id}', headers=h))
    return response
  
  def run_now(self, job_id):
    job_config = {
      "job_id": job_id
    }
    response = self.send_job_request('run-now', lambda u, h: requests.post(f'{u}', json=job_config, headers=h))
    return response

  def wait_get_run_job_terminated_or_skipped(self,
                                             run_id,
                                             timeout=None,#timedelta(minutes=20),
                                             callback = None):
    deadline = time.time()
    if timeout is not None:
      deadline += timeout.total_seconds()
    target_states = ("TERMINATED", "SKIPPED", )
    failure_states = ("INTERNAL_ERROR", )
    status_message = 'polling...'
    attempt = 1
    while timeout is None or time.time() < deadline:
        poll = self.get_run(run_id=run_id)
        state = poll.get("state")
        status = None
        if state is not None:
          status = state.get("life_cycle_state")
        status_message = f'current status: {status}'
        if state is not None:
            status_message = state.get("state_message")
        if status in target_states:
            return poll
        if callback:
            callback(poll)
        if status in failure_states:
            msg = f'failed to reach TERMINATED or SKIPPED, got {status}: {status_message}'
            raise OperationFailed(msg)
        prefix = f"run_id={run_id}"
        sleep = attempt
        if sleep > 10:
            # sleep 10s max per attempt
            sleep = 10
        # _LOG.debug(f'{prefix}: ({status}) {status_message} (sleeping ~{sleep}s)')
        time.sleep(sleep + random.random())
        attempt += 1
    raise TimeoutError(f'timed out after {timeout}: {status_message}')

  def create_python_job(self,
                        job_name,
                        python_file,
                        bootstrap_copy_notebook_path,
                        source_zip,
                        dest_zip,
                        git_url,
                        git_provider="gitHub",
                        git_branch="main",
                        parameters=None,
                        cluster_spec=None,
                        spark_conf=None,
                        libraries=None,
                        packages=None,
                        instance_profile_arn=None):
    if libraries is not None:
      whls = [{"whl":lib} for lib in libraries if lib.endswith(".whl")]
      eggs = [{"egg":lib} for lib in libraries if lib.endswith(".egg")]
      jars = [{"jar":lib} for lib in libraries if lib.endswith(".jar")]
      zips = ",".join([lib for lib in libraries if not lib.endswith(".whl") and not lib.endswith(".egg") and not lib.endswith(".jar")])
      libraries = whls + eggs + jars
    else:
      libraries = []
    if packages is not None:
      pypis = [{"pypi":{"package":p}} for p in packages if not ":" in p]
      mavens = [{"maven":{"coordinates":p}} for p in packages if ":" in p]
      packages = pypis + mavens
    else:
      packages = []
    if cluster_spec is None:
      cluster_config = self._default_cluster_spec
    else:
      cluster_config = json.loads(cluster_spec)
    existing_cluster_id = cluster_config.get("existing_cluster_id")
    if existing_cluster_id is None:
      passed_spark_conf = cluster_config.get("spark_conf")
      if passed_spark_conf is not None:
        if spark_conf is None:
          spark_conf = passed_spark_conf
        else:
          for kv in passed_spark_conf.items():
            spark_conf[kv[0]] = kv[1]
      submit_pyFiles = spark_conf.get("spark.submit.pyFiles")
      if submit_pyFiles is None:
        submit_pyFiles = ""
      if zips is not None:
        submit_pyFiles = ("," if len(submit_pyFiles) > 0 else "") + zips
      if submit_pyFiles is not None and submit_pyFiles != "":
        spark_conf["spark.submit.pyFiles"] = submit_pyFiles
      submit_files = spark_conf.get("spark.files")
      if submit_files is None:
        submit_files = ""
      if zips is not None:
        submit_files = ("," if len(submit_files) > 0 else "") + zips
      if submit_files is not None and submit_files != "":
        spark_conf["spark.files"] = submit_files
      cluster_config["spark_conf"] = spark_conf
      if cluster_config.get("aws_attributes") is None:
        cluster_config["aws_attributes"] = self._default_cluster_spec["aws_attributes"]
        if cluster_config.get("instance_pool_id") is not None:
          del cluster_config["aws_attributes"]["ebs_volume_type"]
          del cluster_config["aws_attributes"]["ebs_volume_count"]
          del cluster_config["aws_attributes"]["ebs_volume_size"]
          del cluster_config["aws_attributes"]["spot_bid_price_percent"]
          del cluster_config["aws_attributes"]["zone_id"]
          del cluster_config["aws_attributes"]["availability"]
          del cluster_config["aws_attributes"]["first_on_demand"]
      if instance_profile_arn is not None:
        cluster_config["aws_attributes"]["instance_profile_arn"] = instance_profile_arn
    bootstrap_copy_task = [{"task_key":"bootstrap_copy",
        "notebook_task": {
          "notebook_path": f"{bootstrap_copy_notebook_path}",
          "source": "GIT",
          "base_parameters": {
            "source": f"{source_zip}",
            "dest": f"{dest_zip}",
            "py-files": f"{zips}",
          }
        },
        "job_cluster_key":f"{job_name}_cluster",
        "timeout_seconds":0,
        "email_notifications":{}
        }]
    job_config = {
      "name":f"{job_name}",
      "email_notifications":{
        "no_alert_for_skipped_runs":False
      },
      "webhook_notifications":{},
      "timeout_seconds":0,
      "max_concurrent_runs":1,
      "tasks":bootstrap_copy_task + [
        {"task_key":f"{job_name}",
        "spark_python_task":{
          "python_file":python_file,
          "parameters":parameters
        },
        "depends_on":{
          "task_key": "bootstrap_copy"
        },
        "libraries":libraries + packages,
        "job_cluster_key":f"{job_name}_cluster",
        "timeout_seconds":0,
        "email_notifications":{}
        }],
      "job_clusters":[{
        "job_cluster_key":f"{job_name}_cluster",
        "new_cluster": cluster_config
      }],
      "git_source": {
          "git_url": f"{git_url}",
          "git_provider": f"{git_provider}",
          "git_branch": f"{git_branch}"
      },
      "format":"MULTI_TASK"
    }
    if existing_cluster_id is not None:
      for t in job_config["tasks"]:
        del t["job_cluster_key"]
        t["existing_cluster_id"] = existing_cluster_id
      del job_config["job_clusters"]
    j = self.find_job_by_name(job_name)
    if j is not None:
      job_id = j['job_id']
      job_config = {
        "job_id": job_id,
        "new_settings": job_config
      }
      response = self.send_job_request('reset', lambda u, h: requests.post(f'{u}', json=job_config, headers=h))
      return job_id
    else:
      response = self.send_job_request('create', lambda u, h: requests.post(f'{u}', json=job_config, headers=h))
      return response["job_id"]
    return response