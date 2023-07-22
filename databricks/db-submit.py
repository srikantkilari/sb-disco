#!/usr/bin/env python3

#from databricks.sdk import WorkspaceClient
import argparse
import subprocess
import os
from jobs import Jobs

parser = argparse.ArgumentParser()
parser.add_argument('--source-zip')
parser.add_argument('--dest-zip')
parser.add_argument('--conf', action="append")
parser.add_argument('--cluster-spec')
parser.add_argument('--py-files')
parser.add_argument('--packages')
parser.add_argument('--instance-profile-arn')
parser.add_argument('--wait-for-completion')
parser.add_argument('python_script', nargs=argparse.REMAINDER)

def read_profile():
  host = os.getenv("DATABRICKS_HOST")
  token = os.getenv("DATABRICKS_TOKEN")
  if host is None:
    with open(f"{os.path.expanduser('~')}/.databrickscfg") as f:
      line = f.readline()
      while(line != "" and line != "[DEFAULT]\n"):
        line = f.readline()
      if line != "":
        host = f.readline().replace(" ", "").split("=")[1].replace("\n", "")
        token = f.readline().replace(" ", "").split("=")[1].replace("\n", "")
  return {
    "DATABRICKS_HOST":host,
    "DATABRICKS_TOKEN":token
  }

def parse(args=None):
  args = parser.parse_args(args) if args is not None else parser.parse_args()
  return args

def run(args=None):
  args = parse(args)
  wait_for_completion = args.wait_for_completion
  if wait_for_completion == 'true' or wait_for_completion == '1':
    wait_for_completion = True
  else:
    wait_for_completion = False
  instance_profile_arn = args.instance_profile_arn
  if instance_profile_arn == "":
    instance_profile_arn = None
  spark_conf = {kv[0]:kv[1] for kv in [conf.split("=") for conf in args.conf]}
  app_name = spark_conf['spark.app.name']
  profile = read_profile()
  py_files = args.py_files
  if py_files is not None:
    py_files = py_files.split(",")
    py_files = [f"{'file://' if f.startswith('/') or f.startswith('.') or f.startswith('~') else ''}{f}" for f in py_files]
  packages = args.packages
  if packages is not None:
    packages = packages.split(",")
  cluster_spec = args.cluster_spec
  jobs = Jobs(api_url=profile["DATABRICKS_HOST"], token=profile["DATABRICKS_TOKEN"])
  job_id = jobs.create_python_job(job_name=app_name.replace(":", "_").replace(".", "_"),
                         bootstrap_copy_notebook_path="databricks/bootstrap_copy",
                         source_zip=args.source_zip,
                         dest_zip=args.dest_zip,
                         git_url="https://github.com/srikantkilari/sb-disco.git",
                         python_file=f"{'file://' if args.python_script[0].startswith('/') or args.python_script[0].startswith('.') or args.python_script[0].startswith('~') else ''}{args.python_script[0]}",
                         parameters=args.python_script[1:] if len(args.python_script)>1 else None,
                         cluster_spec=cluster_spec,
                         libraries=py_files,
                         packages=packages,
                         spark_conf=spark_conf,
                         instance_profile_arn=instance_profile_arn)
  run_id = jobs.run_now(job_id)["run_id"]
  state = None
  if wait_for_completion is True:
    result = jobs.wait_get_run_job_terminated_or_skipped(run_id)
    state = result["state"]["result_state"]
  else:
    state = run_id
  return state

if __name__ == "__main__":
  run()