"""
This script cleans all artefacts deployed to a Databricks environment:
- cluster used to register tables to Unity Catalog
- workflow used to register tables to Unity Catalog
- DLT pipeline
- workflow wrapping the DLT pipeline
- ADLS folders used to upload and save data and tables ( only for dev )
- UC created for integration test ( only for dev )
"""

import argparse
import json
import os
import sys

from jinja2 import Environment, FileSystemLoader

from ci_cd_helpers.auth import get_dbx_http_header
from ci_cd_helpers.azure import delete_adls_folder, generate_spn_ad_token
from ci_cd_helpers.clusters import delete_cluster_by_name
from ci_cd_helpers.helpers import (
    get_cluster_name,
    get_dlt_pipeline_name,
    get_register_table_uc_workflow_name,
    get_repos_parent_folder_path,
    get_repos_path,
    get_unique_repo_branch_id,
    get_wrapping_workflow_name
)
from ci_cd_helpers.pipelines import delete_pipeline_by_name, get_pipeline_id
from ci_cd_helpers.repos import delete_repos_by_path, get_repos_id
from ci_cd_helpers.workflows import delete_workflow_by_name, poll_run, trigger_one_time_run
from datta_pipeline_library.core.base_config import (
    landing_container_name, 
    raw_container_name,
    euh_container_name,
    eh_container_name,
    curated_container_name,
    deltalake_container_name,
    BaseConfig,
    CommonConfig,
    EnvConfig
)

parser = argparse.ArgumentParser(description="Clean Databricks env.")

parser.add_argument(
    "--host",
    help="Databricks host.",
    type=str,
)

parser.add_argument(
    "--tenant-id",
    help="Azure tenant id.",
    type=str,
)

parser.add_argument(
    "--spn-client-id",
    help="Service principal client id used to deploy code.",
    type=str,
)

parser.add_argument(
    "--spn-client-secret",
    help="Service principal client secret used to deploy code.",
    type=str,
)

parser.add_argument(
    "--repository",
    help="GitHub repository (with owner).",
    type=str,
)

parser.add_argument(
    "--branch-name",
    help="Name of the branch in GitHub",
    type=str,
)

parser.add_argument(
    "--env",
    help="Environment. Can only be 'tst' or 'pre'.",
    type=str,
    choices=["dev", "tst", "pre"],
)

parser.add_argument(
    "--deployment-type",
    help="Deployment type, either 'manual' or 'automatic'. Taken into account only when env=dev.",
    choices=["manual", "automatic"],
    type=str,
)

parser.add_argument(
    "--timeout",
    help="Timeout in seconds for the workflow that resets UC and ADLS. Default is 0, i.e. no timeout.",
    type=int,
    default=0,
)

args = parser.parse_args()

if args.env == "dev" and not args.deployment_type:
    raise ValueError("When env=dev, parameter 'type' must be either 'manual' or 'automatic', not None.")

parent_folder_path = get_repos_parent_folder_path(args.repository)
repos_path = get_repos_path(args.repository, args.branch_name, args.deployment_type)

fi_d_hana_suffix = "D-HANA-"+args.deployment_type

# the resources names depend on the repository name, the branch name, and the deployment type (for dev)
dlt_pipeline_name = get_dlt_pipeline_name(args.repository, args.branch_name, args.deployment_type)
fi_d_hana_dlt_pipeline_name = get_dlt_pipeline_name(args.repository, args.branch_name, fi_d_hana_suffix)

workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, args.deployment_type)
fi_d_hana_wrapping_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, fi_d_hana_suffix)

register_table_uc_name = get_register_table_uc_workflow_name(args.repository, args.branch_name, args.deployment_type)
cluster_name = get_cluster_name(args.repository, args.branch_name, args.deployment_type)
# dlt_pipeline_name = get_dlt_pipeline_name(args.repository)
# workflow_name = get_wrapping_workflow_name(args.repository)
# register_table_uc_name = get_register_table_uc_workflow_name(args.repository)
# cluster_name = get_cluster_name(args.repository)
unique_repo_branch_id = get_unique_repo_branch_id(args.repository, args.branch_name, args.deployment_type) if args.env == "dev" else ""

fi_d_hana_unique_repo_branch_id = get_unique_repo_branch_id(args.repository, args.branch_name, fi_d_hana_suffix) if args.env == "dev" else ""

# necessary to call the Databricks REST API
ad_token = generate_spn_ad_token(args.tenant_id, args.spn_client_id, args.spn_client_secret)
http_header = get_dbx_http_header(ad_token)

dir_path = os.path.dirname(os.path.abspath(__file__))
conf_folder = f"{dir_path}/../../conf"
common_conf_file_path = f"{conf_folder}/common/common_conf.json"
env_conf_file_path = f"{conf_folder}/{args.env}/conf.json"

common_conf = CommonConfig.from_file(common_conf_file_path)
env_conf = EnvConfig.from_file(env_conf_file_path)

base_config = BaseConfig.from_confs(env_conf, common_conf)
base_config.set_unique_id(unique_repo_branch_id)
base_config.set_unique_id_schema(unique_repo_branch_id)

storage_account = base_config.storage_account

# check that code was actually deployed, if not then exit right away
repos_id = get_repos_id(repos_path, args.host, http_header)
if not repos_id:
    print(f"No Repos found with path {repos_path} in env {args.env}, meaning no deployment happened. No need to reset anything.")
    sys.exit(0)

# reset UC and ADLS
dir_path = os.path.dirname(os.path.abspath(__file__))
conf_folder_path = f"{dir_path}/../../conf"
common_conf_folder_path = f"{conf_folder_path}/common"
common_templates = Environment(loader=FileSystemLoader(common_conf_folder_path))
reset_uc_and_adls_conf_filename = "reset_uc_and_adls_workflow.json"
reset_uc_and_adls_conf_template = common_templates.get_template(reset_uc_and_adls_conf_filename)

fi_d_hana_reset_uc_and_adls_conf_filename = "fi_d_hana_reset_uc_and_adls_workflow.json"
fi_d_hana_reset_uc_and_adls_conf_template = common_templates.get_template(fi_d_hana_reset_uc_and_adls_conf_filename)

dlt_pipeline_id = get_pipeline_id(dlt_pipeline_name, args.host, http_header)

reset_uc_and_adls_conf_str = reset_uc_and_adls_conf_template.render(
    repos_path=repos_path,
    unique_repo_branch_id=unique_repo_branch_id,
    unique_repo_branch_id_schema=unique_repo_branch_id,
    dlt_pipeline_id=dlt_pipeline_id if dlt_pipeline_id else "",
    env=args.env,
    timeout=args.timeout,
    run_name=f"reset-{args.env}-{unique_repo_branch_id}",
)

fi_d_hana_dlt_pipeline_id = get_pipeline_id(fi_d_hana_dlt_pipeline_name, args.host, http_header)

fi_d_hana_reset_uc_and_adls_conf_str = fi_d_hana_reset_uc_and_adls_conf_template.render(
    repos_path=repos_path,
    unique_repo_branch_id=fi_d_hana_unique_repo_branch_id,
    unique_repo_branch_id_schema=unique_repo_branch_id,
    fi_d_hana_dlt_pipeline_id=fi_d_hana_dlt_pipeline_id if fi_d_hana_dlt_pipeline_id else "",
    env=args.env,
    timeout=args.timeout,
    run_name=f"reset-{args.env}-{fi_d_hana_unique_repo_branch_id}",
)

reset_uc_and_adls_conf = json.loads(reset_uc_and_adls_conf_str)
fi_d_hana_reset_uc_and_adls_conf = json.loads(fi_d_hana_reset_uc_and_adls_conf_str)

reset_uc_and_adls_workflow_run_id = trigger_one_time_run(reset_uc_and_adls_conf, args.host, http_header)
print(f"New job run to delete UC and ADLS folders has been triggered with id {reset_uc_and_adls_workflow_run_id}.")
fi_d_hana_reset_uc_and_adls_workflow_run_id = trigger_one_time_run(fi_d_hana_reset_uc_and_adls_conf, args.host, http_header)
print(f"New job run to delete UC and ADLS folders has been triggered with id {fi_d_hana_reset_uc_and_adls_workflow_run_id}.")

run_status = poll_run(reset_uc_and_adls_workflow_run_id, args.host, http_header)
fi_d_hana_run_status = poll_run(fi_d_hana_reset_uc_and_adls_workflow_run_id, args.host, http_header)

if run_status != "SUCCESS" or fi_d_hana_run_status != "SUCCESS":
    sys.exit(1)

# Remove code,jobs and workflows from Databricks repo
if args.env == "dev":
    delete_cluster_by_name(cluster_name, args.host, http_header)
    delete_workflow_by_name(register_table_uc_name, args.host, http_header) 
    delete_pipeline_by_name(dlt_pipeline_name, args.host, http_header)
    delete_pipeline_by_name(fi_d_hana_dlt_pipeline_name, args.host, http_header)
    delete_workflow_by_name(workflow_name, args.host, http_header)
    delete_workflow_by_name(fi_d_hana_wrapping_workflow_name, args.host, http_header)
    delete_repos_by_path(repos_path, args.host, http_header)