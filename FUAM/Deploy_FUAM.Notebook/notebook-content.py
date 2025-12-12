# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Welcome to FUAM Deployment
# 
# This notebook deployes the latest FUAM version in the specified workspace. It works for initial deployment and for the upgrade process of FUAM.
# 
# **End-to-end documenation on fabric-toolbox:**
# 
# [Visit - How to deploy and configure FUAM](https://github.com/microsoft/fabric-toolbox/blob/main/monitoring/fabric-unified-admin-monitoring/how-to/How_to_deploy_FUAM.md)
# 
# **What is happening in this notebook?**
#  - The notebook checks the two cloud connections for FUAM (if initial deployment, connections will be created, otherwise check only)
#  - It downloads the latest FUAM src files from Github
#  - It deploys/updates the Fabric items in the current workspace
#  - It creates all needed tables automatically, so reports work also with some data missing
# 
# **Next steps**
# - (Optional) Change connection names, only if needed
# - Run this notebook
# 
# If you **deploy** FUAM in this workspace at the **first time**:
# - Navigate to the cloud connections
# - Search under cloud connection for **fuam fabric-service-api admin** and for **fuam pbi-service-api admin** 
# - Add the credentials of your service principal to these connections
# 
# If you **update** your existing FUAM workspace:
# - After the notebooks has been executed, you are **done**


# CELL ********************

%pip install ms-fabric-cli

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

pbi_connection_name = 'fuam pbi-service-api admin'
fabric_connection_name = 'fuam fabric-service-api admin'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Import of needed libaries

# CELL ********************

import subprocess
import os
import json
from zipfile import ZipFile 
import shutil
import re
import requests
import zipfile
from io import BytesIO
import yaml
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Download of source & config files
# This part downloads all source and config files of FUAM needed for the deployment into the ressources of the notebook

# CELL ********************

def download_folder_as_zip(repo_owner, repo_name, output_zip, branch="main", folder_to_extract="src",  remove_folder_prefix = ""):
    # Construct the URL for the GitHub API to download the repository as a zip file
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/zipball/{branch}"
    
    # Make a request to the GitHub API
    response = requests.get(url)
    response.raise_for_status()
    
    # Ensure the directory for the output zip file exists
    os.makedirs(os.path.dirname(output_zip), exist_ok=True)
    
    # Create a zip file in memory
    with zipfile.ZipFile(BytesIO(response.content)) as zipf:
        with zipfile.ZipFile(output_zip, 'w') as output_zipf:
            for file_info in zipf.infolist():
                parts = file_info.filename.split('/')
                if  re.sub(r'^.*?/', '/', file_info.filename).startswith(folder_to_extract): 
                    # Extract only the specified folder
                    file_data = zipf.read(file_info.filename)
                    output_zipf.writestr(('/'.join(parts[1:]).replace(remove_folder_prefix, "")), file_data)

def uncompress_zip_to_folder(zip_path, extract_to):
    # Ensure the directory for extraction exists
    os.makedirs(extract_to, exist_ok=True)
    
    # Uncompress all files from the zip into the specified folder
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Delete the original zip file
    os.remove(zip_path)

repo_owner = "Microsoft"
repo_name = "fabric-toolbox"
branch = "main"
folder_prefix = "monitoring/fabric-unified-admin-monitoring"

download_folder_as_zip(repo_owner, repo_name, output_zip = "./builtin/src/src.zip", branch = branch, folder_to_extract= f"/{folder_prefix}/src", remove_folder_prefix = f"{folder_prefix}/")
download_folder_as_zip(repo_owner, repo_name, output_zip = "./builtin/config/config.zip", branch = branch, folder_to_extract= f"/{folder_prefix}/config" , remove_folder_prefix = folder_prefix)
download_folder_as_zip(repo_owner, repo_name, output_zip = "./builtin/data/data.zip", branch = branch, folder_to_extract= f"/{folder_prefix}/data" , remove_folder_prefix = folder_prefix)
uncompress_zip_to_folder(zip_path = "./builtin/config/config.zip", extract_to= "./builtin")
uncompress_zip_to_folder(zip_path = "./builtin/data/data.zip", extract_to= "./builtin")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

base_path = './builtin/'
config_path = os.path.join(base_path, 'config/deployment_config.yaml')

with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

deploy_order_path = os.path.join(base_path, 'config/deployment_order.json')
with open(deploy_order_path, 'r') as file:
        deployment_order = json.load(file)

src_workspace_name = config['workspace']
src_pbi_connection = config['connections']['pbi_connection']
src_fabric_connection = config['connections']['fabric_connection']

semantic_model_connect_to_lakehouse = config['fuam_lakehouse_semantic_models']

mapping_table=[]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Definition of deployment functions

# CELL ********************

# Set environment parameters for Fabric CLI
token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token

def run_fab_command( command, capture_output: bool = False, silently_continue: bool = False):
    result = subprocess.run(["fab", "-c", command], capture_output=capture_output, text=True)
    if (not(silently_continue) and (result.returncode > 0 or result.stderr)):
       raise Exception(f"Error running fab command. exit_code: '{result.returncode}'; stderr: '{result.stderr}'")    
    if (capture_output): 
        output = result.stdout.strip()
        return output

def fab_get_id(name):
    id = run_fab_command(f"get /{trg_workspace_name}.Workspace/{name} -q id" , capture_output = True, silently_continue= True)
    return(id)

def get_id_by_name(name):
    for it in deployment_order:
        if it.get("name") == name:
                return it.get("fuam_id")
    return None


def copy_to_tmp(name):
    shutil.rmtree("./builtin/tmp",  ignore_errors=True)
    path2zip = "./builtin/src/src.zip"
    with  ZipFile(path2zip) as archive:
        for file in archive.namelist():
            if file.startswith(f'src/{name}/'):
                archive.extract(file, './builtin/tmp')
    return(f"./builtin/tmp/src/{name}" )


def replace_ids_in_folder(folder_path, mapping_table):
    for root, _, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith(('.py', '.json', '.pbir', '.platform', '.ipynb', '.tmdl')) and not file_name.endswith('report.json'):
                file_path = os.path.join(root, file_name)
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                    for mapping in mapping_table:  
                        content = content.replace(mapping["old_id"], mapping["new_id"])
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(content)

def get_semantic_model_id(report_folder):
    definition_file = os.path.join(report_folder, 'definition.pbir')
    if os.path.exists(definition_file):
        with open(definition_file, 'r', encoding='utf-8') as file:
            content = json.load(file)
            semantic_model_id = content.get('datasetReference', {}).get('byConnection', {}).get('pbiModelDatabaseName')
            if semantic_model_id:
                return semantic_model_id
    return None

def update_sm_connection_to_fuam_lakehouse(semantic_model_folder):
    new_sm_db= run_fab_command(f"get /{trg_workspace_name}.Workspace/FUAM_Lakehouse.Lakehouse -q properties.sqlEndpointProperties.connectionString", capture_output = True, silently_continue=True)
    new_lakehouse_sql_id= run_fab_command(f"get /{trg_workspace_name}.Workspace/FUAM_Lakehouse.Lakehouse -q properties.sqlEndpointProperties.id", capture_output = True, silently_continue=True)
        
    expressions_file = os.path.join(semantic_model_folder, 'definition', 'expressions.tmdl')
    if os.path.exists(expressions_file):
        with open(expressions_file, 'r', encoding='utf-8') as file:
            content = file.read()
            match = re.search(r'Sql\.Database\("([^"]+)",\s*"([^"]+)"\)', content)
            if match:
                old_sm_db, old_lakehouse_sql_id = match.group(1), match.group(2)
                content = content.replace(old_sm_db, new_sm_db).replace(old_lakehouse_sql_id, new_lakehouse_sql_id)
                with open(expressions_file, 'w', encoding='utf-8') as file:
                    file.write(content)


def update_report_definition( path): 
    semantic_model_id = get_semantic_model_id(path)
    definition_path = os.path.join(path, "definition.pbir")
   
    with open(definition_path, "r", encoding="utf8") as file:
        report_definition = json.load(file)

    report_definition["datasetReference"]["byPath"] = None

    by_connection_obj = {
            "connectionString": None,
            "pbiServiceModelId": None,
            "pbiModelVirtualServerName": "sobe_wowvirtualserver",
            "pbiModelDatabaseName": semantic_model_id,
            "name": "EntityDataSource",
            "connectionType": "pbiServiceXmlaStyleLive",
        }

    report_definition["datasetReference"]["byConnection"] = by_connection_obj

    with open(definition_path, "w") as file:
            json.dump(report_definition, file, indent=4)

def print_color(text, state):
    red  = '\033[91m'
    yellow = '\033[93m'  
    green = '\033[92m'   
    white = '\033[0m'  
    if state == "error":
        print(red, text, white)
    elif state == "warning":
        print(yellow, text, white)
    elif state == "success":
        print(green, text, white)
    else:
        print("", text)

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Creation of connections

# CELL ********************

def create_or_get_connection(name, baseUrl, audience):
    try:
        run_fab_command(f"""create .connections/{name}.connection 
            -P connectionDetails.type=WebForPipeline,connectionDetails.creationMethod=WebForPipeline.Contents,connectionDetails.parameters.baseUrl={baseUrl},connectionDetails.parameters.audience={audience},credentialDetails.type=Anonymous""")
        print_color("New connection created. Enter service principal credentials", "success")
    except Exception as ex:
        print_color("Connection already exists", "warning")

    conn_id = run_fab_command(f"get .connections/{name}.Connection -q id", silently_continue= True, capture_output= True)
    print("Connection ID:" + conn_id)
    
    
    return(conn_id)
    
conn_pbi_service_api_admin = create_or_get_connection(pbi_connection_name, "https://api.powerbi.com/v1.0/myorg/admin", "https://analysis.windows.net/powerbi/api" )
conn_fabric_service_api_admin = create_or_get_connection(fabric_connection_name, "https://api.fabric.microsoft.com/v1/admin", "	https://api.fabric.microsoft.com" )

mapping_table.append({ "old_id": get_id_by_name(src_pbi_connection), "new_id": conn_pbi_service_api_admin })
mapping_table.append({ "old_id": get_id_by_name(src_fabric_connection), "new_id": conn_fabric_service_api_admin })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Get current Workspace
# This cell gets the current workspace to deploy FUAM automatically inside it

# CELL ********************

trg_workspace_id = fabric.get_notebook_workspace_id()
res = run_fab_command(f"api -X get workspaces/{trg_workspace_id}" , capture_output = True, silently_continue=True)
trg_workspace_name = json.loads(res)["text"]["displayName"]

print(f"Current workspace: {trg_workspace_name}")
print(f"Current workspace ID: {trg_workspace_id}")


mapping_table.append({ "old_id": get_id_by_name(src_workspace_name + ".Workspace"), "new_id": trg_workspace_id })
mapping_table.append({ "old_id": "00000000-0000-0000-0000-000000000000", "new_id": trg_workspace_id })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

mapping_table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Deployment Logic
# This part iterates through all the items, gets the respective source code, replaces all IDs dynamically and deploys the new item

# CELL ********************

exclude = [src_workspace_name + ".Workspace", src_pbi_connection, src_fabric_connection]

for it in deployment_order:

    new_id = None
    
    name = it["name"]
    
    if name in exclude:
            continue

    print("")
    print("#############################################")
    print(f"Deploying {name}")

    # Copy and replace IDs in the item
    tmp_path = copy_to_tmp(name)
    replace_ids_in_folder(tmp_path, mapping_table)

    cli_parameter = ''
    if "Notebook" in name:
        cli_parameter = cli_parameter + " --format .ipynb"
    elif "Lakehouse" in name:
        run_fab_command(f"create /{trg_workspace_name}.Workspace/{name}" , silently_continue=True )
        new_id = fab_get_id(name)
        mapping_table.append({ "old_id": get_id_by_name(name), "new_id": new_id })
        
        continue
    elif "Report" in name:
        update_report_definition(  tmp_path  )
    elif name in semantic_model_connect_to_lakehouse:
        update_sm_connection_to_fuam_lakehouse(tmp_path)
    
    
    run_fab_command(f"import  /{trg_workspace_name}.Workspace/{name} -i {tmp_path} -f {cli_parameter} ", silently_continue= True)
    new_id= fab_get_id(name)
    mapping_table.append({ "old_id": it["fuam_id"], "new_id": new_id })




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Post-Deployment logic
# In this separate notebook, all needed tables for FUAM are automatically deployed. Addtionally new columns will be added to lakehouse tables in order to be available for the semantic model. This notebook has been deployed from the source code in the step before

# CELL ********************

# MAGIC %%configure -f 
# MAGIC {   "defaultLakehouse": { "name": "FUAM_Config_Lakehouse" } }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%pip install ms-fabric-cli

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import subprocess
import json
import sempy.fabric as fabric
import time
import yaml

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Set environment parameters for Fabric CLI
token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token

def run_fab_command( command, capture_output: bool = False, silently_continue: bool = False):
    result = subprocess.run(["fab", "-c", command], capture_output=capture_output, text=True)
    if (not(silently_continue) and (result.returncode > 0 or result.stderr)):
       raise Exception(f"Error running fab command. exit_code: '{result.returncode}'; stderr: '{result.stderr}'")    
    if (capture_output): 
        output = result.stdout.strip()
        return output

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

trg_workspace_id = fabric.get_notebook_workspace_id()
trg_workspace_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

trg_workspace_id = fabric.get_notebook_workspace_id()
res = run_fab_command(f"api -X get workspaces/{trg_workspace_id}" , capture_output = True)
trg_workspace_name = json.loads(res)["text"]["displayName"]

print(f"Current workspace: {trg_workspace_name}")
print(f"Current workspace ID: {trg_workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

src_file_path = "./builtin/data/table_definitions.snappy.parquet"
with open(src_file_path, 'rb') as file:
                    content = file.read()
trg_lakehouse_folder_path = notebookutils.fs.getMountPath('/default') + "/Files/table_definitions/" 
notebookutils.fs.mkdirs(f"file://" +trg_lakehouse_folder_path)
with open(trg_lakehouse_folder_path + "table_definitions.snappy.parquet", "wb") as f:
    f.write(content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

notebookutils.lakehouse.loadTable(
    {
        "relativePath": f"Files/table_definitions/table_definitions.snappy.parquet",
        "pathType": "File",
        "mode": "Overwrite",
        "recursive": False,
        "formatOptions": {
            "format": "Parquet"
        }
    }, "FUAM_Table_Definitions", "FUAM_Config_Lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# In case the last step fails, please try to run it again or go to the Init_FUAM_Lakehouse_Tables Notebook and run it manually

# CELL ********************

# Refresh SQL Endpoint for Config_Lakehouse
items = run_fab_command(f'api -X get -A fabric /workspaces/{trg_workspace_id}/items' , capture_output = True)
for it in json.loads(items)['text']['value']:
    if (it['displayName'] == 'FUAM_Config_Lakehouse' ) & (it['type'] =='SQLEndpoint' ):
        config_sql_endpoint = it['id']
    if (it['displayName'] == 'FUAM_Lakehouse' ) & (it['type'] =='SQLEndpoint' ):
        lh_sql_endpoint = it['id']
print(f"FUAM_Lakehouse SQL Endpoint ID: {lh_sql_endpoint}")
print(f"FUAM_Config_Lakehouse SQL Endpoint ID: {config_sql_endpoint}")

try:
    run_fab_command(f'api -A fabric -X post workspaces/{trg_workspace_id}/sqlEndpoints/{config_sql_endpoint}/refreshMetadata?preview=True -i {{}} ', capture_output=True)
except:
    print("SQL Endpoint Refresh API failed, it is still in Preview, so there can be changes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Fill default tables
time.sleep(10)
run_fab_command('job run ' + trg_workspace_name + '.Workspace/Init_FUAM_Lakehouse_Tables.Notebook -i {"parameters": {"_inlineInstallationEnabled": {"type": "Bool", "value": "True"} } }')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Refresh of SQL Endpoint to make sure all tables are available
try:
    run_fab_command(f'api -A fabric -X post workspaces/{trg_workspace_id}/sqlEndpoints/{lh_sql_endpoint}/refreshMetadata?preview=True -i {{}} ', capture_output=True)
    print("Refresh FUAM_Lakehouse_SQL_Endpoint")
except:
    print("SQL Endpoint Refresh API failed, it is still in Preview, so there can be changes")
# Refresh Semantic Models on top of lakehouse
base_path = './builtin/'
config_path = os.path.join(base_path, 'config/deployment_config.yaml')

with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

semantic_model_connect_to_lakehouse = config['fuam_lakehouse_semantic_models']

for sm in semantic_model_connect_to_lakehouse:
    sm_id = run_fab_command(f"get /{trg_workspace_name}.Workspace/{sm} -q id" , capture_output = True, silently_continue= True)
    run_fab_command(f'api -A powerbi -X post datasets/{sm_id}/refreshes -i  {{ "retryCount":"3" }} ')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
