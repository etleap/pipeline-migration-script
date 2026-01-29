from etleap.api import EtleapApi, EtleapApiException

# env1 API credentials
# This is where pipelines will be migrated from

env1_base_url = "https://api.etleap.com/api/v2"
env1_etleap_access_key = 'access_key'
env1_etleap_secret_key = 'secret_key'

# Maps a connection from the env1 to env2
# Update this as needed in the form of 'env1_connection_id': 'env2_connection_id'
# If a pipeline has a connection (source or destination) that is not in this map, it will be skipped

connection_map = {
    'connection1_old':'connection1_new', # source connection (repeat for each) 
    'connection2_old':'connection2_new'  # destination connection
}
# Any pipelines included in this array will attempt to be migrated

pipeline_ids_to_migrate = [
    # 'pipeline_id1',
    # 'pipeline_id2'
]

# Any pipelines that ingest from sources in this list will attempt to be migrated
# Use this if you are doing a bulk migration for all pipelines from this connection

sources_to_migrate = [
    # 'source_connection_id1',
    # 'source_connection_id2'
] 

# A suffix to append to pipeline names in the target environment
# If you are doing a migration within the same org, this should be used to avoid name conflicts
# Leave as None or empty string to keep original names

pipeline_name_suffix =  ' - New'  # e.g., '_migrated' or ' (copy)'

# -------------------------------------------
# ------- DO NOT EDIT BELOW THIS LINE -------
# -------------------------------------------

env1_client = EtleapApi(env1_etleap_access_key, env1_etleap_secret_key, env1_base_url)
pipelines = env1_client.get_pipelines()

# Only flag pipelines for migration if they match the criteria above 
from_env1 = [
    p for p in pipelines
    if (
        (sources_to_migrate and p.source['connectionId'] in sources_to_migrate)
        or (pipeline_ids_to_migrate and p.id in pipeline_ids_to_migrate)
    )
]

for p in from_env1:
    if (p.destination['connectionId'] in connection_map.keys()):
        p.destination['connectionId'] = connection_map[p.destination['connectionId']]
    else: 
        raise EtleapApiException(f"Pipeline \"{p.name}\" has a destination connection not in the connection map: {p.destination['connectionId']}")  
    if (p.source['connectionId'] in connection_map.keys()):
        p.source['connectionId'] = connection_map[p.source['connectionId']]
    else: 
        raise EtleapApiException(f"Pipeline \"{p.name}\" has a destination connection not in the connection map: {p.source['connectionId']}")  


print(f"\n{len(from_env1)} pipeline(s) to be created in the target environment:")
for p in from_env1:
    print(" -", p.name)

proceed = input("\nProceed with creation? (y/n): ").strip().lower()
if proceed != 'y':
    print("Aborted.")
    exit(0)

try:
    for p in from_env1:
        env1_client.create_pipeline(p, pipeline_name_suffix)
except EtleapApiException as e:
    print(e.error_text)
