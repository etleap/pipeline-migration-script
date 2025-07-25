from etleap.api import EtleapApi, EtleapApiException

# env1 API credentials
# This is where pipelines will be migrated from

env1_base_url = "https://api.etleap.com/api/v2"
env1_etleap_access_key = '<add here>'
env1_etleap_secret_key = '<add here>'

# env2 API credentials
# This is where pipelines will be migrated to

env2_base_url = "https://api.etleap.com/api/v2"
env2_etleap_access_key = '<add here>'
env2_etleap_secret_key = '<add here>'


# Maps a connection from the env1 to env2
# Update this as needed in the form of 'env1_connection_id': 'env2_connection_id'
# If a pipeline has a connection (source or destination) that is not in this map, it will be skipped

connection_map = {
    'env1_connection_id1': 'env2_connection_id1', 
    'env1_connection_id2': 'env2_connection_id2'
}

# Any pipelines included in this array will attempt to be migrated

pipeline_ids_to_migrate = [
    # 'pipeline_id_1',
    # 'pipeline_id_2'
    ]

# Any pipelines that ingest from sources in this list will attempt to be migrated
# Use this if you are doing a bulk migration for all pipelines from this connection

sources_to_migrate = [
    # 'source_connection_id_1', 
    # 'source_connection_id_2'
    ]

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
    if (p.source['connectionId'] in connection_map.keys()):
         p.source['connectionId'] = connection_map[p.source['connectionId']]


print(f"\n{len(from_env1)} pipeline(s) to be created in the target environment:")
for p in from_env1:
    print(" -", p.name)

proceed = input("\nProceed with creation? (y/n): ").strip().lower()
if proceed != 'y':
    print("Aborted.")
    exit(0)

env2_client = EtleapApi(env2_etleap_access_key, env2_etleap_secret_key, env2_base_url)

try: 
    for p in from_env1:
        env2_client.create_pipeline(p)
except EtleapApiException as e:
    print(e.error_text)
