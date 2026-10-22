from etleap.api import EtleapApi, EtleapApiException

# Copies existing pipelines onto new source/destination connections, either within one Etleap
# environment (the default) or into a separate Etleap deployment (set the target credentials below).
# Each copy keeps the original's script, paused state, row error settings, refresh schedule, label,
# pipeline-level update schedule, and the users it is shared with (when those users exist in the target).
# Not copied (not exposed by the API): a "don't notify" row error setting.

# API credentials
# This is where pipelines are read from

base_url = "https://api.etleap.com/api/v2"
etleap_access_key = '<api_access_key>'
etleap_secret_key = '<api_secret_key>'

# OPTIONAL: target deployment credentials
# Fill these in only when migrating pipelines to a different Etleap deployment.
# Leave the keys blank to create the copies in the same environment as above.

target_base_url = "https://api.etleap.com/api/v2"
target_etleap_access_key = ''
target_etleap_secret_key = ''

# Maps each current connection to the connection the copied pipelines should use
# Update this as needed in the form of 'current_connection_id': 'new_connection_id'
# When migrating to a different deployment, the new connection IDs are the ones in the target deployment
# Every pipeline being copied must have both its source and destination connection in this map, otherwise the script stops with an error

connection_map = {
    '<current_source_connection_id>': '<new_source_connection_id>',  # source connection (repeat for each)
    '<current_destination_connection_id>': '<new_destination_connection_id>'  # destination connection (must point at a new destination or there will be table name conflicts)
}
# Any pipelines included in this array will be copied

pipeline_ids_to_migrate = [
    # 'pipeline_id1',
    # 'pipeline_id2'
]

# Any pipelines that ingest from sources in this list will be copied
# Use this if you are copying all pipelines from a source connection

sources_to_migrate = [
    # 'source_connection_id1',
    # 'source_connection_id2'
]

# A suffix to append to the names of the copied pipelines
# This should be used to avoid name conflicts with the original pipelines
# Leave as None or empty string to keep original names

pipeline_name_suffix =  ' - New'  # e.g., '_migrated' or ' (copy)'

# -------------------------------------------
# ------- DO NOT EDIT BELOW THIS LINE -------
# -------------------------------------------

client = EtleapApi(etleap_access_key, etleap_secret_key, base_url)
pipelines = client.get_pipelines()

# Copies are created in the target deployment when its credentials are set, otherwise in the same environment
if target_etleap_access_key and target_etleap_secret_key:
    target_client = EtleapApi(target_etleap_access_key, target_etleap_secret_key, target_base_url)
    print(f"\nCopies will be created in the target deployment: {target_base_url}")
else:
    target_client = client
    print("\nCopies will be created in the same environment")

# Only flag pipelines for migration if they match the criteria above 
to_copy = [
    p for p in pipelines
    if (
        (sources_to_migrate and p.source['connectionId'] in sources_to_migrate)
        or (pipeline_ids_to_migrate and p.id in pipeline_ids_to_migrate)
    )
]

for p in to_copy:
    if (p.destination['connectionId'] in connection_map.keys()):
        p.destination['connectionId'] = connection_map[p.destination['connectionId']]
    else: 
        raise EtleapApiException(f"Pipeline \"{p.name}\" has a destination connection not in the connection map: {p.destination['connectionId']}")  
    if (p.source['connectionId'] in connection_map.keys()):
        p.source['connectionId'] = connection_map[p.source['connectionId']]
    else: 
        raise EtleapApiException(f"Pipeline \"{p.name}\" has a source connection not in the connection map: {p.source['connectionId']}")


print(f"\n{len(to_copy)} pipeline(s) to be copied:")
for p in to_copy:
    print(" -", p.name)

proceed = input("\nProceed with creation? (y/n): ").strip().lower()
if proceed != 'y':
    print("Aborted.")
    exit(0)

# Keep going when one pipeline fails so the rest are still attempted, then summarize
created = 0
failed = []
for p in to_copy:
    original_name = p.name
    try:
        target_client.create_pipeline(p, pipeline_name_suffix)
        created += 1
    except EtleapApiException as e:
        print(e.error_text)
        failed.append(original_name)

print(f"\nDone. {created} of {len(to_copy)} pipeline(s) copied.")
if failed:
    print(f"Failed ({len(failed)}):")
    for name in failed:
        print(" -", name)
