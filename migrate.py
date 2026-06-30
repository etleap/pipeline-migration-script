from etleap.api import EtleapApi, EtleapApiException

# ============================================================
# REQUIRED: API credentials
# ============================================================
base_url = "https://api.etleap.com/api/v2"
etleap_access_key = '<add here>'
etleap_secret_key = '<add here>'

# ============================================================
# REQUIRED: List the pipeline names to clone
pipeline_names_to_clone = [
    # 'schema.table.destination',   e.g. 'altavista.globalx_bond_data.snowflake'
    # 'another.pipeline.name',
]
# ============================================================

# ============================================================
# REQUIRED: Suffix appended to each cloned pipeline's name and destination table.
# The cloned pipeline is created as: <original_name><suffix> → <destination_table><suffix>
# Once the new pipeline catches up, you will rename:
#   original pipeline → <original_name>_old
#   cloned pipeline   → <original_name>  (drop the suffix)
# ============================================================
pipeline_name_suffix = '_new'

# -------------------------------------------
# ------- DO NOT EDIT BELOW THIS LINE -------
# -------------------------------------------

print("  Connecting to Etleap...")
client = EtleapApi(etleap_access_key, etleap_secret_key, base_url)

print("  Fetching all pipelines...")
all_pipelines = client.get_pipelines()
print(f"  Found {len(all_pipelines)} pipeline(s) total")

print(f"\n  Filtering to {len(pipeline_names_to_clone)} requested pipeline name(s)...")
to_clone = [p for p in all_pipelines if p.name in pipeline_names_to_clone]

not_found = [name for name in pipeline_names_to_clone if not any(p.name == name for p in to_clone)]
if not_found:
    print(f"  Warning: {len(not_found)} name(s) not found in this environment:")
    for name in not_found:
        print(f"    - {name}")

if not to_clone:
    print("\n  No matching pipelines found. Check pipeline_names_to_clone.")
    exit(0)

non_s3 = [p for p in to_clone if not p.source.get('type', '').startswith('S3')]
if non_s3:
    print(f"\n  Warning: {len(non_s3)} matched pipeline(s) are not S3 sources and will be skipped:")
    for p in non_s3:
        print(f"    - {p.name} (type: {p.source.get('type')})")
    to_clone = [p for p in to_clone if p.source.get('type', '').startswith('S3')]

print(f"\n  {len(to_clone)} pipeline(s) to be cloned with file change detection enabled:")
for p in to_clone:
    print(f"    {p.name}  →  {p.name}{pipeline_name_suffix}")

proceed = input("\nProceed with creation? (y/n): ").strip().lower()
if proceed != 'y':
    print("Aborted.")
    exit(0)

print()
created = 0
try:
    for p in to_clone:
        print(f"  Creating \"{p.name}{pipeline_name_suffix}\"...")
        p.source['filesCanChange'] = True
        client.create_pipeline(p, pipeline_name_suffix)
        created += 1
except EtleapApiException as e:
    print(f"  Error: {e.error_text}")

print(f"\n  Done. {created}/{len(to_clone)} pipeline(s) created.")
