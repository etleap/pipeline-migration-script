from etleap.api import EtleapApi, EtleapApiException

# ============================================================
# REQUIRED: API credentials (same as migrate.py)
# ============================================================
base_url = "https://api.etleap.com/api/v2"
etleap_access_key = '<add here>'
etleap_secret_key = '<add here>'

# ============================================================
# REQUIRED: The original pipeline names (same list as migrate.py)
# ============================================================
pipeline_names = [
    # 'schema.table.destination',   e.g. 'altavista.globalx_bond_data.snowflake'
    # 'another.pipeline.name',
]

# ============================================================
# These must match the values used in migrate.py
# ============================================================
pipeline_name_suffix = '_new'   # suffix used when cloning
old_suffix = '_old'             # suffix applied to originals during cutover

# -------------------------------------------
# ------- DO NOT EDIT BELOW THIS LINE -------
# -------------------------------------------

# Cutover renames each pair in two steps to avoid name conflicts:
#   Step 1: original (X)     → X_old   (frees the original name)
#   Step 2: cloned   (X_new) → X       (takes the original name)
# The destination table is renamed in the same steps.

print("  Connecting to Etleap...")
client = EtleapApi(etleap_access_key, etleap_secret_key, base_url)

print("  Fetching all pipelines...")
all_pipelines = client.get_pipelines()
print(f"  Found {len(all_pipelines)} pipeline(s) total")

print(f"\n  Looking up {len(pipeline_names)} pipeline pair(s)...")
name_map = {p.name: p for p in all_pipelines}
pairs = []
missing = []
for name in pipeline_names:
    original = name_map.get(name)
    cloned = name_map.get(name + pipeline_name_suffix)
    if not original:
        missing.append(f"{name}  (original not found)")
    elif not cloned:
        missing.append(f"{name + pipeline_name_suffix}  (cloned pipeline not found)")
    else:
        pairs.append((original, cloned))

if missing:
    print(f"\n  Warning: {len(missing)} pipeline(s) could not be paired:")
    for m in missing:
        print(f"    - {m}")

if not pairs:
    print("\n  No complete pairs found. Aborting.")
    exit(0)

print(f"\n  {len(pairs)} pipeline pair(s) ready for cutover:")
for original, cloned in pairs:
    orig_table = original.destination.get('table', '')
    print(f"    {original.name!r} ({orig_table})  →  {original.name + old_suffix!r} ({orig_table + old_suffix})")
    print(f"    {cloned.name!r} ({cloned.destination.get('table', '')})  →  {original.name!r} ({orig_table})")
    print()

proceed = input("Proceed with cutover? (y/n): ").strip().lower()
if proceed != 'y':
    print("Aborted.")
    exit(0)

print()
completed = 0
try:
    for original, cloned in pairs:
        orig_table = original.destination.get('table', '')

        print(f"  [{completed + 1}/{len(pairs)}] Renaming \"{original.name}\" → \"{original.name + old_suffix}\"...")
        client.rename_pipeline(original, original.name + old_suffix, orig_table + old_suffix if orig_table else None)

        print(f"  [{completed + 1}/{len(pairs)}] Renaming \"{cloned.name}\" → \"{original.name}\"...")
        client.rename_pipeline(cloned, original.name, orig_table if orig_table else None)

        completed += 1
except EtleapApiException as e:
    print(f"  Error: {e.error_text}")
    print(f"  Stopped after {completed}/{len(pairs)} pair(s). Remaining pipelines were not renamed.")
    exit(1)

print(f"\n  Done. {completed}/{len(pairs)} pair(s) cut over.")
