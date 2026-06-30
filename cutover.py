import time
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

def fmt_table(dest):
    schema = dest.get('schema', '')
    table = dest.get('table', '')
    return f"{schema}.{table}" if schema else table

print(f"\n  {len(pairs)} pipeline pair(s) ready for cutover:")
for original, cloned in pairs:
    orig_table = original.destination.get('table', '')
    orig_fmt = fmt_table(original.destination)
    cloned_fmt = fmt_table(cloned.destination)
    print(f"    {original.name!r} ({orig_fmt})  →  {original.name + old_suffix!r} ({orig_fmt + old_suffix})")
    print(f"    {cloned.name!r} ({cloned_fmt})  →  {original.name!r} ({orig_fmt})")
    print()

proceed = input("Proceed with cutover? (y/n): ").strip().lower()
if proceed != 'y':
    print("Aborted.")
    exit(0)

def wait_for_rename(pipeline_id, label, expected_name, expected_table, poll_interval=5, timeout=300):
    elapsed = 0
    while elapsed < timeout:
        details = client.get_pipeline_details(pipeline_id)
        dest = details['destinations'][0]['destination']
        changing_to = dest.get('tableChangingTo')
        if not changing_to:
            actual_name = details.get('name')
            actual_table = dest.get('table', '')
            if actual_name != expected_name or actual_table != expected_table:
                raise EtleapApiException(
                    f"Rename for \"{label}\" did not complete as expected: "
                    f"name={actual_name!r} (expected {expected_name!r}), "
                    f"table={actual_table!r} (expected {expected_table!r})"
                )
            print(f"    Confirmed: now \"{actual_name}\" ({actual_table})")
            return
        print(f"    Waiting for table rename to complete (tableChangingTo: {changing_to})...")
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise EtleapApiException(f"Timed out waiting for table rename on \"{label}\" after {timeout}s")

print()
completed = 0
try:
    for original, cloned in pairs:
        orig_table = original.destination.get('table', '')
        orig_fmt = fmt_table(original.destination)
        cloned_fmt = fmt_table(cloned.destination)
        old_table_fmt = orig_fmt + old_suffix if orig_table else orig_fmt

        old_name = original.name + old_suffix
        old_table = orig_table + old_suffix if orig_table else orig_table
        print(f"  [{completed + 1}/{len(pairs)}] Renaming \"{original.name}\" → \"{old_name}\"  ({orig_fmt} → {old_table_fmt})...")
        client.rename_pipeline(original, old_name, old_table)
        wait_for_rename(original.id, original.name, old_name, old_table)

        print(f"  [{completed + 1}/{len(pairs)}] Renaming \"{cloned.name}\" → \"{original.name}\"  ({cloned_fmt} → {orig_fmt})...")
        client.rename_pipeline(cloned, original.name, orig_table)
        wait_for_rename(cloned.id, cloned.name, original.name, orig_table)

        completed += 1
except EtleapApiException as e:
    print(f"  Error: {e.error_text}")
    print(f"  Stopped after {completed}/{len(pairs)} pair(s). Remaining pipelines were not renamed.")
    exit(1)

print(f"\n  Done. {completed}/{len(pairs)} pair(s) cut over.")
