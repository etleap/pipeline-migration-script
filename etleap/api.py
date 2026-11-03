import base64
import json

import requests as r

BASE_URL = "https://api.etleap.com/api/v2"

# Settings each destination type accepts on create, in addition to type, connectionId and the common settings.
# Anything else returned by GET (including the read-only schemaChangingTo and tableChangingTo) is left out of the copy.
COMMON_DESTINATION_FIELDS = ('waitForQualityCheck', 'primaryKey', 'automaticSchemaChanges')
DESTINATION_FIELDS = {
    'REDSHIFT': ('schema', 'table', 'sortColumns', 'distributionStyle', 'truncateStrings', 'retainHistory', 'compressColumns',
                 'lastUpdatedColumn'),
    'SNOWFLAKE': ('schema', 'table', 'retainHistory', 'clusteringKeys', 'lastUpdatedColumn'),
    'DELTA_LAKE': ('schema', 'table', 'retainHistory', 'lastUpdatedColumn', 'pre10Dot2RuntimeSupport'),
    'ICEBERG': ('schema', 'table'),
    'S3_DATA_LAKE': ('pathPrefix', 'outputFormat', 'generateSnapshots'),
}
DEFAULT_S3_DATA_LAKE_OUTPUT_FORMAT = 'PARQUET'

# CDC pipelines that retain history use the time each change was captured as the last-updated column.
# Etleap adds this column to the script when it generates one, so a copied script may need it added.
FILE_MODIFICATION_TIME_COLUMN = 'file_modification_time'
FILE_MODIFICATION_TIME_TYPE = 'date'

class EtleapApi:

    def __init__(self, access_key, secret_key, base_url = BASE_URL):
        self.auth = (access_key, secret_key)
        self.base_url = base_url
        self.connections = {}

    def get_pipelines(self):
        all_pipelines_resp = r.get(self.base_url + "/pipelines?pageSize=0", auth=self.auth)

        if (all_pipelines_resp.status_code != 200):
            raise EtleapApiException("Error getting all pipelines: " + all_pipelines_resp.text)

        return [Pipeline(self, p) for p in all_pipelines_resp.json()["pipelines"]]

    def get_pipeline_details(self, id):
        pipeline_details_resp = r.get(self.base_url + '/pipelines/' + id, auth=self.auth)

        if (pipeline_details_resp.status_code != 200):
            raise EtleapApi("Error getting details for pipeline " + self.name + ": " + pipeline_details_resp.text)

        return pipeline_details_resp.json()

    def get_destinations(self, id):
        destination_connection_details_resp = r.get(self.base_url + '/connections/' + id, auth=self.auth)

        if (destination_connection_details_resp.status_code != 200):
            raise EtleapApiException("Error getting details for connection " + id + ": " + destination_connection_details_resp.text)

        return Destination(destination_connection_details_resp.json())

    def get_connection(self, id):
        if id not in self.connections:
            connection_resp = r.get(self.base_url + '/connections/' + id, auth=self.auth)
            if (connection_resp.status_code != 200):
                raise EtleapApiException("Error getting details for connection " + id + ": " + connection_resp.text)
            self.connections[id] = connection_resp.json()

        return self.connections[id]

    def get_pipeline_script(self, pipelineId, script_version):
        script_resp = r.get(self.base_url + '/pipelines/' + pipelineId + '/scripts/' + str(script_version), auth=self.auth)
        if (script_resp.status_code != 200):
            raise EtleapApiException("Error getting script for pipeline " + pipelineId + ": " + script_resp.text)

        return script_resp.json()

    def create_pipeline(self, pipeline, pipeline_name_suffix=None):
        if pipeline_name_suffix:
            pipeline.name = pipeline.name + pipeline_name_suffix

        post_resp = r.post(self.base_url + '/pipelines', auth=self.auth, json=pipeline.toJSON())
        if (post_resp.status_code != 200):
            raise EtleapApiException("Error creating pipeline " + pipeline.id + ": " + post_resp.text)

        print("Created pipeline:", pipeline.name)
        self.copy_post_create_settings(pipeline, post_resp.json())

    def copy_post_create_settings(self, original, created):
        # POST /pipelines ignores `shares` and does not accept `updateSchedule`, so both have
        # to be applied with a PATCH once the pipeline exists. They are sent as separate PATCHes
        # because the API rejects the whole update if any part of it fails, and shares can
        # legitimately fail when migrating to a deployment where those users don't exist.
        if original.update_schedule:
            self.update_pipeline(created['id'], original.name, 'update schedule', {'updateSchedule': original.update_schedule})

        # A PATCH that sets `shares` must include everyone who already has access (the creator),
        # otherwise the API rejects it as an attempt to unshare.
        existing_shares = created.get('shares') or []
        missing_shares = [s for s in original.shares if s not in existing_shares]
        if missing_shares:
            self.update_pipeline(created['id'], original.name, 'shares', {'shares': existing_shares + missing_shares})

    def update_pipeline(self, pipeline_id, pipeline_name, what, updates):
        patch_resp = r.patch(self.base_url + '/pipelines/' + pipeline_id, auth=self.auth, json=updates)
        if (patch_resp.status_code != 200):
            print("Warning: pipeline " + pipeline_name + " was created, but copying its " + what + " failed: " + patch_resp.text)

class Pipeline:

    def __init__(self, api, resp):
        self.id = resp['id']
        self.api = api
        self.name = resp['name']
        self.source = resp['source']
        self.destination = resp['destinations'][0]['destination']
        self.add_file_modification_time = False
        self.latest_script_version = resp['latestScriptVersion']
        self.paused = resp['paused']
        self.row_error_settings = resp.get('rowErrorSettings') # Renamed from parsingErrorSettings; may not be present
        self.refresh_schedule = resp.get('refreshSchedule')
        self.update_schedule = resp.get('updateSchedule') # Only present when the pipeline overrides the connection's update schedule
        self.shares = resp.get('shares') or [] # Emails of users the pipeline is shared with (the owner is not listed)
        self.label = resp.get('label') # Only present when the pipeline has a label (API v2.13+)
        self.script = None

    def get_source_connection_id(self):
        return self.source['connectionId']

    def get_destination_conection_id(self):
        return self.destinations[0]['destination']['table']

    def get_script(self):
        if self.script is None:
            self.script = self.api.get_pipeline_script(self.id, self.latest_script_version)

        return self.script

    def retarget(self, destination_type, source_cdc_enabled, retain_history=None):
        """
        Converts the destination to the given type, keeping only the settings that type accepts, and applies the
        retain history setting (None keeps the original's). Returns the names of the settings that were dropped.
        Call this after the connection IDs have been swapped.
        """
        if destination_type not in DESTINATION_FIELDS:
            raise EtleapApiException(f"Pipeline \"{self.name}\": unsupported destination type {destination_type}")

        original = self.destination
        allowed = COMMON_DESTINATION_FIELDS + DESTINATION_FIELDS[destination_type]
        destination = {'type': destination_type, 'connectionId': original['connectionId']}
        destination.update({k: v for k, v in original.items() if k in allowed})
        dropped = [k for k, v in original.items()
                   if v and k not in allowed and k not in ('type', 'connectionId', 'schemaChangingTo', 'tableChangingTo')]

        # S3 data lakes are addressed by a path prefix, every other destination by a table name
        if destination_type == 'S3_DATA_LAKE':
            if not destination.get('pathPrefix'):
                destination['pathPrefix'] = original.get('table')
            destination.setdefault('outputFormat', DEFAULT_S3_DATA_LAKE_OUTPUT_FORMAT)
        elif not destination.get('table') and original.get('pathPrefix'):
            destination['table'] = original['pathPrefix'].rstrip('/').split('/')[-1]

        if destination_type == 'ICEBERG' and destination.get('table'):
            # GET may return an Iceberg table name in uppercase, but create only accepts lowercase
            destination['table'] = destination['table'].lower()

        if retain_history is not None:
            if 'retainHistory' in DESTINATION_FIELDS[destination_type]:
                destination['retainHistory'] = retain_history
            elif retain_history:
                raise EtleapApiException(f"Pipeline \"{self.name}\": {destination_type} destinations do not support retaining history")

        if source_cdc_enabled:
            # CDC sources track updates from the replication logs, so a last-updated column from a query-based source no longer applies
            self.source = {k: v for k, v in self.source.items() if k != 'lastUpdatedColumn'}
            if destination.get('retainHistory'):
                destination['lastUpdatedColumn'] = FILE_MODIFICATION_TIME_COLUMN
                self.add_file_modification_time = True
            else:
                destination.pop('lastUpdatedColumn', None)

        self.destination = destination
        return dropped

    def get_script_for_copy(self):
        script = self.get_script()
        if not self.add_file_modification_time:
            return script

        # Scripts for database sources are always returned in legacy form: base64 of the JSON list of script steps
        if 'legacyScript' not in script:
            raise EtleapApiException(f"Error creating pipeline {self.id}: cannot add the {FILE_MODIFICATION_TIME_COLUMN} column "
                                     "because its script is not in legacy form")
        steps = json.loads(base64.b64decode(script['legacyScript']).decode('utf-8'))
        if isinstance(steps, dict):
            steps = steps['transforms']

        last_step = steps[-1]
        if FILE_MODIFICATION_TIME_COLUMN in last_step['outputColumns']:
            return script

        steps.append({
            'name': 'misc',
            'operation': 'file_modification_time',
            'result': 'column',
            'outputColumns': last_step['outputColumns'] + [FILE_MODIFICATION_TIME_COLUMN],
            'outputColumnTypes': last_step['outputColumnTypes'] + [FILE_MODIFICATION_TIME_TYPE],
        })
        return {'legacyScript': base64.b64encode(json.dumps(steps).encode('utf-8')).decode('ascii')}

    def toJSON(self):
        body = {
            'name': self.name,
            'source': self.source,
            'destination': self.destination,
            'paused': self.paused,
            'script': self.get_script_for_copy(),
        }
        if self.row_error_settings:
            body['rowErrorSettings'] = self.row_error_settings
        if self.refresh_schedule:
            body['refreshSchedule'] = self.refresh_schedule
        if self.label:
            body['label'] = self.label
        return body

class EtleapApiException(Exception):
    def __init__(self, error_text):
        self.error_text = error_text
