import requests as r

BASE_URL = "https://api.etleap.com/api/v2"

# Destination properties returned by GET that the API rejects on create (read-only)
READ_ONLY_DESTINATION_FIELDS = ('schemaChangingTo', 'tableChangingTo')

class EtleapApi:

    def __init__(self, access_key, secret_key, base_url = BASE_URL):
        self.auth = (access_key, secret_key)
        self.base_url = base_url

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
        self.destination = {k: v for k, v in resp['destinations'][0]['destination'].items() if k not in READ_ONLY_DESTINATION_FIELDS}
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

    def toJSON(self):
        body = {
            'name': self.name,
            'source': self.source,
            'destination': self.destination,
            'paused': self.paused,
            'script': self.get_script(),
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
