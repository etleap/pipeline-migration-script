import requests as r

BASE_URL = "https://api.etleap.com/api/v2"

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
            raise EtleapApiException("Error getting details for pipeline " + id + ": " + pipeline_details_resp.text)

        return pipeline_details_resp.json()

    def get_pipeline_script(self, pipelineId, script_version):
        script_resp = r.get(self.base_url + '/pipelines/' + pipelineId + '/scripts/' + str(script_version), auth=self.auth)
        if (script_resp.status_code != 200):
            raise EtleapApiException("Error getting script for pipeline " + pipelineId + ": " + script_resp.text)

        return script_resp.json()

    def create_pipeline(self, pipeline, pipeline_name_suffix=None):
        name = pipeline.name + pipeline_name_suffix if pipeline_name_suffix else pipeline.name
        destination = dict(pipeline.destination)
        if pipeline_name_suffix and 'table' in destination:
            destination['table'] = destination['table'] + pipeline_name_suffix
        body = {
            'name': name,
            'source': pipeline.source,
            'destination': destination,
            'paused': pipeline.paused,
            'script': pipeline.get_script(),
            'parsingErrorSettings': pipeline.parsing_error_settings
        }
        post_resp = r.post(self.base_url + '/pipelines', auth=self.auth, json=body)
        if (post_resp.status_code != 200):
            raise EtleapApiException("Error creating pipeline \"" + name + "\": " + post_resp.text)

class Pipeline:

    def __init__(self, api, resp):
        self.id = resp['id']
        self.api = api
        self.name = resp['name']
        self.source = resp['source']
        self.destination = resp['destinations'][0]['destination']
        self.latest_script_version = resp['latestScriptVersion']
        self.paused = resp['paused']
        self.parsing_error_settings = resp.get('parsingErrorSettings', {})
        self.script = None

    def get_script(self):
        if self.script is None:
            self.script = self.api.get_pipeline_script(self.id, self.latest_script_version)
        return self.script

class EtleapApiException(Exception):
    def __init__(self, error_text):
        self.error_text = error_text
