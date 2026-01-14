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
        name = pipeline.name + pipeline_name_suffix if pipeline_name_suffix else pipeline.name
        body = {
            'name': name,
            'source': pipeline.source,
            'destination': pipeline.destination,
            'script': pipeline.script,
            'paused': pipeline.paused,
            'script': pipeline.get_script(),
            'parsingErrorSettings': {
                'threshold': 0,
                'action': 'NOTIFY'
            }
        }       
        
        post_resp = r.post(self.base_url + '/pipelines', auth=self.auth, json=pipeline.toJSON())
        if (post_resp.status_code != 200):
            raise EtleapApiException("Error creating pipeline " + pipeline.id + ": " + post_resp.text)
        else: 
            print("Created pipeline:", pipeline.name)

class Pipeline:
    
    def __init__(self, api, resp):
        self.id = resp['id']
        self.api = api
        self.name = resp['name']
        self.source = resp['source']
        self.destination = resp['destinations'][0]['destination']
        self.latest_script_version = resp['latestScriptVersion']
        self.paused = resp['paused']
        self.parsing_error_settings = resp.get('parsingErrorSettings', {}) # Address issue where parsingErrorSettings may not be present
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
        return {
            'name': self.name,
            'source': self.source,
            'destination': self.destination,
            'script': self.script,
            'paused': self.paused,
            'script': self.get_script(),
            'parsingErrorSettings': self.parsing_error_settings
        }

class EtleapApiException(Exception):
    def __init__(self, error_text):
        self.error_text = error_text