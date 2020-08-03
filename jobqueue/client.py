import json

import requests


class JobQueueException(Exception):
    pass


class JobQueueClient:
    def __init__(self, url):
        self._url = url

    def add_job(self, job):
        if not isinstance(job, dict):
            raise JobQueueException(f"job should be a dictionary, not a {type(job)}")
        response = requests.post(f"{self._url}/add", json.dumps(job))
        if 200 != response.status_code:
            raise JobQueueException(response.content)
        else:
            return json.loads(response.content)["job_id"]

    def get_job(self):
        response = requests.get(f"{self._url}/get")
        if response.status_code != 200:
            raise JobQueueException(response.content)
        elif not response.content:
            return None
        else:
            parsed_response = json.loads(response.content)
            return parsed_response["job_id"], parsed_response["content"]

    def ack(self, job_id):
        response = requests.get(f"{self._url}/ack/{job_id}")
        if response.status_code != 200:
            raise JobQueueException(response.content)

    def nack(self, job_id, traceback_str):
        response = requests.post(f"{self._url}/nack/{job_id}", data={"traceback": traceback_str})
        if response.status_code != 200:
            raise JobQueueException(response.content)
