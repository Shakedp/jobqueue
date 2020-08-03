"""
Job queue.
"""

import os
import sys
import json
import shutil
import traceback
import uuid

from flask import Flask, Response
from flask import request

DEFAULT_JOB_QUEUE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queue")
DEFAULT_WAITING_ACK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "waiting")
DEFAULT_BAD_JOBS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bad")

JOB_EXT = ".json"


def _move_to_dir(file_path, dir_path):
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    return shutil.move(file_path, os.path.join(dir_path, os.path.basename(file_path)))


def create_app(job_queue_path=DEFAULT_JOB_QUEUE_PATH, waiting_ack_path=DEFAULT_WAITING_ACK_PATH,
               bad_jobs_path=DEFAULT_BAD_JOBS_PATH):
    app = Flask("jobqueue")

    for path in [job_queue_path, waiting_ack_path, bad_jobs_path]:
        if not os.path.isdir(path):
            os.makedirs(path)

    app.job_queue_path = job_queue_path
    app.waiting_ack_path = waiting_ack_path
    app.bad_jobs_path = bad_jobs_path

    @app.route("/nack/<job_id>", methods=["POST"])
    def nack(job_id):
        if "traceback" not in request.form:
            return Response(response="Needs to include \"traceback\" parameter", status=400)

        traceback_str = request.form["traceback"]
        file_path = os.path.join(app.waiting_ack_path, job_id + JOB_EXT)
        if not os.path.isfile(file_path):
            print(f"Wrong NACK for {job_id}, ignoring", file=sys.stderr)
            return Response(response=f"Unknown job ID \"{job_id}\"", status=500)
        else:
            print(f"Got NACK for {job_id}:\n{traceback_str}", file=sys.stderr)
            _move_to_dir(file_path, app.bad_jobs_path)
            return Response(response="", status=200)

    @app.route("/ack/<job_id>")
    def ack(job_id):
        file_path = os.path.join(app.waiting_ack_path, job_id + JOB_EXT)
        if not os.path.isfile(file_path):
            print(f"Wrong ACK for {job_id}, ignoring", file=sys.stderr)
            return Response(response=f"Unknown job ID \"{job_id}\"", status=500)
        else:
            print(f"Got ACK for {job_id}")
            os.remove(file_path)
            return Response(response="", status=200)

    @app.route('/get')
    def get_job():
        """Gets a job to execute. Move it to the ack waiting dir"""
        files = [os.path.join(app.job_queue_path, f) for f in os.listdir(app.job_queue_path) if
                 os.path.splitext(f)[1] == JOB_EXT and os.path.isfile(os.path.join(app.job_queue_path, f))]
        if len(files) == 0:
            return Response(response="", status=200)

        job_path = files[0]
        job_id = os.path.basename(job_path)[:-len(JOB_EXT)]
        with open(job_path, "r", encoding="utf-8") as job_file:
            content = job_file.read()

        _move_to_dir(job_path, app.waiting_ack_path)

        print(f"Returning job {files[0]}", file=sys.stderr)
        return Response(response=json.dumps({"content": content, "job_id": job_id}), status=200)

    @app.route("/add", methods=["POST"])
    def add_job():
        data = request.data
        try:
            json.loads(data)
            job_id = str(uuid.uuid1())
            job_name = job_id + JOB_EXT
            with open(os.path.join(app.job_queue_path, job_name), "w", encoding="utf-8") as output:
                output.write(data.decode("utf-8"))
            print(f"Written new job to {job_name}")
            return Response(response=json.dumps({"job_id": job_id}), status=200)
        except Exception as e:
            traceback.print_exc()
            return Response(response=traceback.format_exc(), status=400)

    return app


def main():
    app = create_app()
    app.run(port=8000)


if __name__ == '__main__':
    main()
