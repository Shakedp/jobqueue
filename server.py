import os
import sys
import json

from flask import Flask, Response, jsonify

app = Flask("jobqueue")

JOBS_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
JOBS_EXT = ".json"
BAD_JOBS_DIR_PATH = os.path.join(JOBS_DIR_PATH, "Bad")


@app.route('/')
def get_jobs():
    files = [os.path.join(JOBS_DIR_PATH, f) for f in os.listdir(JOBS_DIR_PATH) if
             os.path.splitext(f)[1] == JOBS_EXT and os.path.isfile(os.path.join(JOBS_DIR_PATH, f))]
    if len(files) == 0:
        return Response(status=200)

    is_job = False
    job_path = files[0]
    with open(job_path) as job:
        try:
            content = json.load(job)
            is_job = True
        except json.JSONDecodeError as e:
            content = str(e)

    if is_job:
        os.remove(os.path.join(JOBS_DIR_PATH, files[0]))

        print(f"Returning job {files[0]}", file=sys.stderr)
        return jsonify(content)
    else:
        if not os.path.isdir(BAD_JOBS_DIR_PATH):
            os.makedirs(BAD_JOBS_DIR_PATH)
        os.rename(job_path, os.path.join(BAD_JOBS_DIR_PATH, os.path.basename(job_path)))
        return Response(response=f"Bad file {job_path}: {content}", status=500)


if __name__ == '__main__':
    app.run(port=8000)
