#!/usr/bin/env python

# Copyright 2017 Spotify AB.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import datetime
import requests
import subprocess
import sys
import time

METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
METADATA_HEADERS = {'Metadata-Flavor': 'Google'}

GCLOUD_PATH = "gcloud"


def metadata(m):
    url = METADATA_URL + m
    while True:
        r = requests.get(
            url,
            headers=METADATA_HEADERS)

        if r.status_code == 503:
            time.sleep(1)
            continue
        r.raise_for_status()

        return r.text


timestamp = metadata('instance/attributes/heartbeat')
update_time = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
delta = datetime.datetime.utcnow() - update_time
expiry = datetime.timedelta(minutes=int(sys.argv[1]))

if delta > expiry:
    cluster = metadata('instance/attributes/dataproc-cluster-name')
    region = metadata('instance/attributes/dataproc-region')
    project = metadata('project/project-id')

    gcloud_command = [GCLOUD_PATH, "--project=" + project, "dataproc",
                      "clusters", "delete", "--region="+ region, cluster,
                      "--async", "--quiet"]
    subprocess.call(gcloud_command)
