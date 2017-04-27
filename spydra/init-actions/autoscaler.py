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

import json
import math
import requests
import socket
import subprocess
import sys
import time

METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
METADATA_HEADERS = {'Metadata-Flavor': 'Google'}

GCLOUD_PATH = "gcloud"

RM_URL = 'http://' + socket.gethostname() + ':8088/ws/v1/cluster/metrics'


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


def cluster_metrics():
    return json.loads(requests.get(RM_URL).text)['clusterMetrics']


def preemptable_worker_count():
    gcloud_command = [GCLOUD_PATH, "dataproc", "clusters", "describe", cluster, "--format=json", "--quiet"]
    j = json.loads(subprocess.check_output(gcloud_command))
    if not 'secondaryWorkerConfig' in j['config']:
        return 0  # Has no secondary workers yet
    return int(j['config']['secondaryWorkerConfig']['numInstances'])


def worker_count():
    gcloud_command = [GCLOUD_PATH, "dataproc", "clusters", "describe", cluster, "--format=json", "--quiet"]
    j = json.loads(subprocess.check_output(gcloud_command))
    if not 'workerConfig' in j['config']:
        return 0  # Has no secondary workers yet
    return int(j['config']['workerConfig']['numInstances'])


def scale(cluster, worker_count):
    gcloud_command = [GCLOUD_PATH, "dataproc", "clusters", "update", cluster,
                      "--num-preemptible-workers=" + str(worker_count), "--quiet"]
    print "Executing: " + str(gcloud_command)
    subprocess.call(gcloud_command)


autoscaler_max = int(metadata('instance/attributes/autoscaler-max'))
factor = float(metadata('instance/attributes/autoscaler-factor'))
downscale = metadata('instance/attributes/autoscaler-mode') == 'downscale'
cluster = metadata('instance/attributes/dataproc-cluster-name')

current_preemtable_count = preemptable_worker_count()
current_worker_count = worker_count()
metrics = cluster_metrics()
active_nodes = int(metrics['activeNodes'])
total_mb = int(metrics['totalMB'])
allocated_mb = int(metrics['allocatedMB'])
available_mb = int(metrics['availableMB'])
containers_allocated = int(metrics['containersAllocated'])
containers_pending = int(metrics['containersPending'])

if active_nodes == 0 or total_mb == 0 or containers_allocated == 0 or allocated_mb == 0:
    print "Nothing to do, exit"
    sys.exit(0)

container_size = allocated_mb / float(containers_allocated)
memory_per_node = total_mb / active_nodes
containers_per_node = memory_per_node / container_size
total_containers = containers_per_node * active_nodes

if containers_pending == 0:
    current_factor = 1
else:
    current_factor = total_containers / float(containers_pending)
nodes_required = int(math.ceil((((containers_allocated + containers_pending) * factor)) / containers_per_node))
preemptable_nodes_required = nodes_required - current_worker_count
preemptable_nodes_required = min(autoscaler_max, preemptable_nodes_required)
preemptable_nodes_required = max(0, preemptable_nodes_required)

if current_preemtable_count == preemptable_nodes_required:
    sys.exit(0)

if current_factor > factor and not downscale:
    print "Downscaling is disabled, continue"
    sys.exit(0)

if current_factor == factor:
    print "Perfectly sized, continue"
    sys.exit(0)

print "Decided to scale. Current factor is %s. Requiring %s nodes." % (current_factor, preemptable_nodes_required)
scale(cluster, preemptable_nodes_required)
