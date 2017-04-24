#!/usr/bin/env bash

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

set -e

INTERVAL=$(/usr/share/google/get_metadata_value attributes/autoscaler-interval || echo -1)
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ ${INTERVAL} != -1 && "${ROLE}" == 'Master' ]]; then
  gsutil cp ${versioned-init-action-uri}/autoscaler.py /usr/local/bin/autoscaler
  chmod +x /usr/local/bin/autoscaler
  crontab -l | { cat; echo "*/${INTERVAL} * * * * /usr/local/bin/autoscaler"; } | crontab -
fi
