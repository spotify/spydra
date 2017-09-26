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

INTERVAL=$(/usr/share/google/get_metadata_value attributes/collector-timeout || echo -1)
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' || $HOSTNAME =~ .*0 ]]; then
  gsutil cp ${versioned-init-action-uri}/self-destruct.py /usr/local/bin/self-destruct
  chmod +x /usr/local/bin/self-destruct
  crontab -l | { cat; echo "* * * * * /usr/local/bin/self-destruct ${INTERVAL}"; } | crontab -
fi
