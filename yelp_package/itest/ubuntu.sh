#!/bin/bash
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -e

# To add additional scripts, just add lines here
SCRIPTS="data_pipeline_tailer
data_pipeline_refresh_runner
data_pipeline_refresh_manager
data_pipeline_refresh_job
data_pipeline_compaction_setter
data_pipeline_introspector"

if dpkg -i /work/dist/*.deb; then
  echo "Looks like it installed correctly"
else
  echo "Dpkg install failed"
  exit 1
fi

for scr in $SCRIPTS
do
  which $scr >/dev/null || (echo "$scr failed to install!"; exit 1)
  echo "Running '$scr -h' to make sure it works"
  $scr -h >/dev/null || (echo "$scr failed to execute!"; exit 1)
done

echo "Everything worked!"
