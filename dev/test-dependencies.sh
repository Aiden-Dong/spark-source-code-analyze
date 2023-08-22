#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

# TODO: This would be much nicer to do in SBT, once SBT supports Maven-style resolution.

# NOTE: These should match those in the release publishing script
HADOOP2_MODULE_PROFILES="-Phive-thriftserver -Pmesos -Pkafka-0-8 -Pkubernetes -Pyarn -Pflume -Phive"
MVN="build/mvn"
HADOOP_PROFILES=(
    hadoop-2.6
    hadoop-2.7
    hadoop-3.1
)

# We'll switch the version to a temp. one, publish POMs using that new version, then switch back to
# the old version. We need to do this because the `dependency:build-classpath` task needs to
# resolve Spark's internal submodule dependencies.

# From http://stackoverflow.com/a/26514030
set +e
OLD_VERSION=$($MVN -q \
    -Dexec.executable="echo" \
    -Dexec.args='${project.version}' \
    --non-recursive \
    org.codehaus.mojo:exec-maven-plugin:1.6.0:exec)
if [ $? != 0 ]; then
    echo -e "Error while getting version string from Maven:\n$OLD_VERSION"
    exit 1
fi
set -e
TEMP_VERSION="spark-$(python -S -c "import random; print(random.randrange(100000, 999999))")"

function reset_version {
  # Delete the temporary POMs that we wrote to the local Maven repo:
  find "$HOME/.m2/" | grep "$TEMP_VERSION" | xargs rm -rf

  # Restore the original version number:
  $MVN -q versions:set -DnewVersion=$OLD_VERSION -DgenerateBackupPoms=false > /dev/null
}
trap reset_version EXIT

$MVN -q versions:set -DnewVersion=$TEMP_VERSION -DgenerateBackupPoms=false > /dev/null

# Generate manifests for each Hadoop profile:
for HADOOP_PROFILE in "${HADOOP_PROFILES[@]}"; do
  echo "Performing Maven install for $HADOOP_PROFILE"
  $MVN $HADOOP2_MODULE_PROFILES -P$HADOOP_PROFILE jar:jar jar:test-jar install:install clean -q

  echo "Performing Maven validate for $HADOOP_PROFILE"
  $MVN $HADOOP2_MODULE_PROFILES -P$HADOOP_PROFILE validate -q

  echo "Generating dependency manifest for $HADOOP_PROFILE"
  mkdir -p dev/pr-deps
  $MVN $HADOOP2_MODULE_PROFILES -P$HADOOP_PROFILE dependency:build-classpath -pl assembly \
    | grep "Dependencies classpath:" -A 1 \
    | tail -n 1 | tr ":" "\n" | rev | cut -d "/" -f 1 | rev | sort \
    | grep -v spark > dev/pr-deps/spark-deps-$HADOOP_PROFILE
done

if [[ $@ == **replace-manifest** ]]; then
  echo "Replacing manifests and creating new files at dev/deps"
  rm -rf dev/deps
  mv dev/pr-deps dev/deps
  exit 0
fi

for HADOOP_PROFILE in "${HADOOP_PROFILES[@]}"; do
  set +e
  dep_diff="$(
    git diff \
    --no-index \
    dev/deps/spark-deps-$HADOOP_PROFILE \
    dev/pr-deps/spark-deps-$HADOOP_PROFILE \
  )"
  set -e
  if [ "$dep_diff" != "" ]; then
    echo "Spark's published dependencies DO NOT MATCH the manifest file (dev/spark-deps)."
    echo "To update the manifest file, run './dev/test-dependencies.sh --replace-manifest'."
    echo "$dep_diff"
    rm -rf dev/pr-deps
    exit 1
  fi
done

exit 0
