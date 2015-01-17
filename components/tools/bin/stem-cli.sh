#!/bin/bash
# Copyright 2014 Alexey Plotnik
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "x$STEM_TOOLS_HOME" = "x" ]; then
    STEM_TOOLS_HOME="`dirname "$0"`/.."
fi

STEM_TOOLS_MAIN=org.stem.tools.cli.StemCli

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=java
fi

if [ ! -f `which $JAVA` ]; then
  echo "Java runtime can not be found!" && exit 1;
fi

#CLASSPATH="$STEM_MANAGER_HOME/conf"

JAVA_OPTS="-ea\
  -Xms256M\
  -Xmx512M\
  -XX:+HeapDumpOnOutOfMemoryError\
  -XX:+UseParNewGC\
  -XX:+UseConcMarkSweepGC\
  -XX:+CMSParallelRemarkEnabled\
  -XX:SurvivorRatio=8\
  -XX:MaxTenuringThreshold=1\
  -XX:CMSInitiatingOccupancyFraction=75\
  -XX:+UseCMSInitiatingOccupancyOnly\
  -Dlogback.configurationFile=logback.xml\
  -DSTEM_TOOLS_HOME=$STEM_TOOLS_HOME"

for file in $(find $STEM_TOOLS_HOME/lib -maxdepth 1 -name '*.jar');
do
    CLASSPATH="$CLASSPATH:$file"
done

exec $JAVA $JAVA_OPTS -cp $CLASSPATH $STEM_TOOLS_MAIN $@