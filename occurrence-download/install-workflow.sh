#exit on any failure
set -e

#!/bin/bash
P=$1
TOKEN=$2


echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download/profiles.xml

NAME_NODE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="dev"]]/*[name()="properties"]/*[name()="hdfs.namenode"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
ENV=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="dev"]]/*[name()="properties"]/*[name()="occurrence.environment"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
OOZIE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="dev"]]/*[name()="properties"]/*[name()="oozie.url"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
HBASE_TABLE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="dev"]]/*[name()="properties"]/*[name()="hbase.table"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
HIVE_DB=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="dev"]]/*[name()="properties"]/*[name()="hive.db"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')

#gets the oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=OccurrenceHDFSBuild-$ENV\;status=RUNNING |  awk 'NR==3' | awk '{print $1;}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  oozie job -oozie $OOZIE -kill $WID
fi

echo "Assembling jar for $ENV"
mvn --settings profiles.xml -P$P -DskipTests clean install package assembly:single

echo "Copy to hadoop"
hdfs dfs -rm -r /occurrence-download-workflows-$ENV/
hdfs dfs -copyFromLocal target/occurrence-download-workflows-$ENV/ /
echo -e "oozie.use.system.libpath=true\noozie.coord.application.path=$NAME_NODE/occurrence-download-workflows-$ENV/create-tables\nhiveDB=$HIVE_DB\noccurrenceHBaseTable=$HBASE_TABLE"  > job.properties

oozie job --oozie $OOZIE -config job.properties -run

