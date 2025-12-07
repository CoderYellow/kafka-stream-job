```shell
./gradlew dependencies --configuration runtimeClasspath

kubectl -n data create -f /data/Git/kafka-stream-job/test/orders-topic.yaml

rm -rf /data/.gradle//caches/modules-2/files-2.1/org.scala-lang
rm -rf /data/.gradle//caches/modules-2/files-2.1/org.scala-sbt
rm -rf /data/.gradle//caches/modules-2/files-2.1/io.confluent
rm -rf /data/.gradle//caches/jars-9

rm -rf /data/.gradle/caches/modules-2/files-2.1
rm -rf /data/.gradle/caches/transforms-3
rm -rf /data/.gradle/caches/jars-9

tree
cat metadata
cat commits/5
cat offsets/0
cat offsets/5
cat sources/0/0

```