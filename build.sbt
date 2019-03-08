name := "Spark2Project"
scalaVersion := "2.11.8"
transitiveClassifiers in Global := Seq(Artifact.SourceClassifier)

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"
libraryDependencies += "org.vegas-viz" %% "vegas-spark" % "0.3.11"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.0"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.2.1"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0" // needed for HTableOutputFormat
libraryDependencies += "com.101tec" % "zkclient" % "0.10"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.46"



resolvers ++= Seq(
  Classpaths.typesafeReleases,
  "confluent" at "http://packages.confluent.io/maven/"
)


assemblyMergeStrategy in assembly := {
 case PathList("META-INF/services/org.apache.spark.sql.sources.DataSourceRegister") => MergeStrategy.concat
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x if Assembly.isConfigFile(x) => MergeStrategy.concat
 case x => MergeStrategy.first
}
