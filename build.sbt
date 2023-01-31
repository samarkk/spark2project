name := "Spark2Project"
scalaVersion := "2.11.8"
transitiveClassifiers in Global := Seq(Artifact.SourceClassifier)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.5"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % "2.4.5"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "2.4.5"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0" // needed for HTableOutputFormat
libraryDependencies += "com.101tec" % "zkclient" % "0.10"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.46"
libraryDependencies += "io.delta" %"delta-core_2.12" % "0.7.0"
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.0"


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
