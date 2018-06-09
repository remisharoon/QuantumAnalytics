
name := "Qunatum Analytics"

version := "0.1"

scalaVersion := "2.11.7"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "2.2.1" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming" % "2.2.1" withSources() withJavadoc(),
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.1" withSources() withJavadoc(),
  "com.github.nscala-time" %% "nscala-time" % "2.20.0" withSources() withJavadoc(),
  "mysql" % "mysql-connector-java" % "5.1.12"
)


resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"