name := "zk-master-election-demo"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "twitter resolver" at "http://maven.twttr.com"
libraryDependencies ++= Seq(
  "com.twitter.common.zookeeper" % "lock" % "0.0.38",
  "com.twitter.common.zookeeper" % "node" % "0.0.55",
  "org.apache.logging.log4j" % "log4j-api" % "2.6.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.6.2"
)
    