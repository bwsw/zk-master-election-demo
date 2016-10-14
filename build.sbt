name := "curator"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.apache.curator" % "curator-framework" % "2.11.0",
  "org.apache.curator" % "curator-recipes" % "2.11.0",
  "org.scalatest" % "scalatest_2.11" % "3.0.0",
  "org.scalactic" % "scalactic_2.11" % "3.0.0"
)
    