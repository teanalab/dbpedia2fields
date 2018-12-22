name := "dbpedia2fields"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.scalactic" %% "scalactic" % "[2.2,3.0[",
  "org.scalatest" %% "scalatest" % "[2.2,3.0[" % "test"
)
