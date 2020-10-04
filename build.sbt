resolvers += Resolver.bintrayRepo("swoop-inc", "maven")

name := "hll-example"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.swoop" %% "spark-alchemy" % "0.5.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "0.38.0"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "0.21.2" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
