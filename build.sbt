name := "access-log-analytics"

scalaVersion := "2.12.11"
val sparkVersion = "3.0.0"

//scalaVersion := "2.11.8"
//val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.github.scopt" %% "scopt" % "3.7.1"
)

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}


