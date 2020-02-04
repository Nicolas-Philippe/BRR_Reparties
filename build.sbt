name := "ProjectBDD"

version := "0.1"

scalaVersion := "2.12.0"

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4"

useCoursier := false
//mainClass in assembly := Some("ca.lif.sparklauncher.app.Application")
/*mainClass in assembly := Some("cmdline.MainConsole")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}*/
