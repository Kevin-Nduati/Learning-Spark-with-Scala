version := "1.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "3.2.2"
val sparkVersion = "3.3.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
