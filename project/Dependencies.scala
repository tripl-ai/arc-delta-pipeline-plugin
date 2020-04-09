import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.5"
  lazy val hadoopVersion = "2.9.2"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"
  val hadoopCommon =  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "it"
  val hadoopAWS = "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "it"
  val junit = "junit" % "junit" % "4.12" % "test"
  val novocode = "com.novocode" % "junit-interface" % "0.11" % "test"

  // arc
  val arc = "ai.tripl" %% "arc" % "2.9.0" % "provided"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()

  // spark
  val sparkCatalyst = "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  // delta
  // val deltaCore = "io.delta" %% "delta-core" % "0.5.0"

  // Project
  val etlDeps = Seq(
    scalaTest,
    hadoopCommon,
    hadoopAWS,
    junit,
    novocode,

    arc,
    typesafeConfig,

    sparkCatalyst,
    sparkCore,
    sparkHive,
    sparkSql

    // deltaCore

  )
}