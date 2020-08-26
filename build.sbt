ThisBuild / name := "swiss-address-repository"
ThisBuild / version := "0.0.1"
ThisBuild / scalaVersion := "2.12.7"

lazy val global = project
  .in(file("."))
  .settings(settings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    core,
    infrastructure,
    processing,
    api
  )

lazy val core = project
  .settings(
    name := "core",
    settings,
    libraryDependencies ++= commonDependencies
  )
  .disablePlugins(AssemblyPlugin)

lazy val infrastructure = project
  .settings(
    name := "infrastructure",
    settings,
    libraryDependencies ++= commonDependencies
  )
  .disablePlugins(AssemblyPlugin)

lazy val processing = project
  .settings(
    name := "processing",
    settings,
    libraryDependencies ++= commonDependencies++ Seq(
      dependencies.guava,
      dependencies.spark,
      dependencies.sparkSql,
      dependencies.sparkSession,
      dependencies.elastic,
      dependencies.elasticSpark,
      dependencies.kafkaClient
    )
  )
  .disablePlugins(AssemblyPlugin)

lazy val api = project
  .settings(
    name := "api",
    settings,
    libraryDependencies ++= commonDependencies
  )
  .disablePlugins(AssemblyPlugin)

lazy val dependencies =
  new {
    val sl4jVersion = "1.7.30"
    val log4jVersion = "2.13.3"
    val akkaVersion = "2.6.8"
    val sparkVersion = "3.0.0"
    val scalaTestVersion = "3.2.2"
    val scalacheckVersion = "1.14.3"
    val scalaMockVersion = "5.0.0"
    val scalaLoggingVersion = "3.9.2"
    val guavaVersion = "29.0-jre"
    val elasticVersion = "7.9.0"
    val kafkaClientVersion = "2.6.0"

    val log4jCore = "org.apache.logging.log4j" % "log4j-core" % log4jVersion
    val log4jApi = "org.apache.logging.log4j" % "log4j-api" % log4jVersion
    val log4jSlf4jImpl= "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion
    val slf4j = "org.slf4j" % "slf4j-api" % sl4jVersion

    val scalaLogging   = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion

    val spark = "org.apache.spark" %% "spark-core" % sparkVersion
    val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
    val sparkSession = "org.apache.spark" %% "spark-streaming" % sparkVersion

    val elastic = "org.elasticsearch" % "elasticsearch-hadoop" % elasticVersion
    val elasticSpark = "org.elasticsearch" %% "elasticsearch-spark-20" % elasticVersion

    val kafkaClient = libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaClientVersion

    val guava = "com.google.guava" % "guava" % guavaVersion

    val akkaActor = "com.typesafe.akka" %% "akka-actor" % akkaVersion

    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
    val akkaActorTest = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
    val scalacheck = "org.scalacheck" %% "scalacheck"% scalacheckVersion
    val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion
  }

lazy val commonDependencies = Seq(
  dependencies.log4jCore,
  dependencies.log4jApi,
  dependencies.slf4j,
  dependencies.scalaLogging,

  // dependencies.typesafeConfig,
  // dependencies.akka,
  dependencies.scalaTest  % Test,
  dependencies.scalacheck % Test,
  dependencies.scalaMock % Test
)

lazy val settings =
  commonSettings/* ++
  wartremoverSettings ++
  scalafmtSettings*/

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    Resolver.mavenCentral,
    Resolver.mavenLocal,
    //"Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

/*
lazy val wartremoverSettings = Seq(
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(Wart.Throw)
)

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "application.conf"            => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)
*/
