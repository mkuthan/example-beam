// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

lazy val beamVersion = "2.17.0"
lazy val scioVersion = "0.8.0"

lazy val commonSettings = Seq(
  name := "beam-examples",
  version := "1.0",
  organization := "http://mkuthan.github.io/",
  scalaVersion := "2.12.10"
)

// https://tpolecat.github.io/2017/04/25/scalac-flags.html
lazy val customScalacOptions = Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding", "UTF-8", // Specify character encoding used by source files.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xlint",
  "-Yno-adapted-args", // Do not adapt an argument list to match the receiver.
  "-Ypartial-unification", // Enable partial unification in type constructor inference
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-extra-implicit", // Warn when more than one implicit parameter section is defined.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-unused",
  "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
)

lazy val customLibraryDependencies = Seq(
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,

  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-bigquery" % scioVersion,

  "com.spotify" %% "scio-test" % scioVersion % Test,

  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

lazy val commonExcludeDependencies = Seq(
  "org.slf4j" % "slf4j-log4j12"
)

lazy val customJavaOptions = Seq(
  "-Xmx1024m",
  "-XX:-MaxFDLimit"
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(scalacOptions ++= customScalacOptions)
  .settings(libraryDependencies ++= customLibraryDependencies)
  .settings(excludeDependencies ++= commonExcludeDependencies)
  .settings(fork in run := true)
  .settings(connectInput in run := true)
  .settings(javaOptions in run ++= customJavaOptions)
  .settings(resolvers += Resolver.sonatypeRepo("snapshots"))
  .settings(addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full))
  .settings(
    scalastyleFailOnError := true,
    compileScalastyle := scalastyle.in(Compile).toTask("").value,
    (compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value,
    testScalastyle := scalastyle.in(Test).toTask("").value,
    (test in Test) := ((test in Test) dependsOn testScalastyle).value)
