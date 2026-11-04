plugins {
    kotlin("jvm") version "2.4.20"
    application
    id("com.gradleup.shadow") version "9.6.1"
    id("com.diffplug.spotless") version "8.11.0"
}

group = "com.kognito"
version = "1.0-SNAPSHOT"

val sparkVersion = providers.gradleProperty("sparkVersion").get()

repositories {
    mavenCentral()
}

// spark-submit provides Spark on the cluster, so it stays out of the fat jar,
// but `make run` and the tests still need it on their classpath.
val spark: Configuration = configurations.create("spark")
configurations.compileOnly { extendsFrom(spark) }
configurations.testImplementation { extendsFrom(spark) }

dependencies {
    spark("org.apache.spark:spark-sql_2.13:$sparkVersion")
    testImplementation(kotlin("test"))
}

// Spark 4.2 supports Java 17/21/25. 21 is what clusters typically run: bytecode built for a newer JDK won't load there.
kotlin {
    jvmToolchain(21)
}

// spark-submit adds these module flags itself. Plain JVM launches (`make run`, tests) need them explicitly.
// Mirrors org.apache.spark.launcher.JavaModuleOptions.
val sparkJvmArgs =
    listOf(
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-modules=jdk.incubator.vector",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Djdk.reflect.useDirectMethodHandle=false",
        "-Dio.netty.tryReflectionSetAccessible=true",
    )

application {
    mainClass = "com.kognito.MainKt"
    applicationDefaultJvmArgs = sparkJvmArgs
}

tasks.named<JavaExec>("run") {
    classpath += spark
}

// Fat jar for spark-submit: our code + the Kotlin stdlib, without Spark. Fixed name keeps submit commands stable.
tasks.shadowJar {
    archiveFileName = "shard-all.jar"
}

spotless {
    kotlin {
        target("src/**/*.kt")
        ktlint()
    }
    kotlinGradle {
        target("*.gradle.kts")
        ktlint()
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs(sparkJvmArgs)
}
