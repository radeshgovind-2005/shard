plugins {
    kotlin("jvm") version "2.4.20"
    application
    id("com.diffplug.spotless") version "8.11.0"
}

group = "com.kognito"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(24)
}

application {
    mainClass = "com.radesh-govind.MainKt"
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
}
