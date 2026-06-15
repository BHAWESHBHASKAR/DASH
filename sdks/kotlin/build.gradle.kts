plugins {
    kotlin("jvm") version "1.9.24"
    id("io.github.gradle-nexus.publish-plugin") version "1.3.0"
}

group = "dev.dash"
version = "0.2.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    api("dev.dash:dash-java:0.2.0")
    api("com.squareup.okhttp3:okhttp:4.12.0")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    testImplementation("org.assertj:assertj-core:3.25.3")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

kotlin {
    jvmToolchain(17)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("dash-kotlin")
                description.set("Kotlin coroutine wrappers around the DASH Java SDK")
                url.set("https://github.com/dash-retrieval/dash")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        name.set("DASH Contributors")
                    }
                }
                scm {
                    url.set("https://github.com/dash-retrieval/dash")
                }
            }
        }
    }
}
