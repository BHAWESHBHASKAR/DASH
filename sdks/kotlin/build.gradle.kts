plugins {
    kotlin("jvm") version "1.9.24"
    `maven-publish`
}

group = "dev.dash"
version = "0.2.0"

// Target Java 17 bytecode with whatever JDK (17+) is installed; avoids
// requiring toolchain auto-provisioning.
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions.jvmTarget = "17"
}

repositories {
    // dash-java is built from ../java in this repository and is not yet on
    // Maven Central. Run `mvn -q install -DskipTests` in sdks/java first so
    // it resolves from the local Maven repository.
    mavenLocal()
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

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("dash-kotlin")
                description.set("Kotlin coroutine wrappers around the DASH Java SDK")
                url.set("https://github.com/BHAWESHBHASKAR/DASH")
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
                    url.set("https://github.com/BHAWESHBHASKAR/DASH")
                }
            }
        }
    }
}
