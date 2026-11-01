plugins { java; application }

group = "dev.taskflow"
version = "0.1.0"
repositories { mavenCentral() }
java { toolchain { languageVersion.set(JavaLanguageVersion.of(21)) } }
dependencies {
    implementation(platform("io.vertx:vertx-stack-depchain:5.2.2"))
    implementation("io.vertx:vertx-web")
    implementation("io.vertx:vertx-pg-client")
    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
application { mainClass.set("dev.taskflow.Main") }
tasks.test { useJUnitPlatform(); testLogging { events("passed", "skipped", "failed") } }
tasks.withType<JavaCompile> { options.encoding = "UTF-8" }
