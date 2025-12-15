plugins {
    scala
    `maven-publish`
    id("com.github.johnrengelman.shadow") version "8.1.1"

}

group = "org.openproject.data.spark.tg"
version = "0.1.0-SNAPSHOT"

val scalaVersion = "2.13"
val scalaVersionFull = "2.13.8"
val sparkVersion = "3.5.5"

//tasks.withType<JavaCompile>().configureEach {
//    dependsOn(tasks.withType<ScalaCompile>())
//}
//
//
//tasks.named<ScalaCompile>("compileScala") {
//    scalaCompileOptions.additionalParameters = listOf("-Ybackend-parallelism", "8")
//}

//tasks.withType<ScalaCompile>().configureEach {
//    source = sourceSets["main"].allSource
//}
//
//tasks.named<JavaCompile>("compileJava") {
//    enabled = false
//}

sourceSets {
    main {
//        withConvention(org.gradle.api.tasks.scala.ScalaSourceSet::class) {
//            scala.setSrcDirs(listOf("src/main/scala", "src/main/java")) // Include Java sources in Scala source set
//        }
        scala {
            setSrcDirs(listOf("src/main/scala" ,"src/main/java"))
        }

        java.setSrcDirs(emptyList<String>()) // Exclude Java sources from Java source set
    }
//    test {
//        withConvention(org.gradle.api.tasks.scala.ScalaSourceSet::class) {
//            scala.setSrcDirs(listOf("src/test/scala", "src/test/java"))
//        }
//        java.setSrcDirs(emptyList<String>())
//    }
}

repositories {
//    jcenter()
//    mavenLocal()
    mavenCentral()
//    maven {
//        url = uri("https://oss.sonatype.org/content/repositories/snapshots")
//    }
    maven {
        url = uri("https://packages.confluent.io/maven")
    }
}

val sparkProvided: Configuration by configurations.creating

val isPublishTask = gradle.startParameter.taskNames.any { name ->
    name.equals("publish", ignoreCase = true) ||
            name.startsWith("publish", ignoreCase = true)
}

val localRun: Boolean = when {
    // if explicitly set, honor it
    project.hasProperty("localRun") -> project.property("localRun").toString().toBoolean()
    // else, if publish is in task graph, force false
    isPublishTask -> false
    // default otherwise
    else -> true
}

if (localRun) {
    // For local run: treat as implementation
    configurations.implementation {
        logger.info("build for local run")
        extendsFrom(sparkProvided)
//        configurations.dependencyScope("sparkProvided").get()
    }
} else {
    // For cluster publish: treat as compileOnly + runtimeOnly
    configurations.compileOnly {
        extendsFrom(sparkProvided)
    }
//    configurations.runtimeOnly {
//        extendsFrom(configurations.sparkProvided.get())
//    }
}

dependencies {
    sparkProvided("org.scala-lang:scala-library:$scalaVersionFull")
    sparkProvided("org.scala-lang:scala-reflect:$scalaVersionFull")
    sparkProvided("org.scala-lang:scala-compiler:$scalaVersionFull")

    sparkProvided("org.apache.spark:spark-mllib_$scalaVersion:$sparkVersion")
    sparkProvided("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    sparkProvided("org.apache.spark:spark-graphx_$scalaVersion:$sparkVersion")
    sparkProvided("org.apache.spark:spark-launcher_$scalaVersion:$sparkVersion")
    sparkProvided("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
    sparkProvided("org.apache.spark:spark-streaming_$scalaVersion:$sparkVersion")
    sparkProvided("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
    sparkProvided("org.apache.spark:spark-sql-kafka-0-10_$scalaVersion:$sparkVersion")
    implementation("org.apache.spark:spark-avro_$scalaVersion:$sparkVersion")
//    implementation("io.confluent:kafka-avro-serializer:8.1.1")
    implementation("org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.0")

    implementation("com.yugabyte:jdbc-yugabytedb:42.7.3-yb-4")
    implementation("org.postgresql:postgresql:42.7.7")

//    implementation("org.apache.hadoop:hadoop-aws:3.3.4")
    // https://mvnrepository.com/artifact/software.amazon.awssdk/s3
    implementation("software.amazon.awssdk:s3:2.31.74")
    implementation("software.amazon.awssdk:sts:2.31.74")
    implementation("software.amazon.awssdk:dynamodb:2.31.74")
    implementation("software.amazon.awssdk:kms:2.31.74")
    implementation("software.amazon.awssdk:glue:2.31.74")
//    implementation("io.circe:circe-core_2.13:0.14.15")
    implementation("io.circe:circe-parser_2.13:0.14.15")

    implementation("commons-io:commons-io:2.5")
}

tasks.shadowJar {
    isZip64 = true
    archiveClassifier.set("all") // or "" to replace the default jar

    // ✅ Merge service/resource files that otherwise overwrite each other
    transform(com.github.jengelman.gradle.plugins.shadow.transformers.ServiceFileTransformer())


}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"]) // For Java libraries or use kotlin?

//            groupId =  group.toString()
//            artifactId = "tg-crawler"
//            version = "0.1.0"
        }
    }

    repositories {
        maven {
            name = "OpenProjectX"
            val snapshotsRepoUrl = uri("https://reposilite.devops.cn-guangzhou-a.k8s.openprojectx.org/snapshots")
            val releasesRepoUrl = uri("https://reposilite.devops.cn-guangzhou-a.k8s.openprojectx.org/releases")

            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl


            credentials {
                username = findProperty("repoUser") as String? ?: System.getenv("REPO_USER")
                password = findProperty("repoPassword") as String? ?: System.getenv("REPO_PASSWORD")
            }
        }
    }
}