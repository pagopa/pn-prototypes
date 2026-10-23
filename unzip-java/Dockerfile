FROM amazoncorretto:17@sha256:6a3a50176d16d65a09514e28acc1a552ed109e3a59e35236f885dd4c4158952f
WORKDIR /app
COPY target/unzip-java-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]