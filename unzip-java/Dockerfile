FROM amazoncorretto:17@sha256:59b17f5fb85253131eed4379fddc7925fc3b97f52cd5969fe562bcfa7ed12c33
WORKDIR /app
COPY target/unzip-java-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]