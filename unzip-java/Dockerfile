FROM amazoncorretto:17
WORKDIR /app
COPY target/unzip-java-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]