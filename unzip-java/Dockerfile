FROM amazoncorretto:17@sha256:335231cadb7eee6f92303e91461b73282fb4c5fb7c02fe23d45c547730a451c1
WORKDIR /app
COPY target/unzip-java-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]