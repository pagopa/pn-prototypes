FROM amazoncorretto:17@sha256:87bfb990b67df35d0ff3cd5780cf03a4bdd47a5c5986c1aa9acc38e0b223de62
WORKDIR /app
COPY target/unzip-java-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]