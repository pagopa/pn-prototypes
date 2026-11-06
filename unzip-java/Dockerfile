FROM amazoncorretto:17@sha256:b505ecf1fd737d85c0fd3930b8ab3ed0cb560326809aacc838b3938a9bc84c4b
WORKDIR /app
COPY target/unzip-java-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]