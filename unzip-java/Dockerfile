FROM amazoncorretto:17@sha256:17b4903d0849e3943762411b4601fa7bffed96cf9be5d43f4629a72e3c7e3166
WORKDIR /app
COPY target/unzip-java-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]