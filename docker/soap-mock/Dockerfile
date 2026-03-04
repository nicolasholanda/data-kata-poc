FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn package -q

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/data-kata-soap-mock-1.0.0-SNAPSHOT-jar-with-dependencies.jar app.jar
EXPOSE 8090 8091
ENTRYPOINT ["java", "-jar", "app.jar"]
