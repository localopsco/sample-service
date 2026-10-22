FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY java/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY java/src ./src
RUN mvn -q -B compile dependency:copy-dependencies -DoutputDirectory=target/lib -DincludeScope=runtime

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/classes ./classes
COPY --from=build /src/target/lib ./lib
EXPOSE 3000
CMD ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-cp", "classes:lib/*", "App"]
