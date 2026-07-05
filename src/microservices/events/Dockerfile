FROM maven:3.9-eclipse-temurin-17-alpine AS builder

WORKDIR /app

COPY . .
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre-alpine

WORKDIR /root/

COPY --from=builder /app/target/events-service.jar ./events-service.jar

EXPOSE 8082

CMD ["java", "-jar", "events-service.jar"]
