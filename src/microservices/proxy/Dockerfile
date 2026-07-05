FROM maven:3.9-eclipse-temurin-17-alpine AS builder

WORKDIR /app

COPY . .
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre-alpine

WORKDIR /root/

COPY --from=builder /app/target/proxy-service.jar ./proxy-service.jar

EXPOSE 8000

CMD ["java", "-jar", "proxy-service.jar"]
