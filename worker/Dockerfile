# ── Stage 1: Build ────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q

# ── Stage 2: Runtime ───────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=builder /build/target/worker.jar ./worker.jar

ENV WORKER_ID=""        \
    WORKER_HOST=""      \
    WORKER_PORT=9091    \
    CAPACITY=10         \
    SCHEDULER_HOST=scheduler \
    SCHEDULER_PORT=9090 \
    REDIS_HOST=redis    \
    REDIS_PORT=6379

EXPOSE 9091

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-Djava.net.preferIPv4Stack=true", \
  "-jar", "worker.jar"]
