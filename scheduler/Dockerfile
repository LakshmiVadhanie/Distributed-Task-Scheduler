# ── Stage 1: Build ────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build
COPY pom.xml .
# Download dependencies first (Docker layer cache)
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q

# ── Stage 2: Runtime ───────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre

WORKDIR /app
RUN mkdir -p logs

COPY --from=builder /build/target/scheduler.jar ./scheduler.jar

# Default env vars (overridden by Kubernetes ConfigMap / env)
ENV REDIS_HOST=redis \
    REDIS_PORT=6379 \
    GRPC_PORT=9090 \
    SCHEDULER_ID=""

EXPOSE 9090

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-Djava.net.preferIPv4Stack=true", \
  "-jar", "scheduler.jar"]
