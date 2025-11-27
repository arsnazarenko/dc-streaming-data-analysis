# Build stage
FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

# Copy gradle wrapper and source
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .
COPY src src

# Make gradlew executable and build
RUN chmod +x gradlew && \
    ./gradlew clean shadowJar -x  test && \
    chmod +x build/libs/*.jar && \
    chmod 755 build/libs/*.jar

# Runtime stage  
FROM flink:2.1.1

# Copy the built jar from builder stage
COPY --from=builder /app/build/libs/dc-streaming-data-analysis-1.0-SNAPSHOT-fat.jar /opt/flink/lib/app.jar
# Set working directory
WORKDIR /opt/flink
