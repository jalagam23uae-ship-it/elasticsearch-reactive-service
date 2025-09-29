# ---------- Build stage ----------
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app

# Cache deps
COPY pom.xml ./
RUN mvn -B -q -DskipTests dependency:go-offline

# Build
COPY src ./src
RUN mvn -B -DskipTests package

# Pick the repackaged Spring Boot jar (exclude *.jar.original) and standardize its name
# (There should be exactly one fat jar; adjust if your project produces several)
RUN set -eux; \
    FAT_JAR="$(ls target/*.jar | grep -v '\.jar\.original$' | head -n1)"; \
    cp "$FAT_JAR" /app/app.jar

# ---------- Runtime stage ----------
FROM eclipse-temurin:21-jre
WORKDIR /app

# Run as non-root (optional but recommended)
RUN addgroup --system spring && adduser --system --ingroup spring spring
USER spring:spring

EXPOSE 8080

# Copy the single, known jar
COPY --from=builder /app/app.jar /app/app.jar

# Container-friendly JVM defaults (optional)
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -Dfile.encoding=UTF-8 -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["java","-jar","/app/app.jar"]
