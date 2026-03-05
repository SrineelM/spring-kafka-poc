# ==============================================================================
# DOCKERFILE TUTORIAL
# ==============================================================================
# This defines how the Spring Boot application is packaged into a container.
#
# Recommended Improvements for Production:
# 1. Multi-stage build (build inside docker, instead of expecting a local target).
# 2. Use a non-root user for security.
# 3. Use JRE instead of JDK to reduce image size and attack surface.
# ==============================================================================

# Base image: Java 21 JDK on a lightweight Debian-based OS (slim)
FROM openjdk:21-slim

# Creates a mount point. Spring Boot uses /tmp natively for internal Tomcat directories.
# Making it a volume can marginally improve performance.
VOLUME /tmp

# Copies the compiled jar from your host's target directory into the container.
# Note: You must run `mvn clean package` before running `docker build`.
COPY target/*.jar app.jar

# Defines the default command that runs when the container starts.
# We execute Java to run our Spring Boot artifact.
ENTRYPOINT ["java","-jar","/app.jar"]
