# =========================================================================
# PRODUCTION DOCKERFILE — THE CLOUD-NATIVE SHIP
# =========================================================================
# This file defines the immutable container image for our Spring Boot 
# application. In a production environment (GKE), this image ensures that 
# the code that runs on your machine is EXACTLY the same code that runs 
# in the cloud.
#
# TUTORIAL — Best Practices for Spring Containers:
# 1. Base Image: We use 'openjdk:21-slim' (Debian-based) to strike a 
#    balance between size and compatibility.
# 2. Entropy: We point java.security.egd to /dev/urandom to prevent 
#    JVM startup stalls caused by waiting for entropy.
# 3. Memory Tuning: Containers require explicit -Xmx/-Xms flags to 
#    respect Kubernetes memory limits (cgroups).
# =========================================================================

FROM openjdk:21-slim

# The JAR_FILE argument allows us to pass the filename at build time.
# Default points to the Maven target directory.
ARG JAR_FILE=target/*.jar

# Copies the compiled application into the image. 
# Rename to 'app.jar' for a clean, consistent entrypoint.
COPY ${JAR_FILE} app.jar

# TUTORIAL — JVM Performance Flags for Containers:
# -Xms / -Xmx: Set symmetric heap sizes to avoid costly heap resizes.
# -XX:+UseG1GC: Standard collector for low-latency Spring applications.
# -XX:+ExitOnOutOfMemoryError: Tells the JVM to crash immediately if it 
#   runs out of memory, allowing Kubernetes to restart the pod.
# -Djava.security.egd: Speeds up secure random generation (SSL/OAuth).
ENTRYPOINT ["java", \
            "-Xms2g", "-Xmx2g", \
            "-XX:+UseG1GC", \
            "-XX:+ExitOnOutOfMemoryError", \
            "-Djava.security.egd=file:/dev/./urandom", \
            "-jar", "/app.jar"]

# PRO TIP: For a true production build, you should also include a 
# HEALTHCHECK instruction to allow Docker/K8s to monitor the app health 
# via the Spring Actuator /health endpoint.
