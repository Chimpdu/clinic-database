# ---------- Build stage: compile and jar ----------
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src

# Copy sources and the JDBC driver jar
COPY src/ src/
COPY lib/ lib/

# Compile and put classes in /src/out, then build a simple jar
RUN mkdir -p out && \
    javac -cp lib/* -d out src/main/Main.java && \
    jar --create --file app.jar -C out .

# ---------- Run stage: slim JRE ----------
FROM eclipse-temurin:21-jre
WORKDIR /app
ENV PORT=8080

# bring in compiled app and the driver
COPY --from=build /src/app.jar /app/app.jar
COPY --from=build /src/lib /app/lib

EXPOSE 8080

# run with classpath so the driver is found
CMD ["sh","-c","java -cp /app/app.jar:/app/lib/* main.Main"]
