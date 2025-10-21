FROM eclipse-temurin:25 AS builder
WORKDIR /app
RUN jlink --module-path "$JAVA_HOME/jmods" --add-modules jdk.jfr,jdk.jcmd --output /app/jcmd
COPY target/*.jar .

FROM eclipse-temurin:25-jre
RUN groupadd --system app && useradd --system --gid app --shell /sbin/nologin app
WORKDIR /app
COPY --from=builder /app/jcmd /app/jcmd
COPY --from=builder /app/*.jar ./app.jar
ENV PATH="/app/jcmd/bin:$PATH"

RUN chown --recursive app:app /app
RUN chmod --recursive 750 /app

USER app
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
