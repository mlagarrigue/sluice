# The Java contender in containers (study §1.4): two images. Build from the
# repository root, like the study's other images:
#
#   docker build -f comparison/java-spring/Dockerfile --target standard-doc -t java-spring:standard-doc .
#   docker build -f comparison/java-spring/Dockerfile --target optimised-opt -t java-spring:optimised-opt .
#
# The jar is built once, by Maven, from pom.xml. standard-doc is the Spring
# Boot reference's "Dockerfiles" page: the jar extracted in layers onto the
# JRE image it names, started by `java -jar`, the JVM's defaults. The
# reference's other way, `spring-boot:build-image` (Buildpacks), cannot carry
# the certificate a local run needs without a binding: the deviation is
# declared in comparison/README.md. optimised-opt runs the same jar on a JRE
# cut by jlink to the modules it uses, on a distroless base, as a user with
# no rights, with the JDK's ahead-of-time cache made by a training run and
# the settings the README lists. The images carry a certificate for a local
# run, made when they are built; a campaign names its own. The bases are
# pinned by digest.
FROM alpine:3@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS cert
RUN apk add --no-cache openssl && mkdir /cert && \
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj /CN=study-contender -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
      -keyout /cert/key.pem -out /cert/cert.pem && chmod 644 /cert/key.pem /cert/cert.pem

FROM maven:3-eclipse-temurin-26@sha256:69a6c37f0a91fca2c9d0cdd684c9c0ea7b1bc00d4196749c24440b2e02ad163d AS jar
WORKDIR /build
COPY comparison/java-spring/pom.xml pom.xml
RUN mvn -q -B dependency:go-offline
COPY comparison/java-spring/src src
RUN mvn -q -B -o package -DskipTests && mv target/java-spring-*.jar application.jar

FROM bellsoft/liberica-openjre-debian:25-cds@sha256:bc35e6ed0cda3f4670900de782b0a7d3ad38bc8c5738f208110a8d57fcb4192a AS builder
WORKDIR /builder
COPY --from=jar /build/application.jar application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM bellsoft/liberica-openjre-debian:25-cds@sha256:bc35e6ed0cda3f4670900de782b0a7d3ad38bc8c5738f208110a8d57fcb4192a AS standard-doc
WORKDIR /application
COPY --from=builder /builder/extracted/dependencies/ ./
COPY --from=builder /builder/extracted/spring-boot-loader/ ./
COPY --from=builder /builder/extracted/snapshot-dependencies/ ./
COPY --from=builder /builder/extracted/application/ ./
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
EXPOSE 8443
ENTRYPOINT ["java", "-jar", "application.jar"]

# The JRE of the optimised image: the modules the jar uses, without debug
# symbols, man pages or header files.
FROM eclipse-temurin:25-jdk@sha256:b8705e936964c228dfce359e63dbf08f6c69cb380ed1d576b1f6b1064f15567d AS jre
RUN jlink --add-modules java.base,java.desktop,java.instrument,java.logging,java.management,java.naming,java.net.http,java.security.jgss,java.sql,java.xml,jdk.jfr,jdk.management,jdk.unsupported \
      --strip-debug --no-man-pages --no-header-files --output /opt/jre
COPY --from=builder /builder/extracted/dependencies/ /application/
COPY --from=builder /builder/extracted/spring-boot-loader/ /application/
COPY --from=builder /builder/extracted/snapshot-dependencies/ /application/
COPY --from=builder /builder/extracted/application/ /application/
COPY --from=cert /cert /cert
# The training run: the context starts and exits before it listens or
# reaches the database (spring.context.exit=onRefresh), and the JVM writes
# the classes it loaded and linked to the cache (JEP 514). The cache is only
# valid for this JRE and this class path, which the final image keeps at the
# same paths.
WORKDIR /application
RUN STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem \
    /opt/jre/bin/java -XX:+UseZGC -XX:AOTCacheOutput=/application/application.aot -Dspring.context.exit=onRefresh -jar application.jar

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:cb87acc574abe719b6e5c547384defa2f7b1f24af9605d3f618cf852495d8be2 AS optimised-opt
COPY --from=jre /opt/jre /opt/jre
COPY --from=jre /application /application
COPY --from=cert /cert /cert
WORKDIR /application
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem \
    SPRING_THREADS_VIRTUAL_ENABLED=true
EXPOSE 8443
ENTRYPOINT ["/opt/jre/bin/java", "-XX:+UseZGC", "-XX:MaxRAMPercentage=75", "-XX:AOTCache=/application/application.aot", "-jar", "application.jar"]
