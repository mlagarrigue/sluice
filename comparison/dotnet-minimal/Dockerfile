# The .NET contender in containers (study §1.4): two images. Build from the
# repository root, like the study's other images:
#
#   docker build -f comparison/dotnet-minimal/Dockerfile --target standard-doc -t dotnet-minimal:standard-doc .
#   docker build -f comparison/dotnet-minimal/Dockerfile --target optimised-opt -t dotnet-minimal:optimised-opt .
#
# standard-doc is the ASP.NET Core documentation's Dockerfile: the SDK image
# restores and publishes, the ASP.NET Core runtime image runs the app as its
# non-root user. optimised-opt publishes ReadyToRun with invariant
# globalisation onto the chiseled runtime image, as the README lists. Both
# carry msquic, which Kestrel's HTTP/3 needs on Linux and the runtime images
# do not: libmsquic from packages.microsoft.com, pinned, with the libnuma it
# links. The images carry a certificate for a local run, made when they are
# built; a campaign names its own. The bases are pinned by digest.
FROM alpine:3@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS cert
RUN apk add --no-cache openssl && mkdir /cert && \
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj /CN=study-contender -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
      -keyout /cert/key.pem -out /cert/cert.pem && chmod 644 /cert/key.pem /cert/cert.pem

FROM mcr.microsoft.com/dotnet/sdk:10.0@sha256:fe0fd4e93bd2fbb47c883b7a6369e5c8d3db8bf01cdc5981f6ee35fbdf3ff4fb AS msquic
RUN . /etc/os-release && \
    wget -q "https://packages.microsoft.com/config/$ID/$VERSION_ID/packages-microsoft-prod.deb" -O /tmp/p.deb && \
    dpkg -i /tmp/p.deb && apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends libmsquic=2.6.4 libnuma1 && \
    mkdir /msquic && cp -a /usr/lib/x86_64-linux-gnu/libmsquic.so.2* /usr/lib/x86_64-linux-gnu/libnuma.so.1* /msquic/

FROM mcr.microsoft.com/dotnet/sdk:10.0@sha256:fe0fd4e93bd2fbb47c883b7a6369e5c8d3db8bf01cdc5981f6ee35fbdf3ff4fb AS build
WORKDIR /source
COPY comparison/dotnet-minimal/Study.csproj comparison/dotnet-minimal/packages.lock.json ./
RUN dotnet restore --locked-mode
COPY comparison/dotnet-minimal/*.cs comparison/dotnet-minimal/appsettings.json ./
RUN dotnet publish --no-restore -c Release -o /app
RUN dotnet publish -p:RestoreLockedMode=true -c Release -r linux-x64 --self-contained false \
      -p:PublishReadyToRun=true -p:InvariantGlobalization=true -o /app-opt

FROM mcr.microsoft.com/dotnet/aspnet:10.0@sha256:bfd912c83295ce66d4b706ebb8541b9f14259b0e76fbb88b2751f58ce2399d2e AS standard-doc
WORKDIR /app
COPY --from=build /app .
COPY --from=msquic /msquic/ /usr/lib/x86_64-linux-gnu/
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
EXPOSE 8443
USER $APP_UID
ENTRYPOINT ["./Study"]

FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled@sha256:7c625e44e5d588b4d7b2a913e94ca4bd5098ce47442da404829e4c7936a764e1 AS optimised-opt
WORKDIR /app
COPY --from=build /app-opt .
COPY --from=msquic /msquic/ /usr/lib/x86_64-linux-gnu/
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem \
    DOTNET_GCDynamicAdaptationMode=0
EXPOSE 8443
ENTRYPOINT ["./Study"]
