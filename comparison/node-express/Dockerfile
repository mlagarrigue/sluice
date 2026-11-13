# The Node contender in containers (study §1.4): two images, as for the
# code (§2.4). Build from the repository root, like the study's other images:
#
#   docker build -f comparison/node-express/Dockerfile --target standard-doc -t node-express:standard .
#   docker build -f comparison/node-express/Dockerfile --target optimised-opt -t node-express:optimised .
#
# dataloader-doc is the standard version's image running its variant "with" a
# DataLoader (CMP-25); dataloader-optimised-opt is the optimised image running
# the optimised variant "with", its batches on a time window.
#
# standard-doc is Docker's guide for Node: the alpine Node image, the
# packages installed by npm ci without the development ones, NODE_ENV set to
# production, run as the image's node user. optimised-opt is the optimised
# version on a distroless Node image of the same Node release, nothing else
# in it, run as a user with no rights. Every image reads the contract's
# variables and listens on STUDY_LISTEN, 0.0.0.0:8443 by default here. Node
# has no API to make a certificate at startup, so the images carry one for a
# local run, made when they are built and named by STUDY_TLS_CERT and
# STUDY_TLS_KEY; a campaign names its own. The bases are pinned by digest.
FROM node:25-alpine@sha256:bdf2cca6fe3dabd014ea60163eca3f0f7015fbd5c7ee1b0e9ccb4ced6eb02ef4 AS cert
RUN apk add --no-cache openssl && mkdir /cert && \
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj /CN=study-contender -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
      -keyout /cert/key.pem -out /cert/cert.pem && chmod 644 /cert/key.pem /cert/cert.pem

FROM node:25-alpine@sha256:bdf2cca6fe3dabd014ea60163eca3f0f7015fbd5c7ee1b0e9ccb4ced6eb02ef4 AS standard-doc
ENV NODE_ENV=production
WORKDIR /usr/src/app
COPY comparison/node-express/package.json comparison/node-express/package-lock.json ./
RUN npm ci --omit=dev
COPY comparison/node-express/ ./
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
USER node
EXPOSE 8443
CMD ["node", "standard/server.js"]

FROM standard-doc AS dataloader-doc
CMD ["node", "dataloader/server.js"]

FROM standard-doc AS build-opt

FROM gcr.io/distroless/nodejs24-debian13:nonroot@sha256:6abfcb367ebc291572c774c1592a986be2ab26a2486f73093401327495a5962a AS optimised-opt
WORKDIR /app
COPY --from=build-opt /usr/src/app /app
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
EXPOSE 8443
CMD ["optimised/server.js"]

FROM optimised-opt AS dataloader-optimised-opt
CMD ["dataloader-optimised/server.js"]
