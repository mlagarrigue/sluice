# The Python contender in containers (study §1.4): two images, as for the
# code (§2.4). Build from the repository root, like the study's other images:
#
#   docker build -f comparison/python-fastapi/Dockerfile --target standard-doc -t python-fastapi:standard .
#   docker build -f comparison/python-fastapi/Dockerfile --target optimised-opt -t python-fastapi:optimised .
#   docker build -f comparison/python-fastapi/Dockerfile --target dataloader-optimised-opt -t python-fastapi:dataloader-optimised-opt .
#
# standard-doc is FastAPI's "FastAPI in Containers": the official Python
# image, the requirements installed with pip, the code copied, one process;
# it starts the server from its own code rather than by `fastapi run`
# (standard/main.py says why). optimised-opt installs the same requirements
# in a virtual environment in a first stage and copies it onto the same slim
# base, run as a user with no rights; no distroless image carries Python
# 3.14. The images carry a certificate for a local run, made when they are
# built and named by STUDY_TLS_CERT and STUDY_TLS_KEY (PKCS#8, which Granian
# requires); a campaign names its own. The bases are pinned by digest.
FROM alpine:3@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS cert
RUN apk add --no-cache openssl && mkdir /cert && \
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj /CN=study-contender -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
      -keyout /cert/key.pem -out /cert/cert.pem && chmod 644 /cert/key.pem /cert/cert.pem

FROM python:3.14-slim@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f AS standard-doc
WORKDIR /code
COPY comparison/python-fastapi/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY comparison/python-fastapi/ /code/
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
EXPOSE 8443
CMD ["python", "standard/main.py"]

FROM python:3.14-slim@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f AS build-opt
RUN python -m venv /venv
COPY comparison/python-fastapi/requirements.txt /requirements.txt
RUN /venv/bin/pip install --no-cache-dir -r /requirements.txt

FROM python:3.14-slim@sha256:9aaa474097d094040649f2162a627c0049b408f52138d980916788d80f04802f AS optimised-opt
RUN useradd --system --no-create-home --uid 10001 appuser
COPY --from=build-opt /venv /venv
WORKDIR /code
COPY comparison/python-fastapi/ /code/
COPY --from=cert /cert /cert
ENV PATH=/venv/bin:$PATH STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
USER appuser
EXPOSE 8443
CMD ["python", "optimised/main.py"]

# The optimised variant "with" grouping (aiodataloader): the optimised image,
# started on dataloader-optimised/main.py.
FROM optimised-opt AS dataloader-optimised-opt
CMD ["python", "dataloader-optimised/main.py"]
