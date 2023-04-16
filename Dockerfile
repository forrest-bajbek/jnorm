FROM python:3.10.11-slim-buster

WORKDIR /usr/src/app
COPY . .
RUN pip install .

VOLUME /data

ENTRYPOINT ["jnorm"]