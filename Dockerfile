FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root

ARG HOST_UID=1000
ARG HOST_GID=1000

COPY requirements.lock.txt /tmp/requirements.lock.txt

RUN python3 -m pip install \
    --no-cache-dir \
    -r /tmp/requirements.lock.txt


RUN groupadd --gid ${HOST_GID} lab \
    && useradd \
        --uid ${HOST_UID} \
        --gid ${HOST_GID} \
        --create-home \
        --shell /bin/bash \
        lab

RUN mkdir -p /workspace \
    && chown -R lab:lab /workspace


ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"

ENV PATH="/opt/spark/bin:${PATH}"


ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /workspace

USER lab

EXPOSE 8888
EXPOSE 4040

CMD ["python3", "-m", "jupyterlab", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
