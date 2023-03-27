FROM registry.twilio.com/library/base-python-38:latest

ENV DOCKERIZE_VERSION v0.6.1
WORKDIR /opt
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /opt -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz
ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN pip install -i https://pypi.dev.twilio.com/simple poetry==1.1.13 && poetry config --local virtualenvs.in-project true
COPY poetry.lock pyproject.toml /opt/
RUN poetry install -n --no-dev
COPY libs /opt/libs
COPY src /opt/src
COPY tests /opt/tests
COPY main /opt/main
COPY log.conf /opt/
COPY remote_logging /opt/remote_logging
COPY run.sh run.sh
COPY config /opt/config
ENV PYTHONPATH "${PYTHONPATH}:/opt/"
CMD ["/opt/run.sh"]
