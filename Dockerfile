FROM python:3.7.5-stretch as requirements

ARG PIP_EXTRA_INDEX_URL

# installing dependencies ONLY
COPY setup.py ./
RUN \
    pip install --user -e . && \
    pip uninstall -y platform-disk-api


FROM python:3.7.5-stretch AS service

WORKDIR /neuromation

COPY setup.py ./
COPY --from=requirements /root/.local /root/.local

# installing platform-disk-api
COPY platform_disk_api platform_disk_api
RUN pip install --user -e .

ENV PATH=/root/.local/bin:$PATH

ENV NP_DISK_API_PORT=8080
EXPOSE $NP_DISK_API_PORT

CMD platform-disk-api
