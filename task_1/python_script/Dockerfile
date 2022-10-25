FROM python:3.10-alpine3.16 as base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1


ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1\
    PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.2.2

RUN apk add --no-cache gcc libffi-dev musl-dev 
#postgresql-dev
RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /python
ADD . .
RUN poetry install
CMD ["poetry", "run","python3", "main.py"]