FROM airdock/python-poetry:latest
WORKDIR /python
ADD . .
RUN poetry install
CMD ["poetry", "run","python3", "main.py"]