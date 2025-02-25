FROM python:3.12-slim

WORKDIR /code

COPY ./requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    rm /code/requirements.txt

COPY ./src ./src

# uvicorn is the part that handles web requests for fast api
# short for "unicorn serving ASGI"
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "80", "--reload"]