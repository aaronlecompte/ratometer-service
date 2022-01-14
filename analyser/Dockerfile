FROM python:3.9-slim-bullseye

RUN pip install requests pandas boto3 pyathena python-dotenv

WORKDIR /app
COPY . .

CMD ["python", "analyser.py"]