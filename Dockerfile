FROM python:alpine

WORKDIR /app
ENV PYTHONPATH /app
COPY . .
RUN pip install -r requirements.txt

CMD ["python","app.py"]