FROM python:alpine
WORKDIR app
COPY . .
RUN pip3 install -r requirements.txt
CMD ["gunicorn", "src.index:app"]
