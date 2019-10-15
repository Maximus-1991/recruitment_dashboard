FROM python:3.7

RUN mkdir -p /usr/app
WORKDIR /usr/app

ADD requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ADD src ./src
ADD .env .

CMD ["python", "-m", "src"]