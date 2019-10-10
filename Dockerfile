FROM python:3.7-alpine

RUN mkdir -p /user/src/app
WORKDIR /usr/src/app

ADD requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ADD src .
ADD .env .

CMD [ "python", "-m", "src"]
