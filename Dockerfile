FROM python:3.7

RUN mkdir -p /user/app
WORKDIR /usr/app

ADD requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ADD src .
ADD .env .

CMD [ "python", "-m", "src"]