from mozilla/sbt

RUN ["ls"]
WORKDIR /app

RUN ["ls"]
COPY . ./

ENTRYPOINT ["./build.sh"]
