from mozilla/sbt

WORKDIR /app

COPY . ./

ENTRYPOINT ["ls"]
