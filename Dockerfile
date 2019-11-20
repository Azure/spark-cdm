from mozilla/sbt

WORKDIR /app

COPY . ./

ENTRYPOINT ["./build.sh"]
