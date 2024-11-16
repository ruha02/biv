docker build . -t bvi
docker run --rm -it -v %cd%:/app/ bvi