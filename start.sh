docker build . -t bvi
docker run --rm -it -v $(pwd):/app/ bvi