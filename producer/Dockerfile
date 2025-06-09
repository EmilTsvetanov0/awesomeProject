FROM gocv/opencv:latest

WORKDIR ${GOPATH}/awesomeProject/
COPY . ${GOPATH}/awesomeProject/

RUN go build -o /build ./cmd \
    && go clean -cache -modcache

CMD ["/build"]