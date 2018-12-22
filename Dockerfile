FROM golang:latest
ADD . /usr/local/go/src/github.com/fenimore/lazy
WORKDIR /usr/local/go/src/github.com/fenimore/lazy
RUN go build -o work worker/worker.go
CMD ["./work"]
