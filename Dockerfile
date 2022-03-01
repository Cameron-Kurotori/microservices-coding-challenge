FROM golang:1.17.7-alpine3.15

WORKDIR /usr/src/distqueue

COPY go.mod go.sum .
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o /usr/local/bin/distqueue