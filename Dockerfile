FROM golang:1.24-alpine AS builder
WORKDIR /workspace

ENV GOTOOLCHAIN=local
ARG APP_VERSION=v0.0.0-dev

COPY ./ ./
RUN go build -ldflags="-s -w -X main.version=${APP_VERSION}" -o goondvr .

FROM alpine:3 AS runnable
RUN apk --no-cache add ca-certificates ffmpeg
WORKDIR /usr/src/app

COPY --from=builder /workspace/goondvr /goondvr

ENTRYPOINT ["/goondvr"]
