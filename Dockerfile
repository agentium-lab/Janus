FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

ENV GOPROXY=https://goproxy.cn,direct
ENV CGO_ENABLED=0

WORKDIR /src

COPY go.work ./
COPY go.work.sum* ./
COPY core/ core/
COPY proto/ proto/
COPY server/ server/
COPY cli/ cli/
COPY sdk/ sdk/
COPY demo/ demo/
COPY migrations/ migrations/

WORKDIR /src/server
RUN go build -ldflags="-s -w" -o /janus-api ./cmd/janus-api/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /janus-api /usr/local/bin/janus-api
COPY --from=builder /src/migrations /migrations

EXPOSE 8080 9090

ENTRYPOINT ["janus-api"]
