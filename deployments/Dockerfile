FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src
COPY go.work go.work
COPY core/ core/
COPY proto/ proto/
COPY sdk/go/ sdk/go/
COPY server/ server/
ENV GOPROXY=https://goproxy.cn,direct
RUN cd server && go build -o /janus-api ./cmd/janus-api

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /janus-api /usr/local/bin/janus-api
COPY migrations/ /migrations/
EXPOSE 8080
ENV JANUS_MIGRATION_PATH=/migrations/
ENTRYPOINT ["janus-api"]
