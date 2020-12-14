FROM golang:alpine
RUN apk add make git build-base --no-cache
COPY . /source
WORKDIR /source
RUN make build


FROM alpine:3.12
COPY --from=builder /source/bin/backup /usr/local/bin/backup
