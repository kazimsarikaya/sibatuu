FROM golang:alpine as builder
RUN apk add make git build-base protoc --no-cache \
    && go get google.golang.org/protobuf/cmd/protoc-gen-go \
    && wget https://github.com/protocolbuffers/protobuf/releases/download/v3.14.0/protoc-3.14.0-linux-x86_64.zip \
    && unzip protoc-3.14.0-linux-x86_64.zip && rm protoc-3.14.0-linux-x86_64.zip && rm bin/protoc && mv /usr/bin/protoc bin/protoc

COPY . /source
WORKDIR /source
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
ENV LDFLAGS="-w -s"
RUN make build

FROM scratch
COPY --from=builder /source/bin/backup /bin/backup
ENTRYPOINT ["/bin/backup"]
