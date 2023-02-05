FROM golang:latest AS builder

WORKDIR /taosclient

RUN apt update && apt install

RUN apt install -y cmake build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config wget iputils-ping

RUN wget https://github.com/taosdata/TDengine/archive/refs/tags/ver-3.0.2.4.tar.gz

RUN tar -xf ver-3.0.2.4.tar.gz

WORKDIR /taosclient/TDengine-ver-3.0.2.4

RUN ./build.sh

WORKDIR /taosclient/TDengine-ver-3.0.2.4/debug

RUN make install

WORKDIR /app

COPY app /app

CMD go run cmd/main.go

# RUN go build -o tdengine-client ./cmd  && chmod +x tdengine-client

# FROM ubuntu:latest AS runner

# RUN apt update && apt install

# RUN apt install -y cmake build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config wget

# COPY --from=builder /taosclient/TDengine-ver-3.0.2.4 /taosclient/TDengine-ver-3.0.2.4

# WORKDIR /taosclient/TDengine-ver-3.0.2.4/debug

# RUN make install

# WORKDIR /app

# COPY --from=builder /app/tdengine-client .

# CMD ./tdengine-client