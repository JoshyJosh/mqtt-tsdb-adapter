FROM golang:latest AS builder

WORKDIR /taosclient

RUN apt update && apt install

RUN apt install -y cmake build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config wget

RUN wget https://www.taosdata.com/assets-download/3.0/TDengine-client-3.0.2.4-Linux-x64.tar.gz

RUN tar -xf TDengine-client-3.0.2.4-Linux-x64.tar.gz

WORKDIR /taosclient/TDengine-client-3.0.2.4

RUN ./install_client.sh

WORKDIR /app

COPY app /app

RUN go build -o tdengine-client 

FROM ubuntu:latest AS runner

RUN apt update && apt install

RUN apt install -y cmake build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config wget

COPY --from=builder /taosclient/TDengine-client-3.0.2.4 /taosclient/TDengine-client-3.0.2.4

WORKDIR /taosclient/TDengine-client-3.0.2.4

RUN ./install_client.sh

WORKDIR /app

COPY --from=builder /app/tdengine-client .

CMD ./tdengine-client