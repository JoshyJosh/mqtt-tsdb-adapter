FROM golang:latest AS builder

WORKDIR /taosclient

RUN apt update && apt install

RUN apt install -y cmake build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config wget

# RUN wget -qO - http://repos.taosdata.com/tdengine.key | apt-key add -

# RUN echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | tee /etc/apt/sources.list.d/tdengine-stable.list

# RUN apt update
# RUN apt-cache policy tdengine
# RUN apt install -y tdengine

RUN wget https://github.com/taosdata/TDengine/archive/refs/tags/ver-3.0.2.1.tar.gz

RUN tar -xf ver-3.0.2.1.tar.gz

WORKDIR /taosclient/TDengine-ver-3.0.2.1

RUN ./build.sh

WORKDIR /taosclient/TDengine-ver-3.0.2.1/debug

RUN make install

WORKDIR /app

COPY app /app

RUN go build -o tdengine-client 

FROM ubuntu:latest AS runner

RUN apt update && apt install

RUN apt install -y cmake build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config wget

COPY --from=builder /taosclient/TDengine-ver-3.0.2.1 /taosclient/TDengine-ver-3.0.2.1

WORKDIR /taosclient/TDengine-ver-3.0.2.1/debug

RUN make install

WORKDIR /app

COPY --from=builder /app/tdengine-client .

CMD ./tdengine-client

###

# RUN wget https://github.com/taosdata/TDengine/archive/refs/tags/ver-3.0.2.1.tar.gz

# RUN mkdir taos

# RUN tar -xf ver-3.0.2.1.tar.gz

# WORKDIR /taosclient/TDengine-ver-3.0.2.1

# RUN ./build.sh

# WORKDIR /taosclient/TDengine-ver-3.0.2.1/debug

# RUN make install

######

# FROM golang:latest AS runner

# WORKDIR /taosclient

# COPY --from=build /taosclient/TDengine-ver-3.0.2.1 ./

# WORKDIR /taosclient/debug

# RUN apt update

# RUN apt install -y cmake libjansson-dev libsnappy-dev liblzma-dev libz-dev

# RUN ls ..
# 
# RUN make install
# 
# RUN echo "waiting"
# 
# CMD tail -f /dev/null 