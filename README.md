# tdengine-golang-adapter

This is an adapter made to connect a mosquitto mqtt broker to a tdengine instance. This is a repo that is used to test a kubernetes cluster. 

This is not meant for any production use and was mainly a proof of concept.

The Dockerfile is used in order to create an instance that would not require the user to install the tdengine package, if you see a `fatal error: taos.h: No such file or directory`, then the taosd is not installed locally.

Ideally this would be redone with a different stack:

- mosquitto would be replaced with rabbitmq
- tdengine would be replaced with graphite
