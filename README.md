# Squillion MQTT Server

MQTT multi-tenant server supporting MQTT 3.1, websockets, and TLS.

## Building

Building requires [Rust and cargo](https://www.rust-lang.org/) to be installed first.

**To build:**

```
$ cargo build
```

***Running tests:***

*Test must be run single threaded as different test use different configurations.*

```
$ cargo test --verbose -- --test-threads=1
```

***Running and loading config:***

```
$ cargo run config.yaml
```

## Docker

**To build the [Docker](https://www.docker.com/) image:**

```
docker build . -t squillion-mqtt
```

***Running the Docker image:***

*You must update the ports and config file mapping to match your deployment.*

```
docker run -t -v ./config.yaml:/opt/mqtt/etc/config.yaml -p 1883:1883 -p 8083:8083 squillion-mqtt
```

## Configuration

```
# squillion-mqtt config

# Set a remote logging endpoint
# If true logs are sent over to
# the logging endpoint.
logging_remote: false
#logging_endpoint: "fluentd.fluentd:24224"

# Set ports and protocols to listen on
# Can be a combination of websocket and TLS
listeners:
- port: 1883
  websocket: false
- port: 8083
  websocket: true
#- port: 8883
#  websocket: false
#  tlscrt: ./tests/tls/tls.crt
#  tlskey: ./tests/tls/tls.key
#- port: 8084
#  websocket: true
#  tlscrt: ./tests/tls/tls.crt
#  tlskey: ./tests/tls/tls.key

# Authentication method
# pwdlist uses users list from this file
auth_method: "pwdlist"

users:
- username: user1
  password: password1
- username: user2
  password: password2

# Alternative authentication method
# See sample file for format
#password_file: ./tests/users.txt

# Method and path to save data such
# as topics, subscriptions, and persistent messages.
# Only 'sqlite' is available.
persist_method: "sqlite"
persist_data_store: './data'
```