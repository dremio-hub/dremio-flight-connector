# Dremio Flight Connector
```diff
- This repository is now retired. Fligth is a first class client connection in Dremio
```
see https://docs.dremio.com/release-notes/49-release-notes.html#preview-arrow-flight for details

[![Build Status](https://travis-ci.org/dremio-hub/dremio-flight-connector.svg?branch=master)](https://travis-ci.org/dremio-hub/dremio-flight-connector)

## Building and Installation

1. In root directory with the pom.xml file run mvn clean install
1. Take the resulting shaded jar `dremio-flight-connector-{VERSION}.jar` file in the target folder and put it in the \dremio\jars folder in Dremio
1. Restart Dremio

## Configuration

### Enable Flight

* the property `-Ddremio.flight.enabled=true` *MUST* be set to enable flight
* the property `-Ddremio.flight.parallel.enabled=true` *MUST* be set on all executors to enable parallel flight

### Parallel Flight
The parallel flight stream is now working in Dremio. However this requires a patched dremio-oss to work correctly. This allows executors to stream
results directly to the python/spark connector in parallel. See: [Spark Connector](https://github.com/rymurr/flight-spark-source). This results in an
approximate linear performance increase over serial Flight (with properly configured parallelization)


### SSL
* ensure you have ssl set up in your `dremio.conf`. This plugin currently uses the same certificates as the webserver.
* set property `-Ddremio.flight.use-ssl=true`

### Hostname & port

* set property `-Ddremio.flight.port=47470` to change the port. Defaults to `47470`
* set property `-Ddremio.flight.host=localhost` to change the host/listening interface. Particularly useful if you are 
accessing remotely or generating ssl certs 

## Accessing Dremio via flight in python

The Flight endpoint is exposed on port `47470`. The most recent release of pyarrow (`0.15.0`) has the flight client 
built in. To access Dremio via Flight first install pyarrow (`conda install pyarrow -c conda-forge` or `pip install pyarrow`). Then 
use the [dremio-client](https://github.com/rymurr/dremio_client) to access flight. Alternatively use:


```python
from pyarrow import flight
import pyarrow as pa

class HttpDremioClientAuthHandler(flight.ClientAuthHandler):

    def __init__(self, username, password):
        super(flight.ClientAuthHandler, self).__init__()
        self.basic_auth = flight.BasicAuth(username, password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self):
        return self.token

username = '<USERNAME>'
password = '<PASSWORD>'
sql = '''<SQL_QUERY>'''

client = flight.FlightClient('grpc+tcp://<DREMIO_COORDINATOR_HOST>:47470')
client.authenticate(HttpDremioClientAuthHandler(username, password)) 
info = client.get_flight_info(flight.FlightDescriptor.for_command(sql))
reader = client.do_get(info.endpoints[0].ticket)
batches = []
while True:
    try:
        batch, metadata = reader.read_chunk()
        batches.append(batch)
    except StopIteration:
        break
data = pa.Table.from_batches(batches)
df = data.to_pandas()
```

## Notes:

* the `Command` protobuf object has been removed to reduce dependencies
