# Dremio Flight Connector

[![Build Status](https://travis-ci.org/dremio-hub/dremio-flight-connector.svg?branch=master)](https://travis-ci.org/dremio-hub/dremio-flight-connector)

## Building and Installation

1. In root directory with the pom.xml file run mvn clean install
1. Take the resulting .jar file in the target folder and put it in the \dremio\jars folder in Dremio
1. Restart Dremio

## Accessing Dremio via flight in python

The Flight endpoint is exposed on port `47470`. The most recent release of pyarrow (`0.14.1`) has the flight client 
built in. To access Dremio via Flight first install pyarrow (`conda install pyarrow -c conda-forge` or `pip install pyarrow`). You must also generate the python class for the Command proto `protoc -I=dremio-flight-connector/command/src/main/protobuf --python_out=. dremio-flight-connector/command/src/main/protobuf/command.proto`. Then:

```python
from pyarrow import flight
import pyarrow as pa
from .command_pb2 import Command

class HttpDremioClientAuthHandler(flight.ClientAuthHandler): 
  
  def __init__(self, username, password): 
    super().__init__() 
    self.username = username
    self.password = password
    self.token = None

  def authenticate(self, outgoing, incoming): 
    outgoing.write(base64.b64encode(self.username + b':' + self.password))
    self.token = incoming.read()

  def get_token(self): 
    return self.token

username = b'<USERNAME>'
password = b'<PASSWORD>'
sql = '''<SQL_QUERY>'''

client = flight.FlightClient.connect('grpc+tcp://<DREMIO_COORDINATOR_HOST>:47470')
client.authenticate(HttpDremioClientAuthHandler(username, password)) 
cmd = Command(query=sql, parallel=False, coalesce=False, ticket=b'')
info = client.get_flight_info(flight.FlightDescriptor.for_command(cmd.SerializeToString()))
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
