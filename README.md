# Dremio Flight Connector

[![Build Status](https://travis-ci.org/dremio-hub/dremio-flight-connector.svg?branch=master)](https://travis-ci.org/dremio-hub/dremio-flight-connector)

##Building and Installation

1. In root directory with the pom.xml file run mvn clean install
1. Take the resulting .jar file in the target folder and put it in the \dremio\jars folder in Dremio
1. Restart Dremio

## Accessing Dremio via flight

The Flight endpoint is exposed on port `47470`. The most recent release of pyarrow (`0.14.0`) has the flight client 
built in. To access Dremio via Flight first install pyarrow (`conda install pyarrow -c conda-forge` or `pip install pyarrow`). Then:

```python
from pyarrow import flight
class HttpDremioClientAuthHandler(flight.ClientAuthHandler): 
  
  def __init__(self, username, password): 
    super().__init__() 
    self.username = tobytes(username)
    self.password = tobytes(password) 
    self.token = None

  def authenticate(self, outgoing, incoming): 
    outgoing.write(base64.b64encode(self.username + b':' + self.password))
    self.token = incoming.read()

  def get_token(self): 
    return self.token
              
c = flight.FlightClient.connect('grpc+tcp://<dremio coordinator host>:47470')
c.authenticate(HttpDremioClientAuthHandler(b'dremio',b'dremio123')) 
...
```
