## *🕸️ MqttDistribNet* - A structured distribution network for amqp messaging

The MqttDistribNet module consist of two concepts, to ease the usage of amqp brokers:
- **Dynamic Infrastructure Management**<br>
  To simplify the configuration of topics and queues and binding them in a logical path, 
  we introduced an <code>IDistributionNetworkManager</code> and <code>IRemoteDistributionNetworkManager</code>. 
  While the <code>IDistributionNetworkManager</code> is the infrastructure management server, the <code>IRemoteDistributionNetworkManager</code> 
  is used by the client to get information about the remote managed infrastructure.
- **Simple Message Distribution**<br>
  Based on Request/Response and Publish/Subscribe Patterns we created <code>Handler</code>, <code>Consumer</code> and <code>Clients</code> to easily 
  configure the wanted behaviour behind queues and reach them by their configured message type.

### 1. Usage:
```go
import (
    mqttdistribnet "github.com/tjarkpr/mqttdistribnet/pkg"
    amqp "github.com/rabbitmq/amqp091-go"
)
```
#### 1.1. DistributionNetworkManager
```go
connection, err := amqp.Dial(url)
manager, err    := mqttdistribnet.MakeIDistributionNetworkManager(connection, "<prefix>")
logs, err       := manager.Start(&ctx)
```
#### 1.2. Distribution & Consumption
```go
connection, err := amqp.Dial(url)
remote, err     := mqttdistribnet.MakeIRemoteDistributionNetworkManager(connection, "<prefix>")
```
```go
remote.Register(reflect.TypeFor[mqttdistribnet.Envelope[TestRequest]](), "L1.L2.L3.L4.*")
```
```go
err = mqttdistribnet.MakeRequestHandler[mqttdistribnet.Envelope[TestRequest], mqttdistribnet.Envelope[TestResponse]](
    remote,
    func(request mqttdistribnet.Envelope[TestRequest]) (mqttdistribnet.Envelope[TestResponse], error) {
        t.Log("Request: " + request.ToString())
        return mqttdistribnet.Envelope[TestResponse]{
            MessageId: uuid.New(),
            Timestamp: time.Now(),
            Payload:   TestResponse{TestResProperty: "Test"},
    }, nil
}, &ctx)
```
```go
request         := &mqttdistribnet.Envelope[TestRequest]{
    MessageId: uuid.New(),
    Timestamp: time.Now(),
    Payload:   TestRequest{TestReqProperty: "Test"},
}
clients, err    := mqttdistribnet.MakeRequestClient[mqttdistribnet.Envelope[TestRequest], mqttdistribnet.Envelope[TestResponse]](remote)
response, err   := slices.Collect(maps.Values(clients))[0].Send(request)
```