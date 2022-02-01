# Microservices Coding Challenge

### Service to Service messaging via gRPC
Create a gRPC client and server that uses a bidirectional channel allowing push and consumption of messages from either side. The gRPC server should not be able to connect directly to the client, yet it should be able to push messages down once the client connects. You can be creative about the types of messages sent and the reason(s) for the server and client to communicate.

Make sure to consider:
- network blips/issues -- what happens if there are issues sending messages due to network issues?
- code style/organization
- performance/scalability


### Technology
Our preferred technology stack is:
- golang
- postgresql (if necessary, for persistence)
Bonus points for:
- docker (or any containers)
- helm/kubernetes

### Helpful links
- [gRPC](https://grpc.io/)
- [A Tour of Go](https://go.dev/tour/welcome/1)
