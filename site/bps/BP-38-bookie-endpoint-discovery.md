---
title: "BP-38: Publish Bookie Service Info on Metadata Service"
issue: https://github.com/apache/bookkeeper/2215
state: 'Accepted'
release: "4.11.0"
---

### Motivation

Bookie server exposes several services and some of them are optional: the binary BookKeeper protocol endpoint, the HTTP service, the StreamStorage service, a Metrics endpoint.

Currently (in BookKeeper 4.10) the client can only discover the main Bookie endpoint:
the main BookKeeper binary RPC service.
Discovery of the TCP address is implicit, because the *id* of the bookie is made of the same host:port that point to the TCP address of the Bookie service.

With this proposal we are now introducing a way for the Bookie to advertise the services it exposes, basically the Bookie will be able to store on the Metadata Service a structure that describes the list of  *available services*.

We will also allow to publish a set of additional unstructured properties in form of a key-value pair that will be useful for futher implementations.

This information will be also useful for Monitoring and Management services as it will enable full discovery of the capabilities of all of the Bookies in a cluster just by having read access to the Metadata Service.

### Public Interfaces

On the Registration API, we introduce a new data structure that describes the services
exposed by a Bookie:

```
interface BookieServiceInfo {
    class Endpoint {
        String name;
        String hostname;
        int port;
        String protocol; // "bookie-rpc", "http", "https"....      
    }
    List<Endpoint> endpoints;
    Map<String, String> properties; // additional properties
}

```

In RegistrationClient interface we expose a new method:

```
CompletableFuture<Versioned<BookieServiceInfo>> getBookieServiceInfo(String bookieId)
```

The client can derive bookieId from a BookieSocketAddress. He can access the list of available bookies using **RegistrationClient#getAllBookies()** and then use this new method to get the details of the services exposed by the Bookie.

On the Bookie side we change the RegistrationManager interface in order to let the Bookie
advertise the services:

in RegistrationManager class the **registerBookie** method 
```
void registerBookie(String bookieId, boolean readOnly)
```

becomes

```
void registerBookie(String bookieId, boolean readOnly, BookieServiceInfo bookieServieInfo)
```

It will be up to the implementation of RegistrationManager and RegistrationClient to serialize
the BookieServiceInfo structure.

For the ZooKeeper based implementation we are going to store such information in Protobuf binary format, as currently this is the format used for every other kind of metadata (the example here is in JSON-like for readability purposes):

```
{
    "endpoints": [
        {
         "name": "bookie",
         "hostname": "localhost",
         "port": 3181,
         "protocol": "bookie-rpc"
        },
        {
         "name": "bookie-http-server",
         "hostname": "localhost",
         "port": 8080,
         "protocol": "http"
        }
    ],
    properties {
        "property": "value",
        "property2": "value2"
    }
}
```
Such information will be stored inside the '/REGISTRATIONPATH/available' znode (or /REGISTRATIONPATH/available/readonly' in case of readonly bookie), these paths are the same used in 4.10, but in 4.10 we are writing empty znodes.

The rationale around this choice is that the client is already using these znodes in order to discover available bookies services.

It is possible that such endpoint information change during the lifetime of a Bookie, like after rebooting a machine and the machine gets a new network address.

It is out of the scope of this proposal to change semantics of ledger metadata, in which we  are currently storing directly the network address of the bookies, but with this proposal we are preparing the way to have an indirection and separate the concept of Bookie ID from the Network address.

**Endpoint structure**
- **name**: this is an id for the service type, like 'bookie'
- **hostname**: the hostname (or a raw IP address)
- **port**: the TCP port (int)
- **protocol**: the type of service (bookie-rpc, http, https)

**Well known additional properties**
- **autorecovery.enabled**: 'true' case of the presence of the auto recovery daemon

The GRPC service would use this mechanism as well.

#### Which kind of properties should be stored in BookieServiceInfo ?

We should store here only the minimal set of information useful to reach the bookie in an efficient way.
So we are not storing on the Metadata Service information about the internal state of the server: if you know the address of an HTTP endpoint you can use the REST API to query the Bookie for its state.

These properties may change during the lifetime of a Bookie, think about a configuration (change network address) or a dynamic assigned DNS name.

It is better not to expose the version of the Bookie, if the client wants to use particular features of the Bookie this should be implemented on the protocol itself, not just by using the version. The version of the Bookie could be exposed on the HTTP endpoint.

### Proposed Changes

See the 'Public interfaces' section.

### Compatibility, Deprecation, and Migration Plan

The proposed change will be compatible with all existing clients.

In case a new client is reading an empty znode it will assume a default configuration with a single 'bookie-rpc' endpoint, like in this example:

{
    "endpoints": [
        {
         "name": "bookie",
         "hostname": "hostname-from-bookieid",
         "port": port-from-bookieid,
         "protocol": "bookie-rpc"
        }
    ]
}

This information is enough in order to use the RPC service.

### Test Plan

New unit tests will be added to cover all of the code changes.
No need for additional integration tests.

### Rejected Alternatives

#### Adding a new set of znodes
For the ZooKeeper implementation we are not introducing a new znode to store BookieServiceInfo. Adding such new node will increase complexity and the usage of resources on ZooKeeper.

#### Storing information inside the Cookie
The *Cookie* stores information about the *identity* of the bookie and it is expected not to change.
It is exceptional to rewrite the Cookie, like when adding a new data directory.
In some environments it is common to have a new network address after a restart or to change the configuration and enable a new service or feature, and you cannot rewrite the Cookie at each restart: by design every change to the Cookie should be manual or controlled by an external entity other than the Bookie itself.
