---
title: "BP-38: Bookie endpoint discovery"
issue: https://github.com/apache/bookkeeper/<issue-number>
state: 'Under Discussion', 'Accepted', 'Adopted', 'Rejected'
release: "4.11.0"
---

### Motivation

Bookie server exposes several services and some of them are optional: the binary BookKeeper protocol endpoint, the HTTP service, the StreamStorage service, a metrics endpoint.
Currently (in BookKeeper 4.10) the client can only discover the main Bookie endpoint,
the main BookKeeper binary service.
Discovery is implicit by the fact the the *id* of the bookie is made of the same host:port that point to the TCP address of the Bookie service.

With this proposal we are now introducing a way for the Bookie to advertise the services it exposes, basically with this change the Bookie will be able to store on the Metadata Service a set of name-value pairs that describe the *available services*.

We will also define a set of well know properties that will be useful for futher implementations.

This information will be also useful for Monitoring and Management services as it will enable full discovery of the capabilities of all of the Bookies just by having access to the Metadata Service.


### Public Interfaces

On the client side, we introduce a new data structure that describes the services
exposed by a Bookie:

```
inteface BookieServiceInfo {
    Iterable<String> keys();
    String get(String key, String defaultValue);
}

```

In RegistrationClient interface we expose a new method:

```
BookieServiceInfo getBookieServiceInfo(String bookieId)
```

You can derive bookieId from a BookieSocketAddress.
Currently we are storing BookieSocketAddresses in ledgers metadata, but in the future it would be possible to store just bookie ids but this change should be covered in the scope
of another BP.

On the Bookie side we change the RegistrationManager interface in order to let the Bookie
advertise the services:

in RegistrationManager class this method 
```
void registerBookie(String bookieId, boolean readOnly)
```

becomes

```
void registerBookie(String bookieId, BookieServiceInfo bookieServieInfo, boolean readOnly)
```

It will be up to the implementation of RegistrationManager and RegistrationClient to serialize
the BookieServiceInfo structure.

For the ZooKeeper based implementation we are going to store such information as a JSON Object

```
{
    "property": "value",
    "property2": "value2",
}
```
Such information will be stored inside the '/available' znode (or 'available_readonline' in case of readonly bookie).
The rationale around this choice is that the client is watching over these znodes, in the future the client will read the endpoint information (BookieSocketAddress) from this node
that is updated whenever the Bookie becomes available.
This about the case when you want to change the network address of the bookie: you reboot the Bookie machine with the new configuration and clients will connect to the new address.
This kind of change is out of the scope of the current proposal.


Notable known keys will be:
- bookie.binary.url: bk://hostname:port - address of the main Bookie RPC interface
- bookie.binary.tls.supported: true|false - information about the availability of TLS
- bookie.binary.auth.type: node|sasl - information about available authentication mechanism
- bookie.admin.url: protocol://hostname:port - information about the HTTP endpoint
- bookie.metrics.url:  protocol://hostname:port/metrics - information about Metrics endpoint

Which kind of properties should be stored in BookieServiceInfo ?

We should store here only the minimal set of information useful to reach the bookie.
For instance internal state of the Bookie should be exposed by the HTTP endpoint
so having the *bookie.admin.url* is enough.
Having information about authentication or security will be useful for PlacementPolicies or for readers in order to select bookies to use.
It is expected that these properties change when a bookie is restarted, like after a reconfiguration (change auth type, enable TLS).
It is better not to expose the version of the Bookie in order not to open the way to protocol usage based on the Bookie version (it is better to implement 'feature detection' instead of using the version).

### Proposed Changes

See the 'Public intefaces' section.

### Compatibility, Deprecation, and Migration Plan

The proposed change will be compatible with existing clients.

It is possible that future client will need these new properties exposed by the Bookie
- What impact (if any) will there be on existing users? 
- If we are changing behavior how will we phase out the older behavior? 
- If we need special migration tools, describe them here.
- When will we remove the existing behavior?

### Test Plan

New unit tests will be added to cover the code change. 
No need for additional integration tests.


### Rejected Alternatives

#### Adding a new set of z-nodes
For the ZooKeeper implementation we are not introducing a new znode to store BookieServiceInfo. Adding such new node will increase complexity and the usage of resources on ZooKeeper.

#### Storing information inside the Cookie
The cookie stores information about the identity of the bookie and it is expected not to change. It is exceptional to rewrite the Cookie, like when adding a new data directory. 