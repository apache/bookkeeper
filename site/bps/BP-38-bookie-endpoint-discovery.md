---
title: "BP-38: Bookie endpoint discovery"
issue: https://github.com/apache/bookkeeper/<issue-number>
state: 'Under Discussion', 'Accepted', 'Adopted', 'Rejected'
release: "4.11.0"
---

### Motivation

Bookie server exposes several services, but up to 4.10 you can only discover the main Bookie endpoint,
the main BookKeeper binary service.
This discovery is also implicit in the fact the the id of the bookie is made of the same host:port that
point to the TPC address of the Bookie service.

We are now introducing a way for the Bookie to advertise the services it exposes, basically with this change the Bookie wil be able
to store on the Metadata service a set of name value pairs that describe *available services*.


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

You can derive bookieId from a BookieSocketAddress, in the future it would be possible
to store bookie ids in Ledger Metadata but this change should be covered in the scope
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
that is updated whenever the Bookie becomes available (like after a restart).


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

_Describe in few sentences how the BP will be tested. We are mostly interested in system tests (since unit-tests are specific to implementation details). How will we know that the implementation works as expected? How will we know nothing broke?_

### Rejected Alternatives

_If there are alternative ways of accomplishing the same thing, what were they? The purpose of this section is to motivate why the design is the way it is and not some other way._
