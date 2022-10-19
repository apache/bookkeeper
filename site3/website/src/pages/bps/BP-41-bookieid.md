# BP-41: Separate BookieId from Bookie Network Address

### Motivation

We want to separate the concepts of **BookieId** from **BookieSocketAddress**.

Currently (up to 4.11.x) there is a too strict coupling from the ID of a Bookie (**BookieId**) and its network location (**BookieSocketAddress**).

The **LedgerMetadata** structure contains the location of the entries of a ledger, and it stores BookieSocketAddresses (simply a hostname:port or ip:port pair).
The client uses this information to connect to the bookies and retrieve ledger data.

So *the identity of a bookie is bound to the network address* of the primary endpoint of the Bookie: the '**bookie-rpc**' endpoint in terms of [BP-38](BP-38-bookie-endpoint-discovery/)

Limits of current version, because:

- You cannot easily change the network address of a Bookie: a manual intervention is needed.
- The Bookie cannot have a **dynamic network address** or DNS name.
- You cannot use a custom Id for the bookie, something that is **meaningful** in the context of the deployment of the bookie.
- In case of future implementations that will open **multiple endpoints** on the bookie it is not possible to decide which endpoint should be used as BookieId. 

This proposal addresses these problems by proposing to separate the concept of **BookieId** from **BookieSocketAddress**.

We will have to introduce a little break in the Client API, in order to switch from using BookieSocketAddress to a more opaque BookieId.

Fortunately we will be able to keep compatibility with old clients and old bookies are far as the Bookie continues to use a BookieId that looks like a BookieSocketAddress.
See the paragraphs below for the details. 

### Public Interfaces

We are introducing a new class BookieId that is simply a wrapper for a String.

```
final class BookieId {
    private final String bookieId;
    public String toString() {
         return bookieId;
    }
    public static BookieId parse(String bookieId) {
         // validation omitted...
         return new BookieId(bookieId);
    }
}
```

Having a class instead of a simple String is better because it provides a strongly typed API.

The LedgerMetadata class will be changed to use BookieId instead of BookieSocketAddress.
This will break source and binary compatibility for Java clients, applications that use LedgerMetadata (usually for debug or for tools) will have to be recompiled.

The serialized representation of a BookieSocketAddress, both for LedgerMetadata and Bookie Registration, is a simple String on ZooKeeper: this change is not about the format of data stored on Metadata Service.

It is simply a pure refactor of Java interfaces.

We have to introduce an internal API, **BookieAddressResolver**, that maps a *BookieId* to a *BookieSocketAddress*: the client connectes to a Bookie it looks up the **current network address** using BookieAddressResolver.

```
interface BookieAddressResolver {
    BookieSocketAddress resolve(BookieId id);
}
```

Initially it is not expected that the user provides a custom implementation of BookieAddressResolver.


It is expected that the implementation of this interface coordinates with the BookieWatcher and the RegistrationDriver in order to:
- map BookieId to BookieSocketAddress using `getBookieServiceInfo(BookieId id)` API
- cache efficiently this mapping in order to do not have a significant impact on the hot paths (reads and writes), and to save resources on the Metadata Service

We provide an utility method BookieSocketAddress#toBookieId that helps particularly in test cases, this method simply returns a BookieId
built by the serialized representation of the BookieSocketAddress (hostname:port or ip:port)

```
final class BookieSocketAddress {
    ....
    BookieId toBookieId() {
        return BookieId.parse(this.toString());
    }
}
```

The RegistrationClient and RegistrationManager interfaces will be refactored to use BookiId instead of String and BookieSocketAddress.

The Bookie itself will expose an API to return the current BookieSocketAddress and current BookieId, this is useful for tests and for the RegistrationManager.

The EnsemblePlacementPolicy and the BookieWatcher will deal with BookieIds and not with BookieSocketAddresses.

The implementations of EnsemblePlacementPolicy that are aware of the network location of the Bookies will need to have access to the 
BookieAddressResolver, in order to map a BookieId to the BookieSocketAddress and the BookieSocketAddress to the network location.


### Details on the proposed Changes

#### BookieId validation

The BookieId is a non empty string that can contain:
- ASCII digits and letters ([a-zA-Z9-0])
- the colon character (':')
- the dash character ('-')
- the dot character ('.') 

#### BookKeeper Client Side Changes

See the 'Public interfaces' section.

On the client side code it will be clearer when we are dealing with BookieId, and basically the client API won't deal with network addresses anymore.
This change will be visible both on the legacy LedgerHandle API and on the new WriteHandle/ReadHandle API, basically because the new API is only a wrapper over the LedgerHandle API.

When the BookKeeper client connects to a bookie (see **PerChannelBookieClient**) it uses the BookieAddressResolver interface to get the current BookieSocketAddress of the Bookie.
The key of the Connection Pool inside the BookieClient will be BookieId and no more BookieSocketAddress. 

#### Disabling BookieAddressResolver

Using the BookieServiceInfo abstraction needs additional accesses to the Metadata Service (usually ZooKeeper) and this comes with a cost especially during the bootstrap of the client, because you have to resolve the address of every Bookie you are going to write to or to read from.

We add a flag to disable the BookieAddressResolver, without this feature the client will be able only to connect to Bookies with the legacy BookieId.
In this case the BookieAddressResolver implementation will be a simple conversion from the BookieId, assuming the 4.11 format hostname:port.

```
enableBookieAddressResolver=true
```
The *enableBookieAddressResolver* flag is used by the Client, by the Auditor and by all of the tools and it is enabled by default. 

#### Handling the Local Bookie Node in EnsemblePlacementPolicy
Unfortunately thru the codebase we used sometimes dummy BookieId that are not mapped to real Bookies, this happens in the EnsamblePlacementPolicies in which we create a BookieId for the 'local node' and using TCP port 0. In this case we have to implement a fallback dummy resolution that created a BookieSocketAddress without using the MetadataService

#### Bookie Side Changes

On the Bookie we introduce **a configuration option** (bookieid) to set a custom bookie id. 
If you set this option then the Bookie will advertise itself on MetadataService using the configured id, and publish the 'bookie-rpc' endpoint as configured by 
the **advertisedAddress** configuration option and the other network related options.
This BookieId will be present only in the configuration file and it is the key to lookup the *Cookie* on the MetadataService.
Inadvertently changing the BookieId will prevent the Bookie to boot as it won't find a matching Cookie.
There is no need to store the BookieId on the cookie or persist it on the local storage (ledgers, indexes or journal directories).

#### Auditor and Replication Changes

The Auditor deals with LedgerMetadata and so it will simply work with BookieIds and not with BookieSocketAddress.
When the Auditor needs to connect to a Bookie it will use the BookieAddressResolver to get the current address of the Bookie. 

#### Bookie Garbage Collection Changes

The Bookie decides to reclaim space by looking into LedgerMetadata and checking that a given ledger does not exist anymore.
It will use its own local BookieId instead of the BookieSocketAddress as expected.

#### Tools changes
All of the tools that deal with LedgerMetadata will use BookieId instead of BookieSocketAddress, in general this fact will allow to use free forn BookieIDs,
instead of hostname:port pairs (we had validations on tools that helped the user to use always BookieIds in hostname:port form).

#### REST API Changes
In the REST API we will deal with BookieIds and not with BookieSocketAddresses anymore, the change will be straighforward and compatible with current API.
When new custom BookieIDs will be used then they will appear on the REST API as well, but this will be expected by users.


### Compatibility, Deprecation, and Migration Plan

The proposed change will be compatible with all existing clients and bookies as far as you still use BookieIds in the hostname:port form and to not use a custom BookieId.
The Bookie by default will continue to use as BookieID a compatible value computed exactly as in version 4.11.
Incompatibility will start as soon as you enable custom BookieIDs on the bookies, from that point clients and old Auditors won't be able to deal with new bookies.
New clients will always be able to connect and use legacy bookies.

Custom EnsemblePlacementPolicies must be adapted to the new interfaces but the change will usually as simple as just replacing BookieSocketAdress with BookieId.
No need to change address to rack mapping scripts, as they will still deal with raw DNS hostnames and not with BookieIds.

### Test Plan

New unit tests will be added to cover all of the code changes.
No need for additional integration tests.

### Rejected Alternatives

#### Make BookieSocketAddress an abstract class

In order to preserve most of the binary compatibility in the Java client we could still keep BookieSocketAddress class in LedgerMetadata and have some "GenericBookieSocketAddress" and "PhysicalBookieSocketAddress" implementations.
But this way it won't be easy to understand where we are using a "bookie id" and when we are referring to a network address.
The BookieAddressResolver interface would be needed anyway and it should deal with pure BookieIds and BookieSocketAddress instance that are already resolved to
a network address.

#### Force a specific format (like UUID) to custom BookieId
The is no need to force the BookieId to use a fixed format, like a UUID or other form of standard ID scheme.
Probably new IDs will include the region/availability zone information in order to simplify EnsemblePlacement policies (no more need to pass from DNS to switch mappers) and we cannot know now all of the usages of this parameter. 
