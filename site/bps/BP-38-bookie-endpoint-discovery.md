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

client:
inteface BookieServiceInfo {
    Iterable<String> keys();
    String get(String key, String defaultValue);
}

in RegistrationClient interface:

BookieServiceInfo getBookieServiceInfo(String bookieId)

in RegistrationManager
void registerBookie(String bookieId, boolean readOnly)
becomes
void registerBookie(String bookieId, Map<String, String> bookieServieInfo, boolean readOnly)

Notable known keys are:
- bookie.binary.address: hostname:port
- bookie.tls.supported: true|false
- boolie.auth.type: node|sasl
- bookie.admin.url: protocol://hostname:port
- bookie.metrics.url:  protocol://hostname:port/metrics


_Briefly list any new interfaces that will be introduced as part of this proposal or any existing interfaces that will be removed or changed. The purpose of this section is to concisely call out the public contract that will come along with this feature._

A public interface is any change to the following:

- Data format, Metadata format
- The wire protocol and api behavior
- Any class in the public packages
- Monitoring
- Command line tools and arguments
- Anything else that will likely break existing users in some way when they upgrade

### Proposed Changes

_Describe the new thing you want to do in appropriate detail. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences. Use judgement based on the scope of the change._

### Compatibility, Deprecation, and Migration Plan

- What impact (if any) will there be on existing users? 
- If we are changing behavior how will we phase out the older behavior? 
- If we need special migration tools, describe them here.
- When will we remove the existing behavior?

### Test Plan

_Describe in few sentences how the BP will be tested. We are mostly interested in system tests (since unit-tests are specific to implementation details). How will we know that the implementation works as expected? How will we know nothing broke?_

### Rejected Alternatives

_If there are alternative ways of accomplishing the same thing, what were they? The purpose of this section is to motivate why the design is the way it is and not some other way._
