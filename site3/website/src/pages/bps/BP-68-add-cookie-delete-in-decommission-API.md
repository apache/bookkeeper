# BP-68: Delete cookie as part of bookkeeper decommission

### Motivation

_The current decommission process in Apache BookKeeper has two main interfaces: the REST API and the command-line interface (CLI). However, there is a discrepancy between these two methods, specifically in the handling of cookie deletion. The CLI includes logic for deleting cookies associated with decommissioned Bookies, while the REST API lacks this functionality. This BP proposes enhancing the REST API to include cookie deletion logic, aligning it with the behavior of the CLI._

### Public Interfaces

_api/v1/autorecovery/decommission_
add the following in payload:

```
delete_cookie
```

default value is false, which means do not delete the Cookie.

### Proposed Changes

_Implement the same cookie deletion logic present in the CLI within the REST API. This ensures that when a Bookie is decommissioned via the REST API, all associated cookies are removed, preventing any stale or conflicting state._

### Compatibility, Deprecation, and Migration Plan

- The current behavior can be preserved as a default, with the cookie deletion being an optional feature that can be toggled via request parameters.

### Test Plan

_Tests to make sure cookies are getting removed as part of the decommission_
