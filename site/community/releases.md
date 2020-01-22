---
title: Release Management
---

> Apache BookKeeper community adopts [Time Based Release Plan](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-13+-+Time+Based+Release+Plan) starting from 4.6.0.

Apache BookKeeper community makes a feture release every 6 month (since `4.10.0`).

- A month before the release date, the release manager will cut branches and also publish a list of features that will be included in the release. These features will typically
    be [BookKeeper Proposals](http://bookkeeper.apache.org/community/bookkeeper_proposals/), but not always.
- Another week will be left for *minor* features to get in, but at this point the community will start efforts to stabilize the release branch and contribute mostly tests and fixes.
- Two weeks before the release date, the bookkeeper community will announce code-freeze and start rolling out release candidates, after which only fixes for blocking bugs will be merged.

## Current Release

### Feature Release Window

The next feature release is `4.11.0`. The release window is the following:

| **Date** | **Event** |
| December 1, 2019 | Merge window opens on master branch |
| April 6, 2020 | Major feature should be in, Cut release branch |
| April 13, 2020 | Minor feature should be in, Stabilize release branch |
| April 20, 2020 | Code freeze, Only accept fixes for blocking issues, Rolling out release candidates |

## Release Schedule

- **4.11.0**: December 2019 - May 2020
- **4.12.0**: June 2020 - November 2020
- **4.13.0**: December 2020 - May 2021

