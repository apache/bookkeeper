---
title: Issue Report
---

To report an issue, you will need to create a **[New Issue](https://github.com/apache/bookkeeper/issues/new)**.
Be aware that resolving your issue may require **your participation**. Please be willing and prepared to aid the developers in finding the actual cause of the issue so that they can develop a comprehensive solution.

## Before creating a new Issue:

-  Search for the issue you want to report, it may already have been reported.
-  If you find a similar issue, add any new information you might have as a comment on the existing issue. If it's different enough, you might decide it needs to be reported in a new issue.
-  If an issue you recently reported was closed, and you don't agree with the reasoning for it being closed, you will need to reopen it to let us re-investigate the issue.
-  Do not reopen the tickets that are in a previously completed milestone. Instead, open a new issue.

## Creating a Issue:

Here is an very useful artical [How to report bugs effectively]( http://www.chiark.greenend.org.uk/%7Esgtatham/bugs.html)

### Provide useful and required information

#### If it is a question

-  Please check our [documentation](http://bookkeeper.apache.org/docs/latest/) first. 
-  If you could not find an answer there, please consider asking your question in our community mailing list at [dev@bookkeeper.apache.org](mailto:dev@bookkeeper.apache.org), or reach out us on our [Slack channel](../slack).  It would benefit other members of our community.

#### If it is a **FEATURE REQUEST**

-  Please describe the feature you are requesting.
-  Indicate the importance of this issue to you (_blocker_, _must-have_, _should-have_, _nice-to-have_). Are you currently using any workarounds to address this issue?
-  Provide any additional detail on your proposed use case for this feature.
-  If it is a [BookKeeper Proposal](http://bookkeeper.apache.org/community/bookkeeper_proposals/), please label this issue as `BP`.

#### If it is a **BUG REPORT**

Please describe the issue you observed:

- What did you do?
- What did you expect to see?
- What did you see instead?

### Use Labels

Issue labels help to find issue reports and recognize the status of where an issue is in the lifecycle. An issue typically has the following 2 types of labels:

1. **type** identifying its type.
1. **area** identifying the areas it belongs to.

#### Type

- **type/bug**: The issue describes a product defect.
- **type/feature**: The issue describes a new feature, which requires extensive design and testing.
- **type/question**: The issue contains a user or contributor question requiring a response.
- **type/task**: The issue describes a new task, which requires extensive design and testing.

#### Area

- **area/bookie**: Code changes related to bookie storage.
- **area/build**: Code changes related to project build.
- **area/client**: Code changes related to clients.
- **area/docker**: Code changes related to docker builds.
- **area/documentation**: Code changes related to documentation (including website changes).
- **area/metadata**: Code changes related to metadata management.
- **area/protocol**: Protocol changes.
- **area/release**: Release related tasks.
- **area/security**: Security related changes.
- **area/tests**: Tests related changes.

#### Priority

At most of the time, it is hard to find a right `priority` for issues. Currently we only have one label `priority/blocker` for marking an issue as a blocker
for a given release. Please only mark this issue as *blocker* only when it is a real blocker for a given release. If you have no idea about this, just leave
it as empty.

#### Status

If an issue is assigned to a contributor, that means there is already a contributor working on it. If an issue is unassigned, you can pick this up by assigning
it to yourself (for committers), or comment on the issue saying you would like to give it a try.

If an issue is not an issue anymore, close it and mark it as `status/wontfix`.

All the issues marked as `status/help-needed` are good candidates for new contributors to start with.

#### BookKeeper Proposal

If an issue is a `BookKeeper Proposal`, label it as `BP`.

#### Milestone and Release

If you want some features merge into a given milestone or release, please associate the issue with a given milestone or release.

If you have no idea, just leave them empty. The committers will manage them for you.

Thank you for contributing to Apache BookKeeper!
