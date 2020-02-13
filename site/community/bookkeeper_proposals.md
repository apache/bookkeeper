---
title: BookKeeper Proposals
---

This page describes a proposed *BookKeeper Proposal (BP)* process for proposing a major change to BookKeeper.

## Process

### What is considered a "major change" that needs a BP?

Any of the following should be considered a major change:

- Any major new feature, subsystem, or piece of functionality
- Any change that impacts the public interfaces of the project
- Any change that impacts developer workflow of the project

All the following are public interfaces that people build around:

- Binary log format
- The network protocol and api behavior
- Configuration, especially client configuration
- Monitoring/Stats provider
- Command line tools and arguments

### What should be included in a BP?

A BP should contain the following sections:

- *Motivation*: describe the problem to be solved
- *Proposed Change*: describe the new thing you want to do. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences, depending on the scope of the change.
- *New or Changed Public Interfaces*: impact to any of the "compatibility commitments" described above. We want to call these out in particular so everyone thinks about them.
- *Migration Plan and Compatibility*: if this feature requires additional support for a no-downtime upgrade describe how that will work
- *Rejected Alternatives*: What are the other alternatives you considered and why are they worse? The goal of this section is to help people understand why this is the best solution now, and also to prevent churn in the future when old alternatives are reconsidered.

### Who should initiate the BP?

Anyone can initiate a BP but you shouldn't do it unless you have an intention of getting the work done to implement it (otherwise it is silly).

### How to make a BP?

Here is the process for making a BP:

1. Create an issue `BP-<number>: [capation of bookkeeper proposal]`. E.g. `BP-1: 64 bits ledger id support`.
    - Take the next available BP number from this page.
    - Write a brief description about what BP is for in this issue. This issue will be the master issue for tracking the status of this BP and its implementations.
      All the implementations of this BP should be listed and linked to this master issues.
1. Write the proposal for this BP. There are two ways to write a bookkeeper proposal. You can choose to write a BP using markdown, or write a BP
using Google Doc.
    - Markdown
        - Make a copy of the [BP-Template](https://github.com/apache/bookkeeper/tree/master/site/bps/BP-template.md). Name the BP file as `BP-<number>-[caption-of-proposal].md`.
        ```shell
        $ cp site/bps/BP-template.md site/bps/BP-xyz-capation-of-proposal.md
        ```
        - Fill the sections listed in the BP template.
            - issue: replace `<issue-number>` with the issue number.
            - state: "Under Discussion"
            - release: leave the release to `N/A`. you can only mark a release after a BP is implemented.
    - Google Doc
        - Make a copy of the [BP-Template](https://docs.google.com/document/d/1DsmH54LoohgwqnEjESPQNtIYxxcOy2rwonZ_TJCwws0). Name the BP file as `BP-<number>-[caption-of-proposal]`.
        - Fill the sections listed in the BP template.
1. Send a PR for this BP. Following the instructions in the pull request template.
    - add `BP` label to this PR
    - attach the google doc link in the PR description if the BP is written in google doc
    - don't associate this PR with any release or milestone
    - edit `site/community/bookkeeper_proposals.md`:
        - bump the next bp number
        - add this BP to `Inprogress` section
1. You can tag committers on this RP for reviewers, or start a `[DISCUSS]` thread on Apache mailing list. If you are sending an email, please make sure that the subject
   of the thread is of the format `[DISCUSS] BP-<number>: capation of bookkeeper proposal`.
1. Once the BP is finalized, reviewed and approved by committers, the BP is accepted. The criteria for acceptance is [lazy majority](http://bookkeeper.apache.org/bylaws.html).
    1. Committers merge the PR after a BP is accepted. The development for this BP moves forward with implementations. The BP should be updated if there is anything changed during implementing it.
    1. After all the implementations for a given BP are completed, a new PR should be sent for changing the state of a BP:
        - state: "Adopted"
        - release: set to the release that includes this BP.
        - moving the BP from `Inprogress` to `Adopted`.
    1. The final PR for changing BP state will be used as the criteria for marking a BP as completed.
1. If a BP is failed or rejected:
    1. Update the PR to change the state of a BP
        - state: "Discarded"
        - add a paragraph at the first paragraph of this BP for describing the reasons.
        - moving the BP from `Inprogress` to `Discarded`.
    2. Once the PR is updated, committers can merge this proposal PR and close the master issue of this BP.

## All Proposals

This section lists all the _bookkeeper proposals_ made to BookKeeper.

*Next Proposal Number: 38*

### Inprogress

Proposal | State
:--------|:-----
[BP-4 - BookKeeper Lifecycle Management](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-4+-+BookKeeper+Lifecycle+Management) | Draft
[BP-8 - Queue based auto rereplicator](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-8+-+Queue+based+auto+rereplicator) | Draft
[BP-12 - Improve documentation](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-12+-+Improve+documentation) | Accepted
[BP-14 Relax durability](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-14+Relax+durability) | Accepted
[BP-16: Thin Client - Remove direct metadata storage access from clients](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-16%3A+Thin+Client+-+Remove+direct+metadata+storage+access+from+clients) | Draft
[BP-18: LedgerType, Flags and StorageHints](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-18%3A+LedgerType%2C+Flags+and+StorageHints) | Accepted
[BP-26: Move distributedlog library as part of bookkeeper](../../bps/BP-26-move-distributedlog-core-library) | Accepted
[BP-27: New BookKeeper CLI](../../bps/BP-27-new-bookkeeper-cli) | Accepted
[BP-28: use etcd as metadata store](../../bps/BP-28-etcd-as-metadata-store) | Accepted
[BP-29: Metadata API module](../../bps/BP-29-metadata-store-api-module) | Accepted
[BP-30: BookKeeper Table Service](https://docs.google.com/document/d/155xAwWv5IdOitHh1NVMEwCMGgB28M3FyMiQSxEpjE-Y/edit#heading=h.56rbh52koe3f) | Accepted
[BP-31: BookKeeper Durability Anchor](../../bps/BP-31-durability) | Accepted
[BP-32: Advisory (optimistic) write close](../../bps/BP-32-advisory-write-close) | Accepted
[BP-33: Move releasing docker images out of main repo](../../bps/BP-33-building-official-docker-imags) | Draft
[BP-34: Cluster Metadata Checker](../../bps/BP-34-cluster-metadata-checker) | Accepted
[BP-35: 128 bits support](../../bps/BP-35-128-bits-support) | Accepted
[BP-36: Stats documentation annotation](../../bps/BP-36-stats-documentation-annotation) | Accepted
[BP-37: Improve configuration management for better documentation](../../bps/BP-37-conf-documentation) | Accepted
[BP-38: Publish Bookie Service Info on Metadata Service](../../bps/BP-38-bookie-endpoint-discovery) | Accepted

### Adopted

Proposal | Release
:--------|:-------
[BP-1 - 64 bits ledger id support](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-1+-+64+bits+ledger+id+support) | 4.5.0
[BP-2 - Resource aware data placement](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-2+-+Resource+aware+data+placement) | 4.5.0
[BP-3 - Security support](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-3+-+Security+support) | 4.5.0
[BP-5 - Allow reads outside the LAC Protocol](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-5+Allow+reads+outside+the+LAC+Protocol) | 4.5.0
[BP-6 - Use separate log for compaction](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-6+-+Use+separate+log+for+compaction) | 4.6.0
[BP-9 - Github issues for Issue Tracking](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-9+-+Github+issues+for+Issue+Tracking) | 4.5.0
[BP-10 - Official Bookkeeper Docker Image](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-10+-+Official+Bookkeeper+Docker+Image) | 4.5.0
[BP-11 - Move website/documentation to Jekyll based site](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=71012301) | 4.5.0
[BP-13 - Time Based Release Plan](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-13+-+Time+Based+Release+Plan) | 4.6.0
[BP-15 - New CreateLedger API](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-15+New+CreateLedger+API) | 4.6.0
[BP-17 - Define BookKeeper public http endpoints](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-17%3A+Define+BookKeeper+public+http+endpoints) | 4.6.0
[BP-20: Github workflow for bookkeeper proposals](../../bps/BP-20-github-workflow-for-bookkeeper-proposals) | 4.7.0
[BP-25: Move checksum to proto](../../bps/BP-25-MovingChecksumToProto) | 4.7.0

### Discarded

Proposal | Reason
:--------|:------
[BP-7 - Explicit LAC on addEntry](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-7+-+Explicit+LAC+on+addEntry) | Not A Problem
[BP-21: New API close inconsistencies](../../bps/BP-21-new-api-close-inconsistencies) | Not A Problem
[BP-22: Separate closing ledgers from opening ledgers](../../bps/BP-22-separate-closing-ledgers-from-opening-ledgers) | Not A Problem
