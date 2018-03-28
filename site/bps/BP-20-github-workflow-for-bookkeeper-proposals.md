---
title: "BP-20: github workflow for bookkeeper proposals"
issue: https://github.com/apache/bookkeeper/761
state: "Adopted"
release: "4.7.0"
---

### Motivation

We have a good BP process for introducing enhancements, features. However, this process is not well integrated with our github review process, and the content of a BP
is not used for documentation. This proposal is to propose moving the BP workflow from ASF wiki to Github. There are a couple of reasons for making this change:

- the ASF cwiki is disconnected from Github, and usually becomes out of date quickly. It isn't really caught up with the code changes.
  Most of the content (documentation, contribution/release guides) are already in website, the ASF wiki is only used for tracking BPs and community meeting notes at this point.
- Moving BP workflow from wiki to github will leverage the same github review process as code changes. So developers are easier to review BPs and make comments.
- The BPs can eventually be used as a basis for documentation.

### Proposed Changes

All the BPs are maintained in `site/bps` directory. To make a bookkeeper proposal, a developer does following steps:

1. Create an issue `BP-<number>: [capation of bookkeeper proposal]`. E.g. `BP-1: 64 bits ledger id support`.
    - Take the next available BP number from this page.
    - Write a brief description about what BP is for in this issue. This issue will be the master issue for tracking the status of this BP and its implementations.
      All the implementations of this BP should be listed and linked to this master issues.
1. Write the proposal for this BP.
    - Make a copy of the [BP-Template](https://github.com/apache/bookkeeper/tree/master/site/bps/BP-template.md). Name the BP file as `BP-<number>-[caption-of-proposal].md`.
    ```shell
    $ cp site/bps/BP-template.md site/bps/BP-xyz-capation-of-proposal.md
    ```
    - Fill the sections listed in the BP template.
        - issue: replace `<issue-number>` with the issue number.
        - state: "Under Discussion"
        - release: leave the release to `N/A`. you can only mark a release after a BP is implemented.
1. Send a PR for this BP. Following the instructions in the pull request template.
    - add `BP` label to this BP
    - don't associate this PR with any release or milestone
1. You can tag committers on this RP for reviewers, or start a `[DISCUSS]` thread on Apache mailing list. If you are sending an email, please make sure that the subject
   of the thread is of the format `[DISCUSS] BP-<number>: capation of bookkeeper proposal`.
1. Once the BP is finalized, reviewed and approved by committers, the BP is accepted. The criteria for acceptance is [lazy majority](http://bookkeeper.apache.org/bylaws.html).
1. Committers merge the PR after a BP is accepted. The development for this BP moves forward with implementations. The BP should be updated if there is anything changed during
   implementing it.
1. After all the implementations for a given BP are completed, a new PR should be sent for changing the state of a BP:
    - state: "Adopted"
    - release: set to the release that includes this BP.
1. The final PR for changing BP state will be used as the criteria for marking a BP as completed.
