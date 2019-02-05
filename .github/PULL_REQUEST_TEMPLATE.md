Descriptions of the changes in this PR:



### Motivation

(Explain: why you're making that change, what is the problem you're trying to solve)

### Changes

(Describe: what changes you have made)

Master Issue: #<master-issue-number>

> ---
> In order to uphold a high standard for quality for code contributions, Apache BookKeeper runs various precommit
> checks for pull requests. A pull request can only be merged when it passes precommit checks. However running all
> the precommit checks can take a long time, some trivial changes don't need to run all the precommit checks. You
> can check following list to skip the tests that don't need to run for your pull request. Leave them unchecked if
> you are not sure, committers will help you:
>
> - [ ] [skip bookkeeper-server bookie tests]: skip testing `org.apache.bookkeeper.bookie` in bookkeeper-server module.
> - [ ] [skip bookkeeper-server client tests]: skip testing `org.apache.bookkeeper.client` in bookkeeper-server module.
> - [ ] [skip bookkeeper-server replication tests]: skip testing `org.apache.bookkeeper.replication` in bookkeeper-server module.
> - [ ] [skip bookkeeper-server tls tests]: skip testing `org.apache.bookkeeper.tls` in bookkeeper-server module.
> - [ ] [skip bookkeeper-server remaining tests]: skip testing all other tests in bookkeeper-server module.
> - [ ] [skip integration tests]: skip docker based integration tests. if you make java code changes, you shouldn't skip integration tests.
> - [ ] [skip build java8]: skip build on java8. *ONLY* skip this when *ONLY* changing files under documentation under `site`.
> - [ ] [skip build java11]: skip build on java11. *ONLY* skip this when *ONLY* changing files under documentation under `site`.
> ---

> ---
> Be sure to do all of the following to help us incorporate your contribution
> quickly and easily:
>
> If this PR is a BookKeeper Proposal (BP):
>
> - [ ] Make sure the PR title is formatted like:
>     `<BP-#>: Description of bookkeeper proposal`
>     `e.g. BP-1: 64 bits ledger is support`
> - [ ] Attach the master issue link in the description of this PR.
> - [ ] Attach the google doc link if the BP is written in Google Doc.
>
> Otherwise:
> 
> - [ ] Make sure the PR title is formatted like:
>     `<Issue #>: Description of pull request`
>     `e.g. Issue 123: Description ...`
> - [ ] Make sure tests pass via `mvn clean apache-rat:check install spotbugs:check`.
> - [ ] Replace `<Issue #>` in the title with the actual Issue number.
> 
> ---
