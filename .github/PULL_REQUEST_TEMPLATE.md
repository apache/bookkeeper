Descriptions of the changes in this PR:



### Motivation

(Explain: why you're making that change, what is the problem you're trying to solve)

### Changes

(Describe: what changes you have made)

Master Issue: #<master-issue-number>

> ---
> In order to uphold a high standard for quality for code contributions, Apache BookKeeper runs various precommit
> checks for pull requests. A pull request can only be merged when it passes precommit checks.
>
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
