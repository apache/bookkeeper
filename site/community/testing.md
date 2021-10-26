---
title: BookKeeper Testing Guide
---

* TOC
{:toc}

## Overview

Apache BookKeeper is a well adopted software project with a strong commitment to testing.
Consequently, it has many testing-related needs. It requires precommit tests to ensure
code going to the repository meets a certain quality bar and it requires ongoing postcommit
tests to make sure that more subtle changes which escape precommit are nonetheless caught.
This document outlines how to write tests, which tests are appropriate where, and when tests
are run, with some additional information about the testing systems at the bottom.

## Testing Scenarios

With the tools at our disposal, we have a good set of utilities which we can use to verify
BookKeeper correctness. To ensure an ongoing high quality of code, we use precommit and postcommit
testing.

### Precommit

For precommit testing, BookKeeper uses GitHub Actions to ensure that pull requests meet a certain quality bar.
These precommits verify correctness via unit/integration tests.

In case of failures, you can re-run failing checks commenting `rerun failure checks` in the pull.
More details on GitHub actions [here](https://github.com/apache/bookkeeper/tree/master/.github/workflows/bot.yml)

### Postcommit

Currently in postcommit, we re-run precommit tests against both master and the most recent release branch.
In this way we can ensure also the main branches are not broken.