---
title: "BP-33: Move releasing official docker images out of main repo"
issue: https://github.com/apache/bookkeeper/1449
state: "Under Discussion"
release: "4.7.1"
---

### Motivation

Current bookkeeper docker images are auto-built by apache docker account. However it becomes problematic in the release process:

Docker autobuild uses release tag for labeling the versions for docker images. But the `Dockerfile` can only be updated after
a release is successfully made. So we have to retag a release after a release, in order to update `Dockerfile` to build the docker
image.

### Proposed Changes

Follow what `flink` is doing, and maintain the docker files outside of the bookkeeper main repo.

- Create an organization `asfbookkeeper-ecosystem` for hosting repos that related bookkeeper but not necessarily needed to be put in main repo.
- Create a repo `docker-bookkeeper` under `asfbookkeeper-ecosystem` for hosting the docker files following the suggested practices from making a docker official image.
- Add a library definition file under `docker-library/official-images` for bookkeeper.
- Add an image doc under `docker-library/docs` for bookkeeper.
- Update the release guide on how to update docker images at the end of each release.
- Remove `docker` dir from main repo or make it used for building *unreleased* docker images only.
- Disable docker autobuild from apache account.

Proposed docker file repo: https://github.com/asfbookkeeper-ecosystem/docker-bookkeeper

### Compatibility, Deprecation, and Migration Plan

N/A

### Test Plan

N/A

### Rejected Alternatives

N/A
