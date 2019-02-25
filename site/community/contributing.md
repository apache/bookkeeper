---
title: Contributing to Apache BookKeeper
---

* TOC
{:toc}

The Apache BookKeeper community welcomes contributions from anyone with a passion for distributed systems! BookKeeper has many different opportunities for contributions --
write new examples/tutorials, add new user-facing libraries, work on the core storage components, integrate with different metadata stores (ZooKeeper, Etcd etc), or
participate on the documentation effort.

We use a review-then-commit workflow in BookKeeper for all contributions.

**For larger contributions or those that affect multiple components:**

1. **Engage**: We encourage you to work with the BookKeeper community on the [Github Issues](https://github.com/apache/bookkeeper/issues) and [developer’s mailing list](../mailing-lists) to identify good areas for contribution.
1. **Design:** More complicated contributions will likely benefit from some early discussion in order to scope and design them well.

**For all contributions:**

1. **Code:** The best part ;-)
1. **Review:** Submit a pull request with your contribution to our [GitHub Repo](https://github.com/apache/bookkeeper). Work with a committer to review and iterate on the code, if needed.
1. **Commit:** A BookKeeper committer merges the pull request into our [GitHub Repo](https://github.com/apache/bookkeeper).

We look forward to working with you!

## Engage

### Mailing list(s)

We discuss design and implementation issues on the [dev@bookkeeper.apache.org](mailto:dev@bookkeeper.apache.org) mailing list, which is archived [here](https://lists.apache.org/list.html?dev@bookkeeper.apache.org). Join by emailing [`dev-subscribe@bookkeeper.apache.org`](mailto:dev-subscribe@bookkeeper.apache.org).

If interested, you can also join the other [mailing lists](../mailing-lists).

### Github Issues

We are moving to use [Github Issues](https://github.com/apache/bookkeeper/issues) as an issue tracking and project management tool, as well as a way to communicate among a very diverse and distributed set of contributors. To be able to gather feedback, avoid frustration, and avoid duplicated efforts all BookKeeper-related work should be tracked there.

If you do not already have an Github account, sign up [here](https://github.com/join).

If a quick [search](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93) doesn’t turn up an existing Github issue for the work you want to contribute, create it. Please discuss your idea with a committer in Github or, alternatively, on the developer mailing list.

If there’s an existing Github issue for your intended contribution, please comment about your intended work. Once the work is understood, a committer will assign the issue to you. If an issue is currently assigned, please check with the current assignee before reassigning.

For moderate or large contributions, you should not start coding or writing a design document unless there is a corresponding Github issue assigned to you for that work. Simple changes, like fixing typos, do not require an associated issue.

### Online discussions

We are using [Apache BookKeeper Slack channel](https://apachebookkeeper.slack.com/) for online discussions. You can self-invite yourself by accessing [this link](http://apachebookkeeper.slack.com/).

Slack channels are great for quick questions or discussions on specialized topics. Remember that we strongly encourage communication via the mailing lists, and we prefer to discuss more complex subjects by email. Developers should be careful to move or duplicate all the official or useful discussions to the issue tracking system and/or the dev mailing list.

## Design

To avoid potential frustration during the code review cycle, we encourage you to clearly scope and design non-trivial contributions with the BookKeeper community before you start coding.

We are using [BookKeeper Proposals](http://bookkeeper.apache.org/community/bookkeeper_proposals/) for managing major changes to BookKeeper.

## Code

To contribute code to Apache BookKeeper, you’ll have to do a few administrative steps once, and then follow the [Coding Guide](../coding_guide).

### One-time Setup

#### [Optionally] Submit Contributor License Agreement

Apache Software Foundation (ASF) desires that all contributors of ideas, code, or documentation to the Apache projects complete, sign, and submit an [Individual Contributor License Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA). The purpose of this agreement is to clearly define the terms under which intellectual property has been contributed to the ASF and thereby allow us to defend the project should there be a legal dispute regarding the software at some future time.

We require you to have an ICLA on file with the Apache Secretary for larger contributions only. For smaller ones, however, we rely on [clause five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of the Apache License, Version 2.0, describing licensing of intentionally submitted contributions and do not require an ICLA in that case.

#### Obtain a GitHub account

We use GitHub’s pull request functionality to review proposed code changes.

If you do not already have a personal GitHub account, sign up [here](https://github.com/join).

#### Fork the repository on GitHub

Go to the [BookKeeper GitHub Repo](https://github.com/apache/bookkeeper/) and fork the repository to your own private account. This will be your private workspace for staging changes.

#### Clone the repository locally

You are now ready to create the development environment on your local machine. Feel free to repeat these steps on all machines that you want to use for development.

We assume you are using SSH-based authentication with GitHub. If necessary, exchange SSH keys with GitHub by following [their instructions](https://help.github.com/articles/generating-an-ssh-key/).

Clone your personal BookKeeper’s GitHub fork.

    $ git clone https://github.com/<Github_user>/bookkeeper.git
    $ cd bookkeeper

Add Apache Repo as additional Git remotes, where you can sync the changes (for committers, you need these two remotes for pushing changes).

	$ git remote add apache https://github.com/apache/bookkeeper
	$ git remote add apache-github https://github.com/apache/bookkeeper

You are now ready to start developing!

#### [Optional] IDE Setup

Depending on your preferred development environment, you may need to prepare it to develop BookKeeper code.

##### IntelliJ

###### Enable Annotation Processing

To configure annotation processing in IntelliJ:

1. Open Annotation Processors Settings dialog box by going to Settings -> Build, Execution, Deployment -> Compiler -> Annotation Processors.
1. Select the following buttons:
    1. "Enable annotation processing"
    1. "Obtain processors from project classpath"
    1. "Store generated sources relative to: Module content root"
1. Set the generated source directories to be equal to the Maven directories:
    1. Set "Production sources directory:" to "target/generated-sources/annotations".
    1. Set "Test sources directory:" to "target/generated-test-sources/test-annotations".
1. Click "OK".

###### Checkstyle
IntelliJ supports checkstyle within the IDE using the Checkstyle-IDEA plugin.

1. Install the "Checkstyle-IDEA" plugin from the IntelliJ plugin repository.
1. Configure the plugin by going to Settings -> Other Settings -> Checkstyle.
1. Set the "Scan Scope" to "Only Java sources (including tests)".
1. In the "Configuration File" pane, add a new configuration using the plus icon:
    1. Set the "Description" to "BookKeeper".
    1. Select "Use a local Checkstyle file", and point it to
      "buildtools/src/main/resources/bookkeeper/checkstyle.xml" within
      your repository.
    1. Check the box for "Store relative to project location", and click
      "Next".
    1. Configure the "checkstyle.suppressions.file" property value to
      "suppressions.xml", and click "Next", then "Finish".
1. Select "BookKeeper" as the only active configuration file, and click "Apply" and
   "OK".
1. Checkstyle will now give warnings in the editor for any Checkstyle
   violations.

You can also scan an entire module by opening the Checkstyle tools window and
clicking the "Check Module" button. The scan should report no errors.

Note: Selecting "Check Project" may report some errors from the archetype
modules as they are not configured for Checkstyle validation.

##### Eclipse

Use a recent Eclipse version that includes m2e. Currently we recommend Eclipse Neon.
Start Eclipse with a fresh workspace in a separate directory from your checkout.

###### Initial setup

1. Import the bookkeeper projects

	File
	-> Import...
	-> Existing Maven Projects
	-> Browse to the directory you cloned into and select "bookkeeper"
	-> make sure all bookkeeper projects are selected
	-> Finalize

You now should have all the bookkeeper projects imported into eclipse and should see no compile errors.

###### Checkstyle
Eclipse supports checkstyle within the IDE using the Checkstyle plugin.

1. Install the [Checkstyle plugin](https://marketplace.eclipse.org/content/checkstyle-plug).
1. Configure Checkstyle plugin by going to Preferences - Checkstyle.
    1. Click "New...".
    1. Select "External Configuration File" for type.
    1. Click "Browse..." and select "buildtools/src/main/resources/bookkeeper/checkstyle.xml".
    1. Enter "BookKeeper Checks" under "Name:".
    1. Click "OK", then "OK".

### Create a branch in your fork

You’ll work on your contribution in a branch in your own (forked) repository. Create a local branch, initialized with the state of the branch you expect your changes to be merged into. Keep in mind that we use several branches, including `master`, feature-specific, and release-specific branches. If you are unsure, initialize with the state of the `master` branch.

	$ git fetch apache
	$ git checkout -b <my-branch> apache/master

At this point, you can start making and committing changes to this branch in a standard way.

### Syncing and pushing your branch

Periodically while you work, and certainly before submitting a pull request, you should update your branch with the most recent changes to the target branch.

    $ git pull --rebase

Remember to always use `--rebase` parameter to avoid extraneous merge commits.

Then you can push your local, committed changes to your (forked) repository on GitHub. Since rebase may change that branch's history, you may need to force push. You'll run:

	$ git push <GitHub_user> <my-branch> --force

### Testing

All code should have appropriate unit testing coverage. New code should have new tests in the same contribution. Bug fixes should include a regression test to prevent the issue from reoccurring.

## Review

Once the initial code is complete and the tests pass, it’s time to start the code review process. We review and discuss all code, no matter who authors it. It’s a great way to build community, since you can learn from other developers, and they become familiar with your contribution. It also builds a strong project by encouraging a high quality bar and keeping code consistent throughout the project.

### Create a pull request

Organize your commits to make a committer’s job easier when reviewing. Committers normally prefer multiple small pull requests, instead of a single large pull request. Within a pull request, a relatively small number of commits that break the problem into logical steps is preferred. For most pull requests, you'll squash your changes down to 1 commit. You can use the following command to re-order, squash, edit, or change description of individual commits.

    $ git rebase -i apache/master

You'll then push to your branch on GitHub. Note: when updating your commit after pull request feedback and use squash to get back to one commit, you will need to do a force submit to the branch on your repo.

Navigate to the [BookKeeper GitHub Repo](https://github.com/apache/bookkeeper) to create a pull request. The title of the pull request should be strictly in the following format:

	Issue <Github-issue-#> <Title of the pull request>

Please include a descriptive pull request message to help make the comitter’s job easier when reviewing. It’s fine to refer to existing design docs or the contents of the associated JIRA as appropriate.

If you know a good committer to review your pull request, please make a comment like the following. If not, don’t worry -- a committer will pick it up.

	Hi @<GitHub-committer-username>, can you please take a look?

When choosing a committer to review, think about who is the expert on the relevant code, who the stakeholders are for this change, and who else would benefit from becoming familiar with the code. If you’d appreciate comments from additional folks but already have a main committer, you can explicitly cc them using `@<GitHub-committer-username>`.

### Code Review and Revision

During the code review process, don’t rebase your branch or otherwise modify published commits, since this can remove existing comment history and be confusing to the committer when reviewing. When you make a revision, always push it in a new commit.

Our GitHub repo automatically provides pre-commit testing coverage using Jenkins. Please make sure those tests pass; the contribution cannot be merged otherwise.

### LGTM

Once the committer is happy with the change, they’ll approve the pull request with an LGTM (“*looks good to me!*”) or a `+1`. At this point, the committer will take over, possibly make some additional touch ups, and merge your changes into the codebase.

In the case the author is also a committer, either can merge the pull request. Just be sure to communicate clearly whose responsibility it is in this particular case.

Thank you for your contribution to BookKeeper!

### Deleting your branch
Once the pull request is merged into the BookKeeper repository, you can safely delete the branch locally and purge it from your forked repository.

From another local branch, run:

	$ git fetch origin
	$ git branch -d <my-branch>
	$ git push origin --delete <my-branch>

## Commit (committers only)

Once the code has been peer reviewed by a committer, the next step is for the committer to merge it into the Github repo.

Pull requests should not be merged before the review has approved from another committer. Exceptions to this rule may be made rarely, on a case-by-case basis only, in the committer’s discretion for situations such as build breakages.

Committers should never commit anything without going through a pull request, since that would bypass test coverage and potentially cause the build to fail due to checkstyle, etc. In addition, pull requests ensure that changes are communicated properly and potential flaws or improvements can be spotted. **Always go through the pull request, even if you won’t wait for the code review.** Even then, comments can be provided in the pull requests after it has been merged to work on follow-ups.

Committing is managed by a python script [bk-merge-pr.py](https://github.com/apache/bookkeeper/blob/master/dev/bk-merge-pr.py). Just follow the instructions promoted by the
script and types the information needed by the script.

### Contributor License Agreement

If you are merging a larger contribution, please make sure that the contributor has an ICLA on file with the Apache Secretary. You can view the list of committers [here](http://home.apache.org/phonebook.html?unix=committers), as well as [ICLA-signers who aren’t yet committers](http://home.apache.org/unlistedclas.html).

For smaller contributions, however, this is not required. In this case, we rely on [clause five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of the Apache License, Version 2.0, describing licensing of intentionally submitted contributions.

### Tests
Before merging, please make sure that Jenkins tests pass, as visible in the GitHub pull request. Do not merge the pull request otherwise.

### Finishing touches

At some point in the review process, you should take the pull request over and complete any outstanding work that is either minor, stylistic, or otherwise outside the expertise of the contributor. The [merge script](https://github.com/apache/bookkeeper/blob/master/dev/bk-merge-pr.py) provides instructions for committers to address such minor conflicts.

## Documentation

### Website

The BookKeeper website is in the same [BookKeeper Github Repo](https://github.com/apache/bookkeeper). The source files are hosted under `site` directory in `master` branch,
the static content is generated by CI job and merged into the `asf-site` branch.

Follow the [README](https://github.com/apache/bookkeeper/tree/master/site) for making contributions to the website.
