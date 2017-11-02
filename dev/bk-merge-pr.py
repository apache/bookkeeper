#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utility for creating well-formed pull request merges and pushing them to Apache. This script is a modified version
# of the one created by the Spark project (https://github.com/apache/spark/blob/master/dev/merge_spark_pr.py).
#
# Usage: ./bk-merge-pr.py (see config env vars below)
#
# This utility assumes you already have local a bookkeeper git folder and that you
# have added remotes corresponding to both:
# (i) the github apache bookkeeper mirror and
# (ii) the apache bookkeeper git repo.

import json
import os
import re
import subprocess
import sys
import urllib2

try:
    import jira.client
    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

PROJECT_NAME = "bookkeeper"

CAPITALIZED_PROJECT_NAME = "bookkeeper".upper()
GITHUB_ISSUES_NAME = "issue".upper()

# Location of the local git repository
REPO_HOME = os.environ.get("%s_HOME" % CAPITALIZED_PROJECT_NAME, os.getcwd())
# Remote name which points to the GitHub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache-github")
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")
# Reference branch name
DEV_BRANCH_NAME =  = os.environ.get("DEV_BRANCH_NAME", "master")
# ASF JIRA username
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", "")
# ASF JIRA password
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD", "")
# OAuth key used for issuing requests against the GitHub API. If this is not defined, then requests
# will be unauthenticated. You should only need to configure this if you find yourself regularly
# exceeding your IP's unauthenticated request rate limit. You can create an OAuth key at
# https://github.com/settings/tokens. This script only requires the "public_repo" scope.
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")

GITHUB_USER = os.environ.get("GITHUB_USER", "apache")
GITHUB_BASE = "https://github.com/%s/%s/pull" % (GITHUB_USER, PROJECT_NAME)
GITHUB_API_URL  = "https://api.github.com"
GITHUB_API_BASE = "%s/repos/%s/%s" % (GITHUB_API_URL, GITHUB_USER, PROJECT_NAME)
JIRA_BASE = "https://issues.apache.org/jira/browse"
JIRA_API_BASE = "https://issues.apache.org/jira"
# Prefix added to temporary branches
TEMP_BRANCH_PREFIX = "PR_TOOL"
RELEASE_BRANCH_PREFIX = "branch-"

DEFAULT_FIX_VERSION = os.environ.get("DEFAULT_FIX_VERSION", "0.9.1.0")

def get_json(url, preview_api = False):
    try:
        request = urllib2.Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header('Authorization', 'token %s' % GITHUB_OAUTH_KEY)
        if preview_api:
            request.add_header('Accept', 'application/vnd.github.black-cat-preview+json')
        return json.load(urllib2.urlopen(request))
    except urllib2.HTTPError as e:
        if "X-RateLimit-Remaining" in e.headers and e.headers["X-RateLimit-Remaining"] == '0':
            print "Exceeded the GitHub API rate limit; see the instructions in " + \
                  "bk-merge-pr.py to configure an OAuth token for making authenticated " + \
                  "GitHub requests."
        else:
            print "Unable to fetch URL, exiting: %s" % url
        sys.exit(-1)

def post_json(url, data):
    try:
        request = urllib2.Request(url, data, { 'Content-Type': 'application/json' })
        if GITHUB_OAUTH_KEY:
            request.add_header('Authorization', 'token %s' % GITHUB_OAUTH_KEY)
        return json.load(urllib2.urlopen(request))
    except urllib2.HTTPError as e:
        if "X-RateLimit-Remaining" in e.headers and e.headers["X-RateLimit-Remaining"] == '0':
            print "Exceeded the GitHub API rate limit; see the instructions in " + \
                  "bk-merge-pr.py to configure an OAuth token for making authenticated " + \
                  "GitHub requests."
        else:
            print "Unable to fetch URL, exiting: %s - %s" % (url, e)
        sys.exit(-1)

def fail(msg):
    print msg
    clean_up()
    sys.exit(-1)


def run_cmd(cmd):
    print cmd
    if isinstance(cmd, list):
        return subprocess.check_output(cmd)
    else:
        return subprocess.check_output(cmd.split(" "))


def continue_maybe(prompt):
    result = raw_input("\n%s (y/n): " % prompt)
    if result.lower() != "y":
        fail("Okay, exiting")

def clean_up():
    if original_head != get_current_branch():
        print "Restoring head pointer to %s" % original_head
        run_cmd("git checkout %s" % original_head)

    branches = run_cmd("git branch").replace(" ", "").split("\n")

    for branch in filter(lambda x: x.startswith(TEMP_BRANCH_PREFIX), branches):
        print "Deleting local branch %s" % branch
        run_cmd("git branch -D %s" % branch)

def get_current_branch():
    return run_cmd("git rev-parse --abbrev-ref HEAD").replace("\n", "")

def get_milestones():
    return get_json("https://api.github.com/repos/%s/%s/milestones?state=open&sort=due_on&direction=asc" % (GITHUB_USER, PROJECT_NAME))

def get_all_labels():
    result = get_json("https://api.github.com/repos/%s/%s/labels" % (GITHUB_USER, PROJECT_NAME))
    return map(lambda x: x['name'], result)

# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref, title, body, default_pr_reviewers, pr_repo_desc):
    pr_branch_name = "%s_MERGE_PR_%s" % (TEMP_BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (TEMP_BRANCH_PREFIX, pr_num, target_ref.upper())
    run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        run_cmd(['git', 'merge', pr_branch_name, '--squash'])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    # Offer to run unit tests before committing
    result = raw_input('Do you want to validate unit tests after the merge? (y/n): ')
    if result.lower() == 'y':
        test_res = subprocess.call('mvn clean install'.split())
        if test_res == 0:
            print('Unit tests execution succeeded')
        else:
            continue_maybe("Unit tests execution FAILED. Do you want to continue with the merge anyway?")

    # Offer to run findbugs and rat before committing
    result = raw_input('Do you want to validate findbugs and rat after the merge? (y/n): ')
    if result.lower() == 'y':
        test_res = subprocess.call('mvn clean install -DskipTests findbugs:check apache-rat:check'.split())
        if test_res == 0:
            print('QA tests execution succeeded')
        else:
            continue_maybe("QA tests execution FAILED. Do you want to continue with the merge anyway?")

    commit_authors = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                             '--pretty=format:%an <%ae>']).split("\n")
    distinct_authors = sorted(set(commit_authors),
                              key=lambda x: commit_authors.count(x), reverse=True)
    primary_author = raw_input(
        "Enter primary author in the format of \"name <email>\" [%s]: " %
        distinct_authors[0])
    if primary_author == "":
        primary_author = distinct_authors[0]

    reviewers = raw_input("Enter reviewers [%s]: " % default_pr_reviewers).strip()
    if reviewers == '':
        reviewers = default_pr_reviewers

    commits = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                      '--pretty=format:%h [%an] %s']).split("\n")
    
    if len(commits) > 1:
        result = raw_input("List pull request commits in squashed commit message? (y/n): ")
        if result.lower() == "y":
          should_list_commits = True
        else:
          should_list_commits = False
    else:
        should_list_commits = False

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    if body is not None:
        # We remove @ symbols from the body to avoid triggering e-mails
        # to people every time someone creates a public fork of the project.
        merge_message_flags += ["-m", body.replace("@", "")]

    authors = "\n".join(["Author: %s" % a for a in distinct_authors])

    merge_message_flags += ["-m", authors]

    if (reviewers != ""):
        merge_message_flags += ["-m", "Reviewers: %s" % reviewers]

    if had_conflicts:
        committer_name = run_cmd("git config --get user.name").strip()
        committer_email = run_cmd("git config --get user.email").strip()
        message = "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>" % (
            committer_name, committer_email)
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    close_line = "This closes #%s from %s" % (pr_num, pr_repo_desc)
    # Find the github issues to close
    github_issues = re.findall("#[0-9]{3,6}", title)

    if len(github_issues) != 0:
        for issue_id in github_issues:
            close_line += ", closes %s" % (issue_id)

    if should_list_commits:
        close_line += " and squashes the following commits:"
    merge_message_flags += ["-m", close_line]

    if should_list_commits:
        merge_message_flags += ["-m", "\n".join(commits)]

    run_cmd(['git', 'commit', '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe("Merge complete (local ref %s). Push to %s?" % (
        target_branch_name, PUSH_REMOTE_NAME))

    try:
        run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, target_branch_name, target_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
    merge_log = run_cmd("git show --format=fuller -q %s" % target_branch_name)
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash, merge_log

def ask_for_branch(default_branch):
    pick_ref = raw_input("Enter a branch name [%s]: " % default_branch)
    if pick_ref == "":
        pick_ref = default_branch
    return pick_ref

def cherry_pick(pr_num, merge_hash, pick_ref):
    pick_branch_name = "%s_PICK_PR_%s_%s" % (TEMP_BRANCH_PREFIX, pr_num, pick_ref.upper())

    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
    run_cmd("git checkout %s" % pick_branch_name)

    try:
        run_cmd("git cherry-pick -sx %s" % merge_hash)
    except Exception as e:
        msg = "Error cherry-picking: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and finish the cherry-pick. Finished?"
        continue_maybe(msg)

    continue_maybe("Pick complete (local ref %s). Push to %s?" % (
        pick_branch_name, PUSH_REMOTE_NAME))

    try:
        run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, pick_branch_name, pick_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    pick_hash = run_cmd("git rev-parse %s" % pick_branch_name)[:8]
    clean_up()

    print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
    print("Pick hash: %s" % pick_hash)
    return pick_ref


def fix_version_from_branch(branch, versions):
    # Note: Assumes this is a sorted (newest->oldest) list of un-released versions
    if branch == DEV_BRANCH_NAME:
        versions = filter(lambda x: x == DEFAULT_FIX_VERSION, versions)
        if len(versions) > 0:
            return versions[0]
        else:
            return None
    else:
        versions = filter(lambda x: x.startswith(branch), versions)
        if len(versions) > 0:
            return versions[-1]
        else:
            return None


def resolve_jira_issue(merge_branches, comment, jira_id):
    asf_jira = jira.client.JIRA({'server': JIRA_API_BASE},
                                basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))

    result = raw_input("Resolve JIRA %s ? (y/n): " % jira_id)
    if result.lower() != "y":
        return

    try:
        issue = asf_jira.issue(jira_id)
    except Exception as e:
        fail("ASF JIRA could not find %s\n%s" % (jira_id, e))

    cur_status = issue.fields.status.name
    cur_summary = issue.fields.summary
    cur_assignee = issue.fields.assignee
    if cur_assignee is None:
        cur_assignee = "NOT ASSIGNED!!!"
    else:
        cur_assignee = cur_assignee.displayName

    if cur_status == "Resolved" or cur_status == "Closed":
        fail("JIRA issue %s already has status '%s'" % (jira_id, cur_status))
    print ("=== JIRA %s ===" % jira_id)
    print ("summary\t\t%s\nassignee\t%s\nstatus\t\t%s\nurl\t\t%s/%s\n" % (
        cur_summary, cur_assignee, cur_status, JIRA_BASE, jira_id))

    versions = asf_jira.project_versions(CAPITALIZED_PROJECT_NAME)
    versions = sorted(versions, key=lambda x: x.name, reverse=True)
    versions = filter(lambda x: x.raw['released'] is False, versions)

    version_names = map(lambda x: x.name, versions)
    default_fix_versions = map(lambda x: fix_version_from_branch(x, version_names), merge_branches)
    default_fix_versions = filter(lambda x: x != None, default_fix_versions)
    default_fix_versions = ",".join(default_fix_versions)

    fix_versions = raw_input("Enter comma-separated fix version(s) [%s]: " % default_fix_versions)
    if fix_versions == "":
        fix_versions = default_fix_versions
    fix_versions = fix_versions.replace(" ", "").split(",")

    def get_version_json(version_str):
        return filter(lambda v: v.name == version_str, versions)[0].raw

    jira_fix_versions = map(lambda v: get_version_json(v), fix_versions)

    resolve = filter(lambda a: a['name'] == "Resolve Issue", asf_jira.transitions(jira_id))[0]
    resolution = filter(lambda r: r.raw['name'] == "Fixed", asf_jira.resolutions())[0]
    asf_jira.transition_issue(
        jira_id, resolve["id"], fixVersions = jira_fix_versions,
        comment = comment, resolution = {'id': resolution.raw['id']})

    print "Successfully resolved %s with fixVersions=%s!" % (jira_id, fix_versions)


def resolve_jira_issues(title, merge_branches, comment):
    jira_ids = re.findall("%s-[0-9]{3,6}" % CAPITALIZED_PROJECT_NAME, title)

    if len(jira_ids) == 0:
        print "No JIRA issue found to update"
    for jira_id in jira_ids:
        resolve_jira_issue(merge_branches, comment, jira_id)


def standardize_jira_ref(text):
    """
    Standardize the jira reference commit message prefix to "PROJECT_NAME-XXX: Issue"

    'BOOKKEEPER-877: Script for generating patch for reviews'
    'ISSUE #376: Script for generating patch for reviews'
    """
    jira_refs = []
    github_issue_refs = []
    github_issue_ids = []
    components = []

    # Extract JIRA ref(s):
    pattern = re.compile(r'(%s[-\s]*[0-9]{3,6})+' % CAPITALIZED_PROJECT_NAME, re.IGNORECASE)
    for ref in pattern.findall(text):
        # Add brackets, replace spaces with a dash, & convert to uppercase
        jira_refs.append(re.sub(r'\s+', '-', ref.upper()))
        text = text.replace(ref, '')

    # Extract Github Issue ref(s)
    pattern = re.compile(r'(%s[-\s]*([0-9]{3,6}))+' % GITHUB_ISSUES_NAME, re.IGNORECASE)
    for ref in pattern.findall(text):
        # Add brackets, replace spaces or a dash with ' #', & convert to uppercase
        github_issue_refs.append(re.sub(r'[-\s]+', ' #', ref[0].upper()))
        text = text.replace(ref[0], '')
        github_issue_ids.append(ref[1].upper())

    # Extract project name component(s):
    # Look for alphanumeric chars, spaces, dashes, periods, and/or commas
    pattern = re.compile(r'(\[[\w\s,-\.]+\])', re.IGNORECASE)
    for component in pattern.findall(text):
        components.append(component.upper())
        text = text.replace(component, '')

    # Cleanup any remaining symbols:
    pattern = re.compile(r'^\W+(.*)', re.IGNORECASE)
    if (pattern.search(text) is not None):
        text = pattern.search(text).groups()[0]

    # Assemble full text (JIRA ref(s), module(s), remaining text)
    prefix = ''
    jira_prefix = ' '.join(jira_refs).strip()
    if jira_prefix:
        prefix = jira_prefix + ": "
    github_prefix = ' '.join(github_issue_refs).strip()
    if github_prefix:
        prefix = github_prefix + ": "
    clean_text = prefix + ' '.join(components).strip() + " " + text.strip()

    # Replace multiple spaces with a single space, e.g. if no jira refs and/or components were included
    clean_text = re.sub(r'\s+', ' ', clean_text.strip())

    return clean_text, github_issue_ids

def get_reviewers(pr_num):
    """
    Get a candidate list of reviewers that have commented on the PR with '+1' or 'LGTM'
    """
    approval_msgs = ['+1', 'lgtm']

    pr_comments = get_json("%s/issues/%s/comments" % (GITHUB_API_BASE, pr_num))

    reviewers_ids = set()
    for comment in pr_comments:
        for approval_msg in approval_msgs:
            if approval_msg in comment['body'].lower():
                reviewers_ids.add(comment['user']['login'])

    approval_review_states = ['approved']
    pr_reviews = get_json('{0}/pulls/{1}/reviews'.format(GITHUB_API_BASE, pr_num), True)
    for review in pr_reviews:
        for approval_state in approval_review_states:
            if approval_state in review['state'].lower():
                reviewers_ids.add(review['user']['login'])

    if len(reviewers_ids) == 0:
        fail("No approvals found in this pull request")

    reviewers_emails = []
    for reviewer_id in reviewers_ids:
        username = None
        useremail = None
        user = get_json("%s/users/%s" % (GITHUB_API_URL, reviewer_id))
        if user['email']:
            useremail = user['email'].strip()
        if user['name']:
            username = user['name'].strip()
        if username is None:
            continue
        reviewers_emails.append('{0} <{1}>'.format(username.encode('utf8'), useremail))
    return ', '.join(reviewers_emails)

def ask_release_for_github_issues(branch, labels):
    print "=== Add release to github issues ==="
    while True:
        fix_releases = ask_for_labels("release/%s" % branch, labels, [])
        if len(fix_releases) != 1:
            print "Please choose only one release to add for branch '%s'." % branch
            continue

        print "=== Apply following releases to github issues ==" 
        print "Fix Releases: %s" % ', '.join(fix_releases)
        print ""

        if raw_input("Would you like to add these releases to github issues? (y/n): ") == "y":
            break
    return fix_releases

def ask_updates_for_github_issues(milestones, labels, issue_labels):
    while True:
        fix_milestone, fix_milestone_number, fix_areas, fix_types = \
            get_updates_for_github_issues(milestones, labels, issue_labels)

        print "=== Apply following milestone, area, type to github issues ==" 
        print "Fix Types: %s" % ', '.join(fix_types)
        print "Fix Areas: %s" % ', '.join(fix_areas)
        print "Fix Milestone: %s" % fix_milestone
        print ""

        if raw_input("Would you like to update github issues with these labels? (y/n): ") == "y":
            break

    return fix_milestone, fix_milestone_number, fix_areas, fix_types

def get_updates_for_github_issues(milestones, labels, issue_labels):
    # get milestone
    default_milestone_name = milestones[0]['title']
    milestone_list = map(lambda x: x['title'], milestones)
    milestone_map = dict((milestone['title'], milestone['number']) for milestone in milestones)
    fix_milestone = ""
    while True:
        fix_milestone = raw_input("Choose fix milestone : options are [%s] - default: [%s]: " % (', '.join(milestone_list).strip(), default_milestone_name))
        fix_milestone = fix_milestone.strip()
        if fix_milestone == "":
            fix_milestone = default_milestone_name
            break
        elif fix_milestone in milestone_map:
            break
        else:
            print "Invalid milestone: %s." % fix_milestone

    # get area
    fix_areas = ask_for_labels("area/", labels, issue_labels)
    
    # get types
    fix_types = ask_for_labels("type/", labels, issue_labels) 

    return fix_milestone, milestone_map[fix_milestone], fix_areas, fix_types

def ask_for_labels(prefix, labels, issue_labels):
    issue_filtered_labels = map(lambda l: l.split('/')[1], filter(lambda x: x.startswith(prefix), issue_labels))
    filtered_labels = map(lambda l: l.split('/')[1], filter(lambda x: x.startswith(prefix), labels))
    while True:
        fix_labels = raw_input("Choose label '%s' - options are: [%s] - default: [%s] (comma separated): "
            % (prefix, ', '.join(filtered_labels).strip(), ', '.join(issue_filtered_labels).strip()))
        if fix_labels == "":
            if not issue_filtered_labels:
                print "Please specify a '%s' label to close the issue!" % prefix
                continue
            else:
                fix_labels = issue_filtered_labels
                break
        fix_labels = fix_labels.replace(" ", "").split(",")
        if not fix_labels:
            print "Please specify a '%s' label to close the issue!" % prefix
            continue
        invalid_label = False
        for label in fix_labels:
            if label not in filtered_labels:
                print "Invalid '%s' label: %s." % (prefix, label)
                invalid_label = True
                break
        if invalid_label:
            continue
        else:
            break
    return fix_labels

def get_github_issue_url(github_issue_id):
    return "https://api.github.com/repos/%s/%s/issues/%s" % (GITHUB_USER, PROJECT_NAME, github_issue_id) 

def get_assignees_url(github_issue_id):
    return "https://api.github.com/repos/%s/%s/issues/%s/assignees" % (GITHUB_USER, PROJECT_NAME, github_issue_id) 

def get_github_issue_labels(github_issue_id):
    url = "https://api.github.com/repos/%s/%s/issues/%s/labels" % (GITHUB_USER, PROJECT_NAME, github_issue_id) 
    result = get_json(url)
    return map(lambda x: x["name"], result)

def add_release_to_github_issues(github_issue_ids, labels, fix_release):
    for github_issue_id in github_issue_ids:
        labels = add_release_to_github_issue(github_issue_id, labels, fix_release)
    return labels

def add_release_to_github_issue(github_issue_id, labels, fix_release):
    url = get_github_issue_url(github_issue_id)
    labels = ["release/%s" % fix_release] + labels
    data = json.dumps({
        'labels': labels
    })
    post_json(url, data)
    return labels

def update_github_issue(github_issue_id, fix_milestone_number, fix_milestone, fix_areas, fix_types, other_labels):
    url = get_github_issue_url(github_issue_id)
    labels = other_labels + map(lambda x: "area/%s" % x, fix_areas)
    labels = labels + map(lambda x: "type/%s" % x, fix_types)
    labels.append("release/%s" % fix_milestone)
    data = json.dumps({
        'milestone': int(fix_milestone_number),
        'labels': labels,
    })
    post_json(url, data)
    return labels

def update_github_issues(github_issue_ids, fix_milestone_number, fix_milestone, fix_areas, fix_types, other_labels):
    for github_issue_id in github_issue_ids:
        labels = update_github_issue(github_issue_id, fix_milestone_number, fix_milestone, fix_areas, fix_types, other_labels)
    return labels

def add_assignees_to_github_issues(github_issue_ids, assignees):
    for github_issue_id in github_issue_ids:
        add_assignees_to_github_issue(github_issue_id, assignees)

def add_assignees_to_github_issue(github_issue_id, assignees):
    url = get_assignees_url(github_issue_id)
    data = json.dumps({
        "assignees": assignees
    })
    post_json(url, data)

def main():
    global original_head

    if not GITHUB_OAUTH_KEY:
        print "OAuth key is needed for merging bookkeeper pull requests."
        print "If environment variable 'GITHUB_OAUTH_KEY' is not defined,"
        print "then requests will be unauthenticated."
        print "You can create an OAuth key at https://github.com/settings/tokens"
        print "and set it to the environment variable 'GITHUB_OAUTH_KEY'."
        print "(This token only needs the 'public_repo' scope permissions)"
        exit(-1)

    # 0. get the current state so we can go back
    original_head = get_current_branch()

    # 1. retrieve milestones, labels, branches
    milestones = get_milestones()
    labels = get_all_labels()
    branches = get_json("%s/branches" % GITHUB_API_BASE)
    branch_names = filter(lambda x: x.startswith(RELEASE_BRANCH_PREFIX), [x['name'] for x in branches])
    # Assumes branch names can be sorted lexicographically
    latest_branch = sorted(branch_names, reverse=True)[0]

    # 2. retrieve the details for a given pull request
    pr_num = raw_input("Which pull request would you like to merge? (e.g. 34): ")
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))
    pr_reviewers = get_reviewers(pr_num)
    url = pr["url"]

    # 3. repare the title for commit message
    pr_title = pr["title"]
    commit_title = raw_input("Commit title [%s]: " % pr_title.encode("utf-8")).decode("utf-8")
    if commit_title == "":
        commit_title = pr_title

    # Decide whether to use the modified title or not
    modified_title, github_issue_ids = standardize_jira_ref(commit_title)
    if modified_title != commit_title:
        print "I've re-written the title as follows to match the standard format:"
        print "Original: %s" % commit_title
        print "Modified: %s" % modified_title
        result = raw_input("Would you like to use the modified title? (y/n): ")
        if result.lower() == "y":
            commit_title = modified_title
            print "Using modified title:"
        else:
            print "Using original title:"
        print commit_title

    body = pr["body"]
    modified_body = ""
    for line in body.split('\n'):
        if line.startswith('>'):
            continue
        modified_body = modified_body + line + "\n"
    if modified_body != body:
        print "I've re-written the body as follows to match the standard formats:"
        print "Original: "
        print body
        print "Modified: "
        print modified_body
        result = raw_input("Would you like to use the modified body? (y/n): ")
        if result.lower() == "y":
            body = modified_body
            print "Using modified body."
        else:
            print "Using original body."

    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)

    # append pr num to the github issues - we need to attach label and milestone to them
    github_issue_ids.append(pr_num)

    #
    # 4. attach milestone, area, type and release to github issues
    #

    # get issue labels
    issue_labels = get_github_issue_labels(pr_num)
    # ask for fix milestone, area and type
    fix_milestone, fix_milestone_number, fix_areas, fix_types = \
        ask_updates_for_github_issues(milestones, labels, issue_labels)
    # update issues with fix milestone, are and type
    other_labels = filter(lambda x: not x.startswith("area"), issue_labels)
    all_issue_labels = update_github_issues( \
        github_issue_ids, \
        fix_milestone_number, \
        fix_milestone, \
        fix_areas, \
        fix_types, \
        other_labels)
    # add the pr author to the assignees
    add_assignees_to_github_issues(github_issue_ids, [ user_login ])

    #
    # 5. Process the merge
    #

    # Merged pull requests don't appear as merged in the GitHub API;
    # Instead, they're closed by asfgit.
    merge_commits = \
        [e for e in pr_events if e["actor"]["login"] == "asfgit" and e["event"] == "closed"]

    if merge_commits:
        merge_hash = merge_commits[0]["commit_id"]
        message = get_json("%s/commits/%s" % (GITHUB_API_BASE, merge_hash))["commit"]["message"]

        print "Pull request %s has already been merged, assuming you want to backport" % pr_num
        commit_is_downloaded = run_cmd(['git', 'rev-parse', '--quiet', '--verify',
                                    "%s^{commit}" % merge_hash]).strip() != ""
        if not commit_is_downloaded:
            fail("Couldn't find any merge commit for #%s, you may need to update HEAD." % pr_num)

        print "Found commit %s:\n%s" % (merge_hash, message)
        
        cherry_pick(pr_num, merge_hash, ask_for_branch(latest_branch))
        sys.exit(0)

    if not bool(pr["mergeable"]):
        msg = "Pull request %s is not mergeable in its current form.\n" % pr_num + \
            "Continue? (experts only!)"
        continue_maybe(msg)

    print ("\n=== Pull Request #%s ===" % pr_num)
    print ("PR title\t%s\nCommit title\t%s\nSource\t\t%s\nTarget\t\t%s\nURL\t\t%s" % (
        pr_title, commit_title, pr_repo_desc, target_ref, url))
    continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    merged_refs = [target_ref]
    # proceed with the merge
    merge_hash, merge_commit_log = merge_pr(pr_num, target_ref, commit_title, body, pr_reviewers, pr_repo_desc)

    pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
    while raw_input("\n%s (y/n): " % pick_prompt).lower() == "y":
        pick_ref = ask_for_branch(latest_branch) 
        branch_version = pick_ref.split('-')[1]
        # add releases
        fix_releases = ask_release_for_github_issues(branch_version, labels)
        if len(fix_releases) > 0:
            all_issue_labels = add_release_to_github_issues(github_issue_ids, all_issue_labels, fix_releases[0])
        merged_refs = merged_refs + [cherry_pick(pr_num, merge_hash, pick_ref)]

    if JIRA_IMPORTED:
        if JIRA_USERNAME and JIRA_PASSWORD:
            jira_comment = '''Issue resolved by merging pull request %s
            [%s/%s]

            {noformat}
            %s
            {noformat}
            ''' % (pr_num, GITHUB_BASE, pr_num, merge_commit_log)
            resolve_jira_issues(commit_title, merged_refs, jira_comment)
        else:
            print "JIRA_USERNAME and JIRA_PASSWORD not set"
            print "Exiting without trying to close the associated JIRA."
    else:
        print "Could not find jira-python library. Run 'sudo pip install jira' to install."
        print "Exiting without trying to close the associated JIRA."

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if (failure_count):
        exit(-1)

    main()
