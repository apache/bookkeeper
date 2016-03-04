#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# copy from {@link https://github.com/apache/kafka/blob/trunk/kafka-patch-review.py}

import argparse
import sys
import os
import time
import datetime
import tempfile
import commands
import getpass
from jira.client import JIRA

def get_jira_config():
  # read the config file
  home=jira_home=os.getenv('HOME')
  home=home.rstrip('/')
  if not (os.path.isfile(home + '/jira.ini')):
    jira_user=raw_input('JIRA user :')
    jira_pass=getpass.getpass('JIRA password :')
    jira_config = {'user':jira_user, 'password':jira_pass}
    return jira_config
  else:
    jira_config = dict(line.strip().split('=') for line in open(home + '/jira.ini'))
    return jira_config

def get_jira(jira_config):
  options = {
    'server': 'https://issues.apache.org/jira'
  }
  jira = JIRA(options=options,basic_auth=(jira_config['user'], jira_config['password']))
  # (Force) verify the auth was really done
  jira_session=jira.session()
  if (jira_session is None):
    raise Exception("Failed to login to the JIRA instance")
  return jira

def cmd_exists(cmd):
  status, result = commands.getstatusoutput(cmd)
  return status

def main():
  ''' main(), shut up, pylint '''
  popt = argparse.ArgumentParser(description='BookKeeper patch review tool')
  popt.add_argument('-b', '--branch', action='store', dest='branch', required=True, help='Tracking branch to create diff against')
  popt.add_argument('-j', '--jira', action='store', dest='jira', required=True, help='JIRA corresponding to the reviewboard')
  popt.add_argument('-s', '--summary', action='store', dest='summary', required=False, help='Summary for the reviewboard')
  popt.add_argument('-d', '--description', action='store', dest='description', required=False, help='Description for reviewboard')
  popt.add_argument('-r', '--rb', action='store', dest='reviewboard', required=False, help='Review board that needs to be updated')
  popt.add_argument('-t', '--testing-done', action='store', dest='testing', required=False, help='Text for the Testing Done section of the reviewboard')
  popt.add_argument('-db', '--debug', action='store_true', required=False, help='Enable debug mode')
  opt = popt.parse_args()

  post_review_tool = None
  if (cmd_exists("post-review") == 0):
    post_review_tool = "post-review"
  elif (cmd_exists("rbt") == 0):
    post_review_tool = "rbt post"
  else:
    print "please install RBTools. See https://www.reviewboard.org/docs/rbtools/dev/ for details."
    sys.exit(1)

  patch_file=tempfile.gettempdir() + "/" + opt.jira + ".patch"
  if opt.reviewboard:
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
    patch_file=tempfile.gettempdir() + "/" + opt.jira + '_' + st + '.patch'

  # first check if rebase is needed
  git_branch_hash="git rev-parse " + opt.branch
  p_now=os.popen(git_branch_hash)
  branch_now=p_now.read()
  p_now.close()

  git_common_ancestor="git merge-base " + opt.branch + " HEAD"
  p_then=os.popen(git_common_ancestor)
  branch_then=p_then.read()
  p_then.close()

  if branch_now != branch_then:
    print 'ERROR: Your current working branch is from an older version of ' + opt.branch + '. Please rebase first by using git pull --rebase'
    sys.exit(1)

  git_configure_reviewboard="git config reviewboard.url https://reviews.apache.org"
  print "Configuring reviewboard url to https://reviews.apache.org"
  p=os.popen(git_configure_reviewboard)
  p.close()

  git_remote_update="git remote update"
  print "Updating your remote branches to pull the latest changes"
  p=os.popen(git_remote_update)
  p.close()

  # Get JIRA configuration and login to JIRA to ensure the credentials work, before publishing the patch to the review board
  print "Verifying JIRA connection configurations"
  try:
    jira_config=get_jira_config()
    jira=get_jira(jira_config)
  except:
    print "Failed to login to the JIRA instance", sys.exc_info()[0], sys.exc_info()[1]
    sys.exit(1)

  git_command="git diff --no-prefix " + opt.branch + " > " + patch_file
  if opt.debug:
    print git_command
  p=os.popen(git_command)
  p.close()

  print 'Getting latest patch attached to the JIRA'
  tmp_dir = tempfile.mkdtemp()
  get_latest_patch_command="""
PATCHFILE={0}/{1}.patch
jiraPage={0}/jira.txt
curl "https://issues.apache.org/jira/browse/{1}" > {0}/jira.txt
if [[ `grep -c 'Patch Available' {0}/jira.txt` == 0 ]] ; then
    echo "{1} is not \"Patch Available\". Exiting."
    echo
    exit 1
fi
relativePatchURL=`grep -o '"/jira/secure/attachment/[0-9]*/[^"]*' {0}/jira.txt \
    | grep -v -e 'htm[l]*$' | sort | tail -1 \
    | grep -o '/jira/secure/attachment/[0-9]*/[^"]*'`
patchURL="https://issues.apache.org$relativePatchURL"
curl $patchURL > {0}/{1}.patch
""".format(tmp_dir, opt.jira)
  p=os.popen(get_latest_patch_command)
  p.close()

  previous_patch=tmp_dir + "/" + opt.jira + ".patch"
  diff_file=tmp_dir + "/" + opt.jira + ".diff"
  if os.path.isfile(previous_patch) and os.stat(previous_patch).st_size > 0:
    print 'Creating diff with previous version of patch uploaded to JIRA'
    diff_command = "diff " + previous_patch+ " " + patch_file + " > " + diff_file
    try:
      p=os.popen(diff_command)
      sys.stdout.flush()
      p.close()
    except:
      pass
    print 'Diff with previous version of patch uploaded to JIRA is saved to ' + diff_file

    print 'Checking if the there are changes that need to be pushed'
    if os.stat(diff_file).st_size == 0:
      print 'No changes found on top of changes uploaded to JIRA'
      print 'Aborting'
      sys.exit(1)

  rb_command= post_review_tool + " --publish --tracking-branch " + opt.branch + " --target-groups=bookkeeper --bugs-closed=" + opt.jira
  if opt.debug:
    rb_command=rb_command + " --debug"
  summary="Patch for " + opt.jira
  if opt.summary:
    summary=opt.summary
  rb_command=rb_command + " --summary \"" + summary + "\""
  if opt.description:
    rb_command=rb_command + " --description \"" + opt.description + "\""
  if opt.reviewboard:
    rb_command=rb_command + " -r " + opt.reviewboard
  if opt.testing:
    rb_command=rb_command + " --testing-done=" + opt.testing
  if opt.debug:
    print rb_command
  p=os.popen(rb_command)
  rb_url=""
  for line in p:
    print line
    if line.startswith('http'):
      rb_url = line
    elif line.startswith("There don't seem to be any diffs"):
      print 'ERROR: Your reviewboard was not created/updated since there was no diff to upload. The reasons that can cause this issue are 1) Your diff is not checked into your local branch. Please check in the diff to the local branch and retry 2) You are not specifying the local branch name as part of the --branch option. Please specify the remote branch name obtained from git branch -r'
      p.close()
      sys.exit(1)
    elif line.startswith("Your review request still exists, but the diff is not attached") and not opt.debug:
      print 'ERROR: Your reviewboard was not created/updated. Please run the script with the --debug option to troubleshoot the problem'
      p.close()
      sys.exit(1)
  if p.close() != None:
    print 'ERROR: reviewboard update failed. Exiting.'
    sys.exit(1)
  if opt.debug:
    print 'rb url=',rb_url

  print 'Creating diff against', opt.branch, 'and uploading patch to JIRA',opt.jira
  issue = jira.issue(opt.jira)
  attachment=open(patch_file)
  jira.add_attachment(issue,attachment)
  attachment.close()

  comment="Created reviewboard "
  if not opt.reviewboard:
    print 'Created a new reviewboard',rb_url,
  else:
    print 'Updated reviewboard',rb_url
    comment="Updated reviewboard "

  comment = comment + rb_url + ' against branch ' + opt.branch
  jira.add_comment(opt.jira, comment)

  #update the JIRA status to PATCH AVAILABLE
  transitions = jira.transitions(issue)
  transitionsMap ={}

  for t in transitions:
    transitionsMap[t['name']] = t['id']

  if('Submit Patch' in transitionsMap):
     jira.transition_issue(issue, transitionsMap['Submit Patch'] , assignee={'name': jira_config['user']} )

if __name__ == '__main__':
  sys.exit(main())
