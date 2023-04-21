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
#

import slacker
import yaml
import time
import re
import datetime
import smtplib
from email.mime.text import MIMEText


conf = yaml.load(open('configuration.yaml'))


def send_digest(channel, address, digest):
    msg = MIMEText(digest, _charset='utf-8')
    msg['From'] = conf['mail']['fromAddress']
    msg['To'] = address
    msg['Subject'] = 'Slack digest for #%s - %s' % (
        channel, datetime.datetime.now().strftime('%Y-%m-%d'))
    server = smtplib.SMTP(conf['mail']['smtp'])
    if conf['mail']['useTLS']:
        server.starttls()
    if 'username' in conf['mail']:
        server.login(conf['mail']['username'], conf['mail']['password'])

    server.sendmail(conf['mail']['fromAddress'], address, msg.as_string())
    server.quit()


slack = slacker.Slacker(conf['slack']['token'])

channels = slack.channels.list().body['channels']

# Get a mapping between Slack internal user ids and real names
users = {}
for user in slack.users.list().body['members']:
    real_name = user.get('real_name', user.get('name'))
    users[user['id']] = real_name

last_day_timestamp = time.time() - (24 * 3600)

for channel in channels:
    id = channel['id']
    name = channel['name']
    topic = channel['topic']['value']

    if name not in conf['channels']:
        print('Ignoring channel: #%s' % name)
        continue

    toAddress = conf['channels'][name]
    print('Getting digest of #%s --> %s' % (name, toAddress))

    messages = slack.channels.history(channel=id,
                                      oldest=last_day_timestamp,
                                      count=1000)
    digest = ''
    for m in reversed(messages.body['messages']):
        if not m['type'] == 'message':
            continue

        user = m.get('user')
        if not user:
            user = m['comment']['user']
        sender = users.get(user, '')

        date = datetime.datetime.utcfromtimestamp(float(m['ts'])).strftime('%Y-%m-%d %H:%M:%S UTC')
        # Replace users id mentions with real names
        text = re.sub(r'<@(\w+)>', lambda m: '@' + users[m.group(1)], m['text'])

        digest += '%s - %s: %s\n' % (date, sender, text)
        for reaction in m.get('reactions', []):
            digest += '%s : %s\n' % (reaction['name'], ', '.join(map(users.get, reaction['users'])))
        digest += '----\n'

    if digest:
        send_digest(name, toAddress, digest)
