/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

function Help(props) {
  const {config: siteConfig, language = ''} = props;
  const {baseUrl, docsUrl} = siteConfig;
  const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
  const langPart = `${language ? `${language}/` : ''}`;
  const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`;

  const supportLinks = [
    {
      content: `Subscribe to [Apache BookKeeper Mailing Lists](${docUrl('mailing-lists')}) to learn more about latest developments.`,
      title: "Mailing lists"
    },
    {
      content: `There is an Apache BookKeeper channel that is used for informal discussions for BookKeeper developers and users.  
      The Slack channel is at http://apachebookkeeper.slack.com/.  
      You can self-register at https://apachebookkeeper.herokuapp.com/.`,
      title: "Apache BookKeeper on Slack"
    },
    {
      content: `Learn more about the upcoming and on-going releases at [Apache BookKeeper Release Management.](${docUrl('release-management')})`,
      title: "Release Mangement"
    },
    {
      content: `Planning to contribute back to the project? Read our [Contribution Guide.](${docUrl('contributing')})`,
      title: "Contribution Guide"
    },
    {
      content: `Confused on which coding standard to pick? Read our [Coding Guide.](${docUrl('coding-guide')})`,
      title: "Coding Guide"
    },
    {
      content: `What to learn about how to write tests, which tests are appropriate where, and when do tests run. Read our [Testing Guide.](${docUrl('testing-guide')})`,
      title: "Testing Guide"
    },
    {
      content: `Found a bug or facing a functional issue? Refer our [Issue Report Guide](${docUrl('issue-report-guide')})`,
      title: "Issue Report Guide"
    },
    {
      content: `Interested in learning how Apache BookKeeper releases are managed? Checkout our [Release Guide.](${docUrl('release-guide')})`,
      title: "Release Guide"
    },
    {
      content: `Read more about this project at [Apache BookKeeper Papers And Presentations](${docUrl('papers-and-presentations')})`,
      title: "Papers And Presentations"
    },
    {
      content: `Have a new idea which might lead to a major change in the code-base? File a [BookKeeper Proposal](${docUrl('bookkeeper-proposals')})`,
      title: "BookKeeper Proposals"
    }
  ];

  const pmcDetails = [
      {
        "username": "breed",
        "name": "Ben Reed",
        "organization": "Facebook",
        "timezone": "-8"
      },
      {
        "username": "drusek",
        "name": "Dave Rusek",
        "organization": "Twitter",
        "timezone": "-7"
      },
      {
        "username": "eolivelli",
        "name": "Enrico Olivelli",
        "organization": "Diennea",
        "timezone": "+2"
      },
      {
        "username": "fcuny",
        "name": "Franck Cuny",
        "organization": "Twitter",
        "timezone": "-8"
      },
      {
        "username": "fpj",
        "name": "Flavio Junqueira",
        "organization": "Pravega",
        "timezone": "+1"
      },
      {
        "username": "hsaputra",
        "name": "Henry Saputra",
        "organization": "",
        "timezone": "-8"
      },
      {
        "username": "ivank",
        "name": "Ivan Kelly",
        "organization": "Splunk",
        "timezone": "+2"
      },
      {
        "username": "jiannan",
        "name": "Jiannan Wang",
        "organization": "Yahoo Inc.",
        "timezone": "+8"
      },
      {
        "username": "jujjuri",
        "name": "Venkateswararao (JV) Jujjuri",
        "organization": "Salesforce",
        "timezone": "-8"
      },
      {
        "username": "lstewart",
        "name": "Leigh Stewart",
        "organization": "Twitter",
        "timezone": "-8"
      },
      {
        "username": "mmerli",
        "name": "Matteo Merli",
        "organization": "Splunk",
        "timezone": "-8"        
      },
      {
        "username": "rakeshr",
        "name": "Rakesh Radhakrishnan",
        "organization": "Huawei",
        "timezone": "+5:30"     
      },
      {
        "username": "sijie",
        "name": "Sijie Guo",
        "organization": "StreamNative",
        "timezone": "-8"  
      },
      {
        "username": "umamahesh",
        "name": "Uma Maheswara Rao G",
        "organization": "Intel",
        "timezone": "+5"  
      },
      {
        "username": "zhaijia",
        "name": "Jia Zhai",
        "organization": "StreamNative",
        "timezone": "+8"  
      },
      {
        "username": "rdhabalia",
        "name": "Rajan Dhabalia",
        "organization": "Yahoo Inc.",
        "timezone": "-8"  
      },
      {
        "username": "reddycharan",
        "name": "Charan Reddy Guttapalem",
        "organization": "Salesforce",
        "timezone": "-8" 
      }
  ];

  const pmcDetailsRows = pmcDetails.map(function(row){
    return (
        <tr>
          <td>{row.username}</td>
          <td>{row.name}</td>
          <td>{row.organization}</td>
          <td>{row.timezone}</td>
        </tr>
      )
    });

  const committerDetails = [
    {
      "username": "ayegorov",
      "name": "Andrey Yegorov",
      "organization": "Salesforce",
      "timezone": "-8" 
    },
    {
      "username": "breed",
      "name": "Ben Reed",
      "organization": "Facebook",
      "timezone": "-8"
    },
    {
      "username": "drusek",
      "name": "Dave Rusek",
      "organization": "Twitter",
      "timezone": "-7"
    },
    {
      "username": "eolivelli",
      "name": "Enrico Olivelli",
      "organization": "Diennea",
      "timezone": "+2"
    },
    {
      "username": "fcuny",
      "name": "Franck Cuny",
      "organization": "Twitter",
      "timezone": "-8"
    },
    {
      "username": "fpj",
      "name": "Flavio Junqueira",
      "organization": "Pravega",
      "timezone": "+1"
    },
    {
      "username": "hsaputra",
      "name": "Henry Saputra",
      "organization": "",
      "timezone": "-8"
    },
    {
      "username": "ivank",
      "name": "Ivan Kelly",
      "organization": "Splunk",
      "timezone": "+2"
    },
    {
      "username": "jiannan",
      "name": "Jiannan Wang",
      "organization": "Yahoo Inc.",
      "timezone": "+8"
    },
    {
      "username": "jujjuri",
      "name": "Venkateswararao (JV) Jujjuri",
      "organization": "Salesforce",
      "timezone": "-8"
    },
    {
      "username": "mmerli",
      "name": "Matteo Merli",
      "organization": "Splunk",
      "timezone": "-8"
    },
    {
      "username": "rakeshr",
      "name": "Rakesh Radhakrishnan",
      "organization": "Huawei",
      "timezone": "+5:30"     
    },
    {
      "username": "reddycharan",
      "name": "Charan Reddy Guttapalem",
      "organization": "Salesforce",
      "timezone": "-8" 
    },
    {
      "username": "robindh",
      "name": "Robin Dhamankar",
      "organization": "Facebook",
      "timezone": "-8" 
    },
    {
      "username": "sboobna",
      "name": "Siddharth Boobna",
      "organization": "Salesforce",
      "timezone": "-8" 
    },
    {
      "username": "sijie",
      "name": "Sijie Guo",
      "organization": "StreamNative",
      "timezone": "-8" 
    },
    {
      "username": "sjust",
      "name": "Sam Just",
      "organization": "Salesforce",
      "timezone": "-8" 
    },
    {
      "username": "umamahesh",
      "name": "Uma Maheswara Rao G",
      "organization": "Intel",
      "timezone": "+5" 
    },
    {
      "username": "zhaijia",
      "name": "Jia Zhai",
      "organization": "StreamNative",
      "timezone": "+8" 
    }
  ];

  const committerDetailsRows = committerDetails.map(function(row){
    return (
        <tr>
          <td>{row.username}</td>
          <td>{row.name}</td>
          <td>{row.organization}</td>
          <td>{row.timezone}</td>
        </tr>
      )
    });
  
  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <GridBlock contents={supportLinks} layout="threeColumn"/>
            <h2 id="pmc">Project Management Committee (PMC)</h2>
            <p>BookKeeper’s PMC members are:</p>
            <table>
                <thead>
                  <tr>
                    <th>Username</th>
                    <th>Name</th>
                    <th>Organization</th>
                    <th>Timezone</th>
                  </tr>
                </thead>
                <tbody>
                  {pmcDetailsRows}
                </tbody>
            </table>
            <h2 id="committers">Committers</h2>
            <p>BookKeeper’s active committers are:</p>
            <table>
                <thead>
                <tr>
                    <th>Username</th>
                    <th>Name</th>
                    <th>Organization</th>
                    <th>Timezone</th>
                </tr>
                </thead>
                <tbody>
                  {committerDetailsRows}
                </tbody>
            </table>
          </div>
      </Container>
    </div>
  );
}

module.exports = Help;
