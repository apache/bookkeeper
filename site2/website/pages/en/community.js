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
      content: `Learn more about the [license.](http://www.apache.org/licenses/)`,
      title: 'Apache License',
    },
    {
      content: 'Ask questions about the documentation and project',
      title: 'Join the community',
    },
    {
      content: "Learn more about our [sponsors.](http://www.apache.org/licenses/)",
      title: 'Sponsorship',
    },
  ];

  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer documentContainer postContainer">
        <div className="post">
          <header className="postHeader">
            <h1>Need help?</h1>
          </header>
          <p>This project is maintained by a dedicated group of people.</p>
          <GridBlock contents={supportLinks} layout="threeColumn" />
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
    <tr>
      <td>breed</td>
      <td>Ben Reed</td>
      <td>Facebook</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>drusek</td>
      <td>Dave Rusek</td>
      <td>Twitter</td>
      <td>-7</td>
    </tr>
    <tr>
      <td>eolivelli</td>
      <td>Enrico Olivelli</td>
      <td>Diennea</td>
      <td>+2</td>
    </tr>
    <tr>
      <td>fcuny</td>
      <td>Franck Cuny</td>
      <td>Twitter</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>fpj</td>
      <td>Flavio Junqueira</td>
      <td>Pravega</td>
      <td>+1</td>
    </tr>
    <tr>
      <td>hsaputra</td>
      <td>Henry Saputra</td>
      <td> </td>
      <td>-8</td>
    </tr>
    <tr>
      <td>ivank</td>
      <td>Ivan Kelly</td>
      <td>Splunk</td>
      <td>+2</td>
    </tr>
    <tr>
      <td>jiannan</td>
      <td>Jiannan Wang</td>
      <td>Yahoo Inc.</td>
      <td>+8</td>
    </tr>
    <tr>
      <td>jujjuri</td>
      <td>Venkateswararao (JV) Jujjuri</td>
      <td>Salesforce</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>lstewart</td>
      <td>Leigh Stewart</td>
      <td>Twitter</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>mmerli</td>
      <td>Matteo Merli</td>
      <td>Splunk</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>rakeshr</td>
      <td>Rakesh Radhakrishnan</td>
      <td>Huawei</td>
      <td>+5:30</td>
    </tr>
    <tr>
      <td>sijie</td>
      <td>Sijie Guo</td>
      <td>StreamNative</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>umamahesh</td>
      <td>Uma Maheswara Rao G</td>
      <td>Intel</td>
      <td>+5</td>
    </tr>
    <tr>
      <td>zhaijia</td>
      <td>Jia Zhai</td>
      <td>StreamNative</td>
      <td>+8</td>
    </tr>
    <tr>
      <td>rdhabalia</td>
      <td>Rajan Dhabalia</td>
      <td>Yahoo Inc.</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>reddycharan</td>
      <td>Charan Reddy Guttapalem</td>
      <td>Salesforce</td>
      <td>-8</td>
    </tr>
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
    <tr>
      <td>ayegorov</td>
      <td>Andrey Yegorov</td>
      <td>Salesforce</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>breed</td>
      <td>Ben Reed</td>
      <td>Facebook</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>drusek</td>
      <td>Dave Rusek</td>
      <td>Twitter</td>
      <td>-7</td>
    </tr>
    <tr>
      <td>eolivelli</td>
      <td>Enrico Olivelli</td>
      <td>Diennea</td>
      <td>+2</td>
    </tr>
    <tr>
      <td>fcuny</td>
      <td>Franck Cuny</td>
      <td>Twitter</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>fpj</td>
      <td>Flavio Junqueira</td>
      <td>Pravega</td>
      <td>+1</td>
    </tr>
    <tr>
      <td>hsaputra</td>
      <td>Henry Saputra</td>
      <td> </td>
      <td>-8</td>
    </tr>
    <tr>
      <td>ivank</td>
      <td>Ivan Kelly</td>
      <td>Splunk</td>
      <td>+2</td>
    </tr>
    <tr>
      <td>jiannan</td>
      <td>Jiannan Wang</td>
      <td>Yahoo Inc.</td>
      <td>+8</td>
    </tr>
    <tr>
      <td>jujjuri</td>
      <td>Venkateswararao (JV) Jujjuri</td>
      <td>Salesforce</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>mmerli</td>
      <td>Matteo Merli</td>
      <td>Splunk</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>rakeshr</td>
      <td>Rakesh Radhakrishnan</td>
      <td>Huawei</td>
      <td>+5:30</td>
    </tr>
    <tr>
      <td>reddycharan</td>
      <td>Charan Reddy G</td>
      <td>Salesforce</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>robindh</td>
      <td>Robin Dhamankar</td>
      <td>Facebook</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>sboobna</td>
      <td>Siddharth Boobna</td>
      <td>Salesforce</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>sijie</td>
      <td>Sijie Guo</td>
      <td>StreamNative</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>sjust</td>
      <td>Sam Just</td>
      <td>Salesforce</td>
      <td>-8</td>
    </tr>
    <tr>
      <td>umamahesh</td>
      <td>Uma Maheswara Rao G</td>
      <td>Intel</td>
      <td>+5</td>
    </tr>
    <tr>
      <td>zhaijia</td>
      <td>Jia Zhai</td>
      <td>StreamNative</td>
      <td>+8</td>
    </tr>
  </tbody>
</table>

        </div>
      </Container>
    </div>
  );
}

module.exports = Help;
