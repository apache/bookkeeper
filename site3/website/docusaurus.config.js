// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');
const baseUrl = process.env.BASE_URL || "/"
const deployUrl = process.env.DEPLOY_URL || "https://bookkeeper.apache.org";
const variables = {
  /** They are used in .md files*/
  latest_release: "4.16.3",
  stable_release: "4.14.8",
  github_repo: "https://github.com/apache/bookkeeper",
  github_master: "https://github.com/apache/bookkeeper/tree/master",
  mirror_base_url: "https://www.apache.org/dyn/closer.lua/bookkeeper",
  dist_base_url: "https://www.apache.org/dist/bookkeeper",
  javadoc_base_url: deployUrl + "/docs/latest/api/javadoc",
  archive_releases_base_url: deployUrl + "/archives",
}

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Apache BookKeeper',
  url: deployUrl,
  baseUrl,
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'apache',
  projectName: 'bookkeeper',
  plugins: ['docusaurus-plugin-sass'],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.json'),
          breadcrumbs: false
        },
        blog: {
          showReadingTime: true,
        },
        theme: {
          customCss: require.resolve('./src/sass/index.scss'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Apache BookKeeper',
        logo: {
          alt: 'Apache Bookkeeper',
          src: 'img/bk-logo.svg',
        },
        items: [
          {
            type: 'doc',
            docId: 'overview/overview',
            position: 'left',
            label: 'Documentation',
          },
          {
            position: 'left',
            label: 'Community',
            items: [
              {
                label: "Mailing lists",
                to: "community/mailing-lists"
              },
              {
                label: "Slack",
                to: "community/slack"
              },
              {
                label: "Github issues",
                href: "https://github.com/apache/bookkeeper/issues"
              },
              {
                label: "Release management",
                to: "community/releases"
              },
              {
                label: "Community meetings",
                to: "community/meeting"
              },
              {
                label: "Contribution guide",
                to: "community/contributing"
              },
              {
                label: "Coding guide",
                to: "community/coding-guide"
              },  
              {
                label: "Testing guide",
                to: "community/testing"
              },  
              {
                label: "Issue report guide",
                to: "community/issue-report"
              },  
              {
                label: "Release guide",
                to: "community/release-guide"
              },  
              {
                label: "Presentations",
                to: "community/presentations"
              },  
              {
                label: "BookKeeper proposals (BP)",
                to: "community/bookkeeper-proposals"
              },  
            ]
          },
          {
            position: 'left',
            label: 'Project',
            items: [
              {
                label: "Who are we?",
                to: "project/who"
              },
              {
                label: "Bylaws",
                to: "project/bylaws"
              },
              {
                label: "License",
                href: "https://apache.org/licenses"
              },
              {
                label: "Privacy policy",
                to: "project/privacy"
              },
              {
                label: "Sponsorship",
                href: "https://www.apache.org/foundation/sponsorship.html"
              },
              {
                label: "Thanks",
                href: "https://www.apache.org/foundation/thanks.html"
              }
            ]
          },
          {
            type: 'docsVersionDropdown',
            position: 'right'
          },
          {
            to: "releases",
            label: 'Download',
            position: 'right'
          }
        ],
      },
      footer: {
        style: 'dark',
        copyright: `<footer class="footer">
        <div class="container">
          <div class="content has-text-centered">
            <p>
              Copyright &copy; 2016 - ${new Date().getFullYear()} <a href="https://www.apache.org/">The Apache Software Foundation</a>,<br /> licensed under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache License, version 2.0</a>.
            </p>
            <p>
              Apache BookKeeper, BookKeeper®, Apache®, the Apache feature logo, and the Apache BookKeeper logo are either registered trademarks or trademarks of The Apache Software Foundation.
            </p>
          </div>
        </div>
      </footer>
      `,
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: "Overview",
              to: 'docs/overview',
            },
            {
              label: "Getting started",
              to: 'docs/getting-started/installation',
            },
            {
              label: "Deployment",
              to: 'docs/deployment/manual',
            },
            {
              label: "Administration",
              to: 'docs/admin/bookies',
            },
            {
              label: "API",
              to: 'docs/api/overview',
            },
            {
              label: "Security",
              to: 'docs/security/overview',
            },
            {
              label: "Development",
              to: 'docs/development/protocol',
            },
            {
              label: "Reference",
              to: 'docs/reference/config',
            },

          ]
        },
        {
          title: 'Community',
          items: [
            {
              label: "Mailing lists",
              to: "community/mailing-lists"
            },
            {
              label: "Slack",
              to: "community/slack"
            },
            {
              label: "Github",
              href: "https://github.com/apache/bookkeeper"
            },
            {
              label: "Twitter",
              href: "https://twitter.com/asfbookkeeper"
            }
          ]
        },
        {
          
          title: 'Project',
          items: [
            {
              label: "Who are we?",
              to: "project/who"
            },
            {
              label: "Bylaws",
              to: "project/bylaws"
            },
            {
              label: "License",
              href: "https://apache.org/licenses"
            },
            {
              label: "Privacy policy",
              to: "project/privacy"
            },
            {
              label: "Sponsorship",
              href: "https://www.apache.org/foundation/sponsorship.html"
            },
            {
              label: "Thanks",
              href: "https://www.apache.org/foundation/thanks.html"
            }
          ]
        }
     
      ],
    },

      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      
    }),
    customFields: variables
};

module.exports = config;

