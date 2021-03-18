/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();
const siteConfig = require(`${CWD}/siteConfig.js`);
// const translate = require('../../server/translate.js').translate;
// const users = require(`${CWD}/data/users.js`)
// const featuredUsers = users.filter(x => x.hasOwnProperty('featured'))
// featuredUsers.sort((a, b) => (a.featured > b.featured) ? 1 : -1);

function imgUrl(img) {
    return siteConfig.baseUrl + 'img/' + img;
}

function docUrl(doc, language) {
    return siteConfig.baseUrl + 'docs/' + (language ? language + '/' : '') + doc;
}

function pageUrl(page, language) {
    return siteConfig.baseUrl + (language ? language + '/' : '') + page;
}

function githubUrl() {
    return siteConfig.githubUrl;
}

class HomeSplash extends React.Component {
    render() {
        let language = this.props.language || '';
        return (
            <SplashContainer>
                <Logo img_src={imgUrl('bk-header.png')}/>
                <div className="inner">
                    <ProjectTitle/>
                    <PromoSection>
                        <Button href={docUrl('getting-started', language)}>Read the docs</Button>
                        <Button href={githubUrl()}>GitHub</Button>
                    </PromoSection>
                </div>
            </SplashContainer>
        );
    }
}

const SplashContainer = props => (
    <div className="homeContainer">
        <div className="homeSplashFade" style={{marginTop: '5rem'}}>
            <div className="wrapper homeWrapper">{props.children}</div>
        </div>
    </div>
);

const Logo = props => (
    <div className="" style={{width: '500px', alignItems: 'center', margin: 'auto'}}>
        <img src={props.img_src} />
    </div>
);

const ProjectTitle = props => (
    <h2 className="projectTitle" style={{maxWidth:'1024px', margin: 'auto'}}>
        <small style={{color: 'black', fontSize: '2.0rem'}}>{siteConfig.projectDescription}</small>
    </h2>
);

const PromoSection = props => (
    <div className="section promoSection">
        <div className="promoRow">
            <div className="pluginRowBlock">{props.children}</div>
        </div>
    </div>
);

class Button extends React.Component {
    render() {
        return (
            <div className="pluginWrapper buttonWrapper">
                <a className="button" href={this.props.href} target={this.props.target}>
                    {this.props.children}
                </a>
            </div>
        );
    }
}

const KeyFeaturesGrid = props => (
    <Container
        padding={['bottom']}
        id={props.id}
        background={props.background}>
        <GridBlock align="center" contents={props.features.row1} layout="threeColumn" />
        <GridBlock align="center" contents={props.features.row2} layout="threeColumn" />
        <GridBlock align="center" contents={props.features.row3} layout="threeColumn" />
    </Container>
);

const features_lang = language => {
    return {
        row1: [
            {
                content: 'BookKeeper has run in production at Yahoo, Twitter and Salesforce at scale for over 3 years, with petabytes of data stored',
                title: `[Proven in production](${docUrl('concepts-architecture-overview', language)})`,
            },
            {
                content: 'Seamlessly expand capacity to hundreds of nodes',
                title: `[Horizontally scalable](${docUrl('concepts-architecture-overview', language)})`,
            },
            {
                content: 'Persistent message storage based on Apache BookKeeper. Provides IO-level isolation between write and read operations',
                title: `[Persistent storage](${docUrl('concepts-architecture-overview#persistent-storage', language)})`,
            },
        ],
        row2: [
            {
                content: 'Designed for low publish latency (< 5ms) at scale with strong durabilty guarantees',
                title: `[Low latency with durability](${docUrl('concepts-architecture-overview', language)})`,
            },
            {
                content: 'Designed for configurable replication between data centers across multiple geographic regions',
                title: `[Geo-replication](${docUrl('administration-geo', language)})`,
            },
            {
                content: 'Built from the ground up as a multi-tenant system. Supports Isolation, Authentication, Authorization and Quotas',
                title: `[Multi-tenancy](${docUrl('concepts-multi-tenancy', language)})`,
            }
        ]
    };
};

const UsersBlock = props => (
    <Container
        padding={['bottom']}
        id={props.id}
        background={props.background}>

        <p align="center"><small style={{color: 'black', fontSize: '1.7rem'}}>Used by companies such as</small></p>
        <div class="logo-wrapper">
            {
/* TODO: Ghatage/eolivelli: Confirm with companies if it is OK to use their logo for BookKeeper website.
                featuredUsers.map(
                    c => (
                        (() => {
                            if (c.hasOwnProperty('logo_white')) {
                                return <div className="logo-box-background-for-white">
                                    <a href={c.url} title={c.name} target="_blank">
                                        <img src={c.logo} alt={c.name} className={c.logo.endsWith('.svg') ? 'logo-svg' : ''}/>
                                    </a>
                                </div>
                            } else {
                                return <div className="logo-box">
                                    <a href={c.url} title={c.name} target="_blank">
                                        <img src={c.logo} alt={c.name} className={c.logo.endsWith('.svg') ? 'logo-svg' : ''}/>
                                    </a>
                                </div>
                            }
                        })()
                    )
                )
*/
	}
        </div>
        <p align="center"><small style={{color: 'black', fontSize: '1.7rem'}}>and many more</small></p>

    </Container>
);

const ApacheBlock = prop => (
    <Container>
        <div className="Block" style={{textAlign: 'center'}}>
            <p>
                Apache BookKeeper is available under the <a href="https://www.apache.org/licenses">Apache License, version 2.0</a>.
            </p>
        </div>
    </Container>
);

class Index extends React.Component {
    render() {
        let language = this.props.language || '';
        let features = features_lang(language);

        return (
            <div>
                <HomeSplash language={language} />
                <div className="mainContainer">
                    <KeyFeaturesGrid features={features} id={'key-features'} />
                    //<UsersBlock id={'users'} />
                    <ApacheBlock />
                </div>
            </div>
        );
    }
}

module.exports = Index;
