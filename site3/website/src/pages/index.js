import React from 'react';
import Layout from '@theme/Layout';
import Logo from '@site/static/img/bk-logo.svg';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';



export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  const getStartedHref = `docs/getting-started/installation`
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="A scalable, fault-tolerant, and low-latency storage service optimized for real-time workloads>">
          <div class="hero">
            <div class="container">
              <div class="row" style={{alignItems: 'center'}}>
                <div class="col col--6">
                  
                  <h1 class="hero__title">Apache BookKeeper</h1>
                  <p class="hero__subtitle">A scalable, fault-tolerant, and low-latency storage service optimized for real-time workloads</p>
                  <div>
                    <a href={getStartedHref}>
                      <button class="button button--primary button--lg margin-right--sm" href={getStartedHref}>
                        Get Started
                      </button>
                    </a>
                  </div>
                  </div>
                
                <div class="col col--6 padding--lg" >
                  <Logo height="100%" width="100%" />
                </div>
              </div>
            
            </div>
          </div>      
      
    </Layout>
  );
}
