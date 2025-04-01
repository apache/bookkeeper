"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[694],{15680:(e,t,r)=>{r.d(t,{xA:()=>s,yg:()=>g});var n=r(96540);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var p=n.createContext({}),u=function(e){var t=n.useContext(p),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},s=function(e){var t=u(e.components);return n.createElement(p.Provider,{value:t},e.children)},c="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},y=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,a=e.originalType,p=e.parentName,s=i(e,["components","mdxType","originalType","parentName"]),c=u(r),y=o,g=c["".concat(p,".").concat(y)]||c[y]||m[y]||a;return r?n.createElement(g,l(l({ref:t},s),{},{components:r})):n.createElement(g,l({ref:t},s))}));function g(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=r.length,l=new Array(a);l[0]=y;var i={};for(var p in t)hasOwnProperty.call(t,p)&&(i[p]=t[p]);i.originalType=e,i[c]="string"==typeof e?e:o,l[1]=i;for(var u=2;u<a;u++)l[u]=r[u];return n.createElement.apply(null,l)}return n.createElement.apply(null,r)}y.displayName="MDXCreateElement"},76572:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>p,contentTitle:()=>l,default:()=>m,frontMatter:()=>a,metadata:()=>i,toc:()=>u});var n=r(58168),o=(r(96540),r(15680));const a={id:"manual",title:"Manual deployment"},l=void 0,i={unversionedId:"deployment/manual",id:"version-4.12.1/deployment/manual",title:"Manual deployment",description:"A BookKeeper cluster consists of two main components:",source:"@site/versioned_docs/version-4.12.1/deployment/manual.md",sourceDirName:"deployment",slug:"/deployment/manual",permalink:"/docs/4.12.1/deployment/manual",draft:!1,tags:[],version:"4.12.1",frontMatter:{id:"manual",title:"Manual deployment"},sidebar:"version-4.12.1/docsSidebar",previous:{title:"BookKeeper concepts and architecture",permalink:"/docs/4.12.1/getting-started/concepts"},next:{title:"Deploying Apache BookKeeper on Kubernetes",permalink:"/docs/4.12.1/deployment/kubernetes"}},p={},u=[{value:"ZooKeeper setup",id:"zookeeper-setup",level:2},{value:"Cluster metadata setup",id:"cluster-metadata-setup",level:2},{value:"Starting up bookies",id:"starting-up-bookies",level:2},{value:"System requirements",id:"system-requirements",level:3}],s={toc:u},c="wrapper";function m(e){let{components:t,...r}=e;return(0,o.yg)(c,(0,n.A)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,o.yg)("p",null,"A BookKeeper cluster consists of two main components:"),(0,o.yg)("ul",null,(0,o.yg)("li",{parentName:"ul"},"A ",(0,o.yg)("a",{parentName:"li",href:"#zookeeper-setup"},"ZooKeeper")," cluster that is used for configuration- and coordination-related tasks"),(0,o.yg)("li",{parentName:"ul"},"An ",(0,o.yg)("a",{parentName:"li",href:"#starting-up-bookies"},"ensemble")," of bookies")),(0,o.yg)("h2",{id:"zookeeper-setup"},"ZooKeeper setup"),(0,o.yg)("p",null,"We won't provide a full guide to setting up a ZooKeeper cluster here. We recommend that you consult ",(0,o.yg)("a",{parentName:"p",href:"https://zookeeper.apache.org/doc/current/zookeeperAdmin.html"},"this guide")," in the official ZooKeeper documentation."),(0,o.yg)("h2",{id:"cluster-metadata-setup"},"Cluster metadata setup"),(0,o.yg)("p",null,"Once your ZooKeeper cluster is up and running, there is some metadata that needs to be written to ZooKeeper, so you need to modify the bookie's configuration to make sure that it points to the right ZooKeeper cluster."),(0,o.yg)("p",null,"On each bookie host, you need to ",(0,o.yg)("a",{parentName:"p",href:"../getting-started/installation#download"},"download")," the BookKeeper package as a tarball. Once you've done that, you need to configure the bookie by setting values in the ",(0,o.yg)("inlineCode",{parentName:"p"},"bookkeeper-server/conf/bk_server.conf")," config file. The one parameter that you will absolutely need to change is the ",(0,o.yg)("inlineCode",{parentName:"p"},"zkServers")," parameter, which you will need to set to the ZooKeeper connection string for your ZooKeeper cluster. Here's an example:"),(0,o.yg)("pre",null,(0,o.yg)("code",{parentName:"pre",className:"language-properties"},"zkServers=100.0.0.1:2181,100.0.0.2:2181,100.0.0.3:2181\n")),(0,o.yg)("blockquote",null,(0,o.yg)("p",{parentName:"blockquote"},"A full listing of configurable parameters available in ",(0,o.yg)("inlineCode",{parentName:"p"},"bookkeeper-server/conf/bk_server.conf")," can be found in the ",(0,o.yg)("a",{parentName:"p",href:"../reference/config"},"Configuration")," reference manual.")),(0,o.yg)("p",null,"Once the bookie's configuration is set, you can set up cluster metadata for the cluster by running the following command from any bookie in the cluster:"),(0,o.yg)("pre",null,(0,o.yg)("code",{parentName:"pre",className:"language-shell"},"$ bookkeeper-server/bin/bookkeeper shell metaformat\n")),(0,o.yg)("p",null,"You can run in the formatting "),(0,o.yg)("blockquote",null,(0,o.yg)("p",{parentName:"blockquote"},"The ",(0,o.yg)("inlineCode",{parentName:"p"},"metaformat")," command performs all the necessary ZooKeeper cluster metadata tasks and thus only needs to be run ",(0,o.yg)("em",{parentName:"p"},"once")," and from ",(0,o.yg)("em",{parentName:"p"},"any")," bookie in the BookKeeper cluster.")),(0,o.yg)("p",null,"Once cluster metadata formatting has been completed, your BookKeeper cluster is ready to go!"),(0,o.yg)("h2",{id:"starting-up-bookies"},"Starting up bookies"),(0,o.yg)("p",null,"Before you start up your bookies, you should make sure that all bookie hosts have the correct configuration, then you can start up as many bookies as you'd like to form a cluster by using the ",(0,o.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-bookie"},(0,o.yg)("inlineCode",{parentName:"a"},"bookie"))," command of the ",(0,o.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper"},(0,o.yg)("inlineCode",{parentName:"a"},"bookkeeper"))," CLI tool:"),(0,o.yg)("pre",null,(0,o.yg)("code",{parentName:"pre",className:"language-shell"},"$ bookkeeper-server/bin/bookkeeper bookie\n")),(0,o.yg)("h3",{id:"system-requirements"},"System requirements"),(0,o.yg)("p",null,"The number of bookies you should run in a BookKeeper cluster depends on the quorum mode that you've chosen, the desired throughput, and the number of clients using the cluster simultaneously."),(0,o.yg)("table",null,(0,o.yg)("thead",{parentName:"table"},(0,o.yg)("tr",{parentName:"thead"},(0,o.yg)("th",{parentName:"tr",align:"left"},"Quorum type"),(0,o.yg)("th",{parentName:"tr",align:"left"},"Number of bookies"))),(0,o.yg)("tbody",{parentName:"table"},(0,o.yg)("tr",{parentName:"tbody"},(0,o.yg)("td",{parentName:"tr",align:"left"},"Self-verifying quorum"),(0,o.yg)("td",{parentName:"tr",align:"left"},"3")),(0,o.yg)("tr",{parentName:"tbody"},(0,o.yg)("td",{parentName:"tr",align:"left"},"Generic"),(0,o.yg)("td",{parentName:"tr",align:"left"},"4")))),(0,o.yg)("p",null,"Increasing the number of bookies will enable higher throughput, and there is ",(0,o.yg)("strong",{parentName:"p"},"no upper limit")," on the number of bookies."))}m.isMDXComponent=!0}}]);