"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[3210],{15680:(e,t,o)=>{o.d(t,{xA:()=>c,yg:()=>y});var n=o(96540);function r(e,t,o){return t in e?Object.defineProperty(e,t,{value:o,enumerable:!0,configurable:!0,writable:!0}):e[t]=o,e}function i(e,t){var o=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),o.push.apply(o,n)}return o}function a(e){for(var t=1;t<arguments.length;t++){var o=null!=arguments[t]?arguments[t]:{};t%2?i(Object(o),!0).forEach((function(t){r(e,t,o[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(o)):i(Object(o)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(o,t))}))}return e}function l(e,t){if(null==e)return{};var o,n,r=function(e,t){if(null==e)return{};var o,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)o=i[n],t.indexOf(o)>=0||(r[o]=e[o]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)o=i[n],t.indexOf(o)>=0||Object.prototype.propertyIsEnumerable.call(e,o)&&(r[o]=e[o])}return r}var s=n.createContext({}),p=function(e){var t=n.useContext(s),o=t;return e&&(o="function"==typeof e?e(t):a(a({},t),e)),o},c=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},g=n.forwardRef((function(e,t){var o=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),u=p(o),g=r,y=u["".concat(s,".").concat(g)]||u[g]||d[g]||i;return o?n.createElement(y,a(a({ref:t},c),{},{components:o})):n.createElement(y,a({ref:t},c))}));function y(e,t){var o=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=o.length,a=new Array(i);a[0]=g;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:r,a[1]=l;for(var p=2;p<i;p++)a[p]=o[p];return n.createElement.apply(null,a)}return n.createElement.apply(null,o)}g.displayName="MDXCreateElement"},59264:(e,t,o)=>{o.r(t),o.d(t,{assets:()=>s,contentTitle:()=>a,default:()=>d,frontMatter:()=>i,metadata:()=>l,toc:()=>p});var n=o(9668),r=(o(96540),o(15680));const i={id:"zookeeper",title:"ZooKeeper Authentication"},a=void 0,l={unversionedId:"security/zookeeper",id:"version-4.16.6/security/zookeeper",title:"ZooKeeper Authentication",description:"New Clusters",source:"@site/versioned_docs/version-4.16.6/security/zookeeper.md",sourceDirName:"security",slug:"/security/zookeeper",permalink:"/docs/4.16.6/security/zookeeper",draft:!1,tags:[],version:"4.16.6",frontMatter:{id:"zookeeper",title:"ZooKeeper Authentication"},sidebar:"docsSidebar",previous:{title:"Authentication using SASL",permalink:"/docs/4.16.6/security/sasl"},next:{title:"The BookKeeper protocol",permalink:"/docs/4.16.6/development/protocol"}},s={},p=[{value:"New Clusters",id:"new-clusters",level:2},{value:"Migrating Clusters",id:"migrating-clusters",level:2},{value:"Migrating the ZooKeeper ensemble",id:"migrating-the-zookeeper-ensemble",level:2}],c={toc:p},u="wrapper";function d(e){let{components:t,...o}=e;return(0,r.yg)(u,(0,n.A)({},c,o,{components:t,mdxType:"MDXLayout"}),(0,r.yg)("h2",{id:"new-clusters"},"New Clusters"),(0,r.yg)("p",null,"To enable ",(0,r.yg)("inlineCode",{parentName:"p"},"ZooKeeper")," authentication on Bookies or Clients, there are two necessary steps:"),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},"Create a ",(0,r.yg)("inlineCode",{parentName:"li"},"JAAS")," login file and set the appropriate system property to point to it as described in ",(0,r.yg)("a",{parentName:"li",href:"sasl#notes"},"GSSAPI (Kerberos)"),"."),(0,r.yg)("li",{parentName:"ol"},"Set the configuration property ",(0,r.yg)("inlineCode",{parentName:"li"},"zkEnableSecurity")," in each bookie to ",(0,r.yg)("inlineCode",{parentName:"li"},"true"),".")),(0,r.yg)("p",null,"The metadata stored in ",(0,r.yg)("inlineCode",{parentName:"p"},"ZooKeeper")," is such that only certain clients will be able to modify and read the corresponding znodes.\nThe rationale behind this decision is that the data stored in ZooKeeper is not sensitive, but inappropriate manipulation of znodes can cause cluster\ndisruption."),(0,r.yg)("h2",{id:"migrating-clusters"},"Migrating Clusters"),(0,r.yg)("p",null,"If you are running a version of BookKeeper that does not support security or simply with security disabled, and you want to make the cluster secure,\nthen you need to execute the following steps to enable ZooKeeper authentication with minimal disruption to your operations."),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},"Perform a rolling restart setting the ",(0,r.yg)("inlineCode",{parentName:"li"},"JAAS")," login file, which enables bookie or clients to authenticate. At the end of the rolling restart,\nbookies (or clients) are able to manipulate znodes with strict ACLs, but they will not create znodes with those ACLs."),(0,r.yg)("li",{parentName:"ol"},"Perform a second rolling restart of bookies, this time setting the configuration parameter ",(0,r.yg)("inlineCode",{parentName:"li"},"zkEnableSecurity")," to true, which enables the use\nof secure ACLs when creating znodes."),(0,r.yg)("li",{parentName:"ol"},"Currently we don't have provide a tool to set acls on old znodes. You are recommended to set it manually using ZooKeeper tools.")),(0,r.yg)("p",null,"It is also possible to turn off authentication in a secured cluster. To do it, follow these steps:"),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},"Perform a rolling restart of bookies setting the ",(0,r.yg)("inlineCode",{parentName:"li"},"JAAS")," login file, which enable bookies to authenticate, but setting ",(0,r.yg)("inlineCode",{parentName:"li"},"zkEnableSecurity")," to ",(0,r.yg)("inlineCode",{parentName:"li"},"false"),".\nAt the end of rolling restart, bookies stop creating znodes with secure ACLs, but are still able to authenticate and manipulate all znodes."),(0,r.yg)("li",{parentName:"ol"},"You can use ZooKeeper tools to manually reset all ACLs under the znode set in ",(0,r.yg)("inlineCode",{parentName:"li"},"zkLedgersRootPath"),", which defaults to ",(0,r.yg)("inlineCode",{parentName:"li"},"/ledgers"),"."),(0,r.yg)("li",{parentName:"ol"},"Perform a second rolling restart of bookies, this time omitting the system property that sets the ",(0,r.yg)("inlineCode",{parentName:"li"},"JAAS")," login file.")),(0,r.yg)("h2",{id:"migrating-the-zookeeper-ensemble"},"Migrating the ZooKeeper ensemble"),(0,r.yg)("p",null,"It is also necessary to enable authentication on the ",(0,r.yg)("inlineCode",{parentName:"p"},"ZooKeeper")," ensemble. To do it, we need to perform a rolling restart of the ensemble and\nset a few properties. Please refer to the ZooKeeper documentation for more details."),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},(0,r.yg)("a",{parentName:"li",href:"http://zookeeper.apache.org/doc/r3.4.6/zookeeperProgrammers.html#sc_ZooKeeperAccessControl"},"Apache ZooKeeper Documentation")),(0,r.yg)("li",{parentName:"ol"},(0,r.yg)("a",{parentName:"li",href:"https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL"},"Apache ZooKeeper Wiki"))))}d.isMDXComponent=!0}}]);