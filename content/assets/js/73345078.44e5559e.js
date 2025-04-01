"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[390],{15680:(e,r,t)=>{t.d(r,{xA:()=>d,yg:()=>m});var a=t(96540);function i(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function o(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);r&&(a=a.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,a)}return t}function n(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?o(Object(t),!0).forEach((function(r){i(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function s(e,r){if(null==e)return{};var t,a,i=function(e,r){if(null==e)return{};var t,a,i={},o=Object.keys(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||(i[t]=e[t]);return i}(e,r);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(i[t]=e[t])}return i}var l=a.createContext({}),g=function(e){var r=a.useContext(l),t=r;return e&&(t="function"==typeof e?e(r):n(n({},r),e)),t},d=function(e){var r=g(e.components);return a.createElement(l.Provider,{value:r},e.children)},p="mdxType",c={inlineCode:"code",wrapper:function(e){var r=e.children;return a.createElement(a.Fragment,{},r)}},u=a.forwardRef((function(e,r){var t=e.components,i=e.mdxType,o=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),p=g(t),u=i,m=p["".concat(l,".").concat(u)]||p[u]||c[u]||o;return t?a.createElement(m,n(n({ref:r},d),{},{components:t})):a.createElement(m,n({ref:r},d))}));function m(e,r){var t=arguments,i=r&&r.mdxType;if("string"==typeof e||i){var o=t.length,n=new Array(o);n[0]=u;var s={};for(var l in r)hasOwnProperty.call(r,l)&&(s[l]=r[l]);s.originalType=e,s[p]="string"==typeof e?e:i,n[1]=s;for(var g=2;g<o;g++)n[g]=t[g];return a.createElement.apply(null,n)}return a.createElement.apply(null,t)}u.displayName="MDXCreateElement"},6452:(e,r,t)=>{t.r(r),t.d(r,{assets:()=>l,contentTitle:()=>n,default:()=>c,frontMatter:()=>o,metadata:()=>s,toc:()=>g});var a=t(58168),i=(t(96540),t(15680));const o={id:"distributedlog-api",title:"DistributedLog"},n=void 0,s={unversionedId:"api/distributedlog-api",id:"version-4.17.1/api/distributedlog-api",title:"DistributedLog",description:"DistributedLog began its life as a separate project under the Apache Foundation. It was merged into BookKeeper in 2017.",source:"@site/versioned_docs/version-4.17.1/api/distributedlog-api.md",sourceDirName:"api",slug:"/api/distributedlog-api",permalink:"/docs/api/distributedlog-api",draft:!1,tags:[],version:"4.17.1",frontMatter:{id:"distributedlog-api",title:"DistributedLog"},sidebar:"docsSidebar",previous:{title:"The Advanced Ledger API",permalink:"/docs/api/ledger-adv-api"},next:{title:"BookKeeper Security",permalink:"/docs/security/overview"}},l={},g=[{value:"Architecture",id:"architecture",level:2},{value:"Logs",id:"logs",level:2},{value:"Log records",id:"log-records",level:3},{value:"Log segments",id:"log-segments",level:3},{value:"Namespaces",id:"namespaces",level:3},{value:"Writers",id:"writers",level:2},{value:"Write Proxy",id:"write-proxy",level:3},{value:"Readers",id:"readers",level:2},{value:"Read Proxy",id:"read-proxy",level:3},{value:"Guarantees",id:"guarantees",level:2},{value:"API",id:"api",level:2}],d={toc:g},p="wrapper";function c(e){let{components:r,...o}=e;return(0,i.yg)(p,(0,a.A)({},d,o,{components:r,mdxType:"MDXLayout"}),(0,i.yg)("blockquote",null,(0,i.yg)("p",{parentName:"blockquote"},"DistributedLog began its life as a separate project under the Apache Foundation. It was merged into BookKeeper in 2017.")),(0,i.yg)("p",null,"The DistributedLog API is an easy-to-use interface for managing BookKeeper entries that enables you to use BookKeeper without needing to interact with ",(0,i.yg)("a",{parentName:"p",href:"ledger-api"},"ledgers")," directly."),(0,i.yg)("p",null,"DistributedLog (DL) maintains sequences of records in categories called ",(0,i.yg)("em",{parentName:"p"},"logs")," (aka ",(0,i.yg)("em",{parentName:"p"},"log streams"),"). ",(0,i.yg)("em",{parentName:"p"},"Writers")," append records to DL logs, while ",(0,i.yg)("em",{parentName:"p"},"readers")," fetch and process those records."),(0,i.yg)("h2",{id:"architecture"},"Architecture"),(0,i.yg)("p",null,"The diagram below illustrates how the DistributedLog API works with BookKeeper:"),(0,i.yg)("p",null,(0,i.yg)("img",{alt:"DistributedLog API",src:t(55679).A,width:"818",height:"441"})),(0,i.yg)("h2",{id:"logs"},"Logs"),(0,i.yg)("p",null,"A ",(0,i.yg)("em",{parentName:"p"},"log")," in DistributedLog is an ordered, immutable sequence of ",(0,i.yg)("em",{parentName:"p"},"log records"),"."),(0,i.yg)("p",null,"The diagram below illustrates the anatomy of a log stream:"),(0,i.yg)("p",null,(0,i.yg)("img",{alt:"DistributedLog log",src:t(72057).A,width:"620",height:"516"})),(0,i.yg)("h3",{id:"log-records"},"Log records"),(0,i.yg)("p",null,"Each log record is a sequence of bytes. Applications are responsible for serializing and deserializing byte sequences stored in log records."),(0,i.yg)("p",null,"Log records are written sequentially into a ",(0,i.yg)("em",{parentName:"p"},"log stream")," and assigned with a a unique sequence number called a DLSN (",(0,i.yg)("strong",null,"D"),"istributed",(0,i.yg)("strong",null,"L"),"og ",(0,i.yg)("strong",null,"S"),"equence ",(0,i.yg)("strong",null,"N"),"umber)."),(0,i.yg)("p",null,"In addition to a DLSN, applications can assign their own sequence number when constructing log records. Application-defined sequence numbers are known as ",(0,i.yg)("em",{parentName:"p"},"TransactionIDs")," (or ",(0,i.yg)("em",{parentName:"p"},"txid"),"). Either a DLSN or a TransactionID can be used for positioning readers to start reading from a specific log record."),(0,i.yg)("h3",{id:"log-segments"},"Log segments"),(0,i.yg)("p",null,"Each log is broken down into ",(0,i.yg)("em",{parentName:"p"},"log segments")," that contain subsets of records. Log segments are distributed and stored in BookKeeper. DistributedLog rolls the log segments based on the configured ",(0,i.yg)("em",{parentName:"p"},"rolling policy"),", which be either"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"a configurable period of time (such as every 2 hours), or"),(0,i.yg)("li",{parentName:"ul"},"a configurable maximum size (such as every 128 MB).")),(0,i.yg)("p",null,"The data in logs is divided up into equally sized log segments and distributed evenly across bookies. This allows logs to scale beyond a size that would fit on a single server and spreads read traffic across the cluster."),(0,i.yg)("h3",{id:"namespaces"},"Namespaces"),(0,i.yg)("p",null,"Log streams that belong to the same organization are typically categorized and managed under a ",(0,i.yg)("em",{parentName:"p"},"namespace"),". DistributedLog namespaces essentially enable applications to locate log streams. Applications can perform the following actions under a namespace:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"create streams"),(0,i.yg)("li",{parentName:"ul"},"delete streams"),(0,i.yg)("li",{parentName:"ul"},"truncate streams to a given sequence number (either a DLSN or a TransactionID)")),(0,i.yg)("h2",{id:"writers"},"Writers"),(0,i.yg)("p",null,"Through the DistributedLog API, writers write data into logs of their choice. All records are appended into logs in order. The sequencing is performed by the writer, which means that there is only one active writer for a log at any given time."),(0,i.yg)("p",null,"DistributedLog guarantees correctness when two writers attempt to write to the same log when a network partition occurs using a ",(0,i.yg)("em",{parentName:"p"},"fencing")," mechanism in the log segment store."),(0,i.yg)("h3",{id:"write-proxy"},"Write Proxy"),(0,i.yg)("p",null,"Log writers are served and managed in a service tier called the ",(0,i.yg)("em",{parentName:"p"},"Write Proxy")," (see the diagram ",(0,i.yg)("a",{parentName:"p",href:"#architecture"},"above"),"). The Write Proxy is used for accepting writes from a large number of clients."),(0,i.yg)("h2",{id:"readers"},"Readers"),(0,i.yg)("p",null,"DistributedLog readers read records from logs of their choice, starting with a provided position. The provided position can be either a DLSN or a TransactionID."),(0,i.yg)("p",null,"Readers read records from logs in strict order. Different readers can read records from different positions in the same log."),(0,i.yg)("p",null,"Unlike other pub-sub systems, DistributedLog doesn't record or manage readers' positions. This means that tracking is the responsibility of applications, as different applications may have different requirements for tracking and coordinating positions. This is hard to get right with a single approach. Distributed databases, for example, might store reader positions along with SSTables, so they would resume applying transactions from the positions store in SSTables. Tracking reader positions could easily be done at the application level using various stores (such as ZooKeeper, the filesystem, or key-value stores)."),(0,i.yg)("h3",{id:"read-proxy"},"Read Proxy"),(0,i.yg)("p",null,"Log records can be cached in a service tier called the ",(0,i.yg)("em",{parentName:"p"},"Read Proxy")," to serve a large number of readers. See the diagram ",(0,i.yg)("a",{parentName:"p",href:"#architecture"},"above"),". The Read Proxy is the analogue of the ",(0,i.yg)("a",{parentName:"p",href:"#write-proxy"},"Write Proxy"),"."),(0,i.yg)("h2",{id:"guarantees"},"Guarantees"),(0,i.yg)("p",null,"The DistributedLog API for BookKeeper provides a number of guarantees for applications:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"Records written by a ",(0,i.yg)("a",{parentName:"li",href:"#writers"},"writer")," to a ",(0,i.yg)("a",{parentName:"li",href:"#logs"},"log")," are appended in the order in which they are written. If a record ",(0,i.yg)("strong",{parentName:"li"},"R1")," is written by the same writer as a record ",(0,i.yg)("strong",{parentName:"li"},"R2"),", ",(0,i.yg)("strong",{parentName:"li"},"R1")," will have a smaller sequence number than ",(0,i.yg)("strong",{parentName:"li"},"R2"),"."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("a",{parentName:"li",href:"#readers"},"Readers")," see ",(0,i.yg)("a",{parentName:"li",href:"#log-records"},"records")," in the same order in which they are ",(0,i.yg)("a",{parentName:"li",href:"#writers"},"written")," to the log."),(0,i.yg)("li",{parentName:"ul"},"All records are persisted on disk by BookKeeper before acknowledgements, which guarantees durability."),(0,i.yg)("li",{parentName:"ul"},"For a log with a replication factor of N, DistributedLog tolerates up to N-1 server failures without losing any records.")),(0,i.yg)("h2",{id:"api"},"API"),(0,i.yg)("p",null,"Documentation for the DistributedLog API can be found ",(0,i.yg)("a",{parentName:"p",href:"https://bookkeeper.apache.org/docs/next/api/distributedlog-api"},"here"),"."),(0,i.yg)("blockquote",null,(0,i.yg)("p",{parentName:"blockquote"},"At a later date, the DistributedLog API docs will be added here.")))}c.isMDXComponent=!0},55679:(e,r,t)=>{t.d(r,{A:()=>a});const a=t.p+"assets/images/distributedlog-e72b5c54b4a5ca53e33a6740bb2b4242.png"},72057:(e,r,t)=>{t.d(r,{A:()=>a});const a=t.p+"assets/images/logs-4fa7115af12e41a46d64d9e300847af4.png"}}]);