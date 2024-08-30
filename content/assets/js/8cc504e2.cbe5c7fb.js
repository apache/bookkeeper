"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[4601],{15680:(e,t,r)=>{r.d(t,{xA:()=>d,yg:()=>y});var a=r(96540);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},i=Object.keys(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var l=a.createContext({}),p=function(e){var t=a.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},d=function(e){var t=p(e.components);return a.createElement(l.Provider,{value:t},e.children)},c="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,i=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),c=p(r),m=n,y=c["".concat(l,".").concat(m)]||c[m]||g[m]||i;return r?a.createElement(y,o(o({ref:t},d),{},{components:r})):a.createElement(y,o({ref:t},d))}));function y(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=r.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[c]="string"==typeof e?e:n,o[1]=s;for(var p=2;p<i;p++)o[p]=r[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,r)}m.displayName="MDXCreateElement"},63079:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>g,frontMatter:()=>i,metadata:()=>s,toc:()=>p});var a=r(9668),n=(r(96540),r(15680));const i={id:"metrics",title:"Metric collection"},o=void 0,s={unversionedId:"admin/metrics",id:"version-4.5.1/admin/metrics",title:"Metric collection",description:"BookKeeper enables metrics collection through a variety of stats providers.",source:"@site/versioned_docs/version-4.5.1/admin/metrics.md",sourceDirName:"admin",slug:"/admin/metrics",permalink:"/docs/4.5.1/admin/metrics",draft:!1,tags:[],version:"4.5.1",frontMatter:{id:"metrics",title:"Metric collection"},sidebar:"version-4.5.1/docsSidebar",previous:{title:"Using AutoRecovery",permalink:"/docs/4.5.1/admin/autorecovery"},next:{title:"Upgrade",permalink:"/docs/4.5.1/admin/upgrade"}},l={},p=[{value:"Stats providers",id:"stats-providers",level:2},{value:"Enabling stats providers in bookies",id:"enabling-stats-providers-in-bookies",level:2}],d={toc:p},c="wrapper";function g(e){let{components:t,...r}=e;return(0,n.yg)(c,(0,a.A)({},d,r,{components:t,mdxType:"MDXLayout"}),(0,n.yg)("p",null,"BookKeeper enables metrics collection through a variety of ",(0,n.yg)("a",{parentName:"p",href:"#stats-providers"},"stats providers"),"."),(0,n.yg)("h2",{id:"stats-providers"},"Stats providers"),(0,n.yg)("p",null,"BookKeeper has stats provider implementations for four five sinks:"),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:"left"},"Provider"),(0,n.yg)("th",{parentName:"tr",align:"left"},"Provider class name"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://mvnrepository.com/artifact/org.apache.bookkeeper.stats/codahale-metrics-provider"},"Codahale Metrics")),(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"org.apache.bookkeeper.stats.CodahaleMetricsProvider"))),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://prometheus.io/"},"Prometheus")),(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"org.apache.bookkeeper.stats.PrometheusMetricsProvider"))),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://twitter.github.io/finagle/guide/Metrics.html"},"Finagle")),(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"org.apache.bookkeeper.stats.FinagleStatsProvider"))),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/twitter/ostrich"},"Ostrich")),(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"org.apache.bookkeeper.stats.OstrichProvider"))),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://mvnrepository.com/artifact/org.apache.bookkeeper.stats/twitter-science-provider"},"Twitter Science Provider")),(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"org.apache.bookkeeper.stats.TwitterStatsProvider"))))),(0,n.yg)("blockquote",null,(0,n.yg)("p",{parentName:"blockquote"},"The ",(0,n.yg)("a",{parentName:"p",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats-providers/codahale-metrics-provider"},"Codahale Metrics")," stats provider is the default provider.")),(0,n.yg)("h2",{id:"enabling-stats-providers-in-bookies"},"Enabling stats providers in bookies"),(0,n.yg)("p",null,"Two stats-related ",(0,n.yg)("a",{parentName:"p",href:"../reference/config/"},"configuration parameters")," are available for bookies:"),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:"left"},"Parameter"),(0,n.yg)("th",{parentName:"tr",align:"left"},"Description"),(0,n.yg)("th",{parentName:"tr",align:"left"},"Default"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"enableStatistics")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Whether statistics are enabled for the bookie"),(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"false"))),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"statsProviderClass")),(0,n.yg)("td",{parentName:"tr",align:"left"},"The stats provider class used by the bookie"),(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"org.apache.bookkeeper.stats.CodahaleMetricsProvider"))))),(0,n.yg)("p",null,"To enable stats:"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},"set the ",(0,n.yg)("inlineCode",{parentName:"li"},"enableStatistics")," parameter to ",(0,n.yg)("inlineCode",{parentName:"li"},"true")),(0,n.yg)("li",{parentName:"ul"},"set ",(0,n.yg)("inlineCode",{parentName:"li"},"statsProviderClass")," to the desired provider (see the ",(0,n.yg)("a",{parentName:"li",href:"#stats-providers"},"table above")," for a listing of classes)")))}g.isMDXComponent=!0}}]);