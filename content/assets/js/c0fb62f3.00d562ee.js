"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[7743],{3905:function(e,t,r){r.d(t,{Zo:function(){return p},kt:function(){return m}});var n=r(67294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function a(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var s=n.createContext({}),c=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):a(a({},t),e)),r},p=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,i=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=c(r),f=o,m=u["".concat(s,".").concat(f)]||u[f]||d[f]||i;return r?n.createElement(m,a(a({ref:t},p),{},{components:r})):n.createElement(m,a({ref:t},p))}));function m(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var i=r.length,a=new Array(i);a[0]=f;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:o,a[1]=l;for(var c=2;c<i;c++)a[c]=r[c];return n.createElement.apply(null,a)}return n.createElement.apply(null,r)}f.displayName="MDXCreateElement"},76043:function(e,t,r){r.r(t),r.d(t,{assets:function(){return s},contentTitle:function(){return a},default:function(){return d},frontMatter:function(){return i},metadata:function(){return l},toc:function(){return c}});var n=r(83117),o=(r(67294),r(3905));const i={id:"overview",title:"BookKeeper API"},a=void 0,l={unversionedId:"api/overview",id:"version-4.16.5/api/overview",title:"BookKeeper API",description:"BookKeeper offers a few APIs that applications can use to interact with it:",source:"@site/versioned_docs/version-4.16.5/api/overview.md",sourceDirName:"api",slug:"/api/overview",permalink:"/docs/4.16.5/api/overview",draft:!1,tags:[],version:"4.16.5",frontMatter:{id:"overview",title:"BookKeeper API"},sidebar:"docsSidebar",previous:{title:"Decommission Bookies",permalink:"/docs/4.16.5/admin/decomission"},next:{title:"The Ledger API",permalink:"/docs/4.16.5/api/ledger-api"}},s={},c=[{value:"Trade-offs",id:"trade-offs",level:2}],p={toc:c},u="wrapper";function d(e){let{components:t,...r}=e;return(0,o.kt)(u,(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"BookKeeper offers a few APIs that applications can use to interact with it:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"The ",(0,o.kt)("a",{parentName:"li",href:"ledger-api"},"ledger API")," is a lower-level API that enables you to interact with ledgers directly"),(0,o.kt)("li",{parentName:"ul"},"The ",(0,o.kt)("a",{parentName:"li",href:"ledger-adv-api"},"Ledger Advanced API")," is an advanced extension to ",(0,o.kt)("a",{parentName:"li",href:"ledger-api"},"Ledger API")," to provide more flexibilities to applications."),(0,o.kt)("li",{parentName:"ul"},"The ",(0,o.kt)("a",{parentName:"li",href:"distributedlog-api"},"DistributedLog API")," is a higher-level API that provides convenient abstractions.")),(0,o.kt)("h2",{id:"trade-offs"},"Trade-offs"),(0,o.kt)("p",null,"The ",(0,o.kt)("inlineCode",{parentName:"p"},"Ledger API")," provides direct access to ledgers and thus enables you to use BookKeeper however you'd like."),(0,o.kt)("p",null,"However, in most of use cases, if you want a ",(0,o.kt)("inlineCode",{parentName:"p"},"log stream"),"-like abstraction, it requires you to manage things like tracking list of ledgers,\nmanaging rolling ledgers and data retention on your own. In such cases, you are recommended to use ",(0,o.kt)("a",{parentName:"p",href:"distributedlog-api"},"DistributedLog API"),",\nwith semantics resembling continuous log streams from the standpoint of applications."))}d.isMDXComponent=!0}}]);