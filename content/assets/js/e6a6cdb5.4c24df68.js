"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[6501],{15680:(e,t,r)=>{r.d(t,{xA:()=>u,yg:()=>f});var n=r(96540);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var l=n.createContext({}),c=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=c(e.components);return n.createElement(l.Provider,{value:t},e.children)},p="mdxType",y={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,a=e.originalType,l=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),p=c(r),d=o,f=p["".concat(l,".").concat(d)]||p[d]||y[d]||a;return r?n.createElement(f,i(i({ref:t},u),{},{components:r})):n.createElement(f,i({ref:t},u))}));function f(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=r.length,i=new Array(a);i[0]=d;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[p]="string"==typeof e?e:o,i[1]=s;for(var c=2;c<a;c++)i[c]=r[c];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},66183:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>y,frontMatter:()=>a,metadata:()=>s,toc:()=>c});var n=r(9668),o=(r(96540),r(15680));const a={id:"overview",title:"BookKeeper Security"},i=void 0,s={unversionedId:"security/overview",id:"version-4.10.0/security/overview",title:"BookKeeper Security",description:"In the 4.5.0 release, the BookKeeper community added a number of features that can be used, together or separately, to secure a BookKeeper cluster.",source:"@site/versioned_docs/version-4.10.0/security/overview.md",sourceDirName:"security",slug:"/security/overview",permalink:"/docs/4.10.0/security/overview",draft:!1,tags:[],version:"4.10.0",frontMatter:{id:"overview",title:"BookKeeper Security"},sidebar:"version-4.10.0/docsSidebar",previous:{title:"DistributedLog",permalink:"/docs/4.10.0/api/distributedlog-api"},next:{title:"Encryption and Authentication using TLS",permalink:"/docs/4.10.0/security/tls"}},l={},c=[{value:"Next Steps",id:"next-steps",level:2}],u={toc:c},p="wrapper";function y(e){let{components:t,...r}=e;return(0,o.yg)(p,(0,n.A)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,o.yg)("p",null,"In the 4.5.0 release, the BookKeeper community added a number of features that can be used, together or separately, to secure a BookKeeper cluster.\nThe following security measures are currently supported:"),(0,o.yg)("ol",null,(0,o.yg)("li",{parentName:"ol"},"Authentication of connections to bookies from clients, using either ",(0,o.yg)("a",{parentName:"li",href:"tls"},"TLS")," or ",(0,o.yg)("a",{parentName:"li",href:"sasl"},"SASL (Kerberos)"),"."),(0,o.yg)("li",{parentName:"ol"},"Authentication of connections from clients, bookies, autorecovery daemons to ",(0,o.yg)("a",{parentName:"li",href:"zookeeper"},"ZooKeeper"),", when using zookeeper based ledger managers."),(0,o.yg)("li",{parentName:"ol"},"Encryption of data transferred between bookies and clients, between bookies and autorecovery daemons using ",(0,o.yg)("a",{parentName:"li",href:"tls"},"TLS"),".")),(0,o.yg)("p",null,"It\u2019s worth noting that security is optional - non-secured clusters are supported, as well as a mix of authenticated, unauthenticated, encrypted and non-encrypted clients."),(0,o.yg)("p",null,"NOTE: authorization is not yet available in 4.5.0. The Apache BookKeeper community is looking to add this feature in subsequent releases."),(0,o.yg)("h2",{id:"next-steps"},"Next Steps"),(0,o.yg)("ul",null,(0,o.yg)("li",{parentName:"ul"},(0,o.yg)("a",{parentName:"li",href:"tls"},"Encryption and Authentication using TLS")),(0,o.yg)("li",{parentName:"ul"},(0,o.yg)("a",{parentName:"li",href:"sasl"},"Authentication using SASL")),(0,o.yg)("li",{parentName:"ul"},(0,o.yg)("a",{parentName:"li",href:"zookeeper"},"ZooKeeper Authentication"))))}y.isMDXComponent=!0}}]);