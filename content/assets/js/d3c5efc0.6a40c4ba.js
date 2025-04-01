"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[9028],{15680:(e,r,o)=>{o.d(r,{xA:()=>s,yg:()=>y});var t=o(96540);function n(e,r,o){return r in e?Object.defineProperty(e,r,{value:o,enumerable:!0,configurable:!0,writable:!0}):e[r]=o,e}function i(e,r){var o=Object.keys(e);if(Object.getOwnPropertySymbols){var t=Object.getOwnPropertySymbols(e);r&&(t=t.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),o.push.apply(o,t)}return o}function a(e){for(var r=1;r<arguments.length;r++){var o=null!=arguments[r]?arguments[r]:{};r%2?i(Object(o),!0).forEach((function(r){n(e,r,o[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(o)):i(Object(o)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(o,r))}))}return e}function l(e,r){if(null==e)return{};var o,t,n=function(e,r){if(null==e)return{};var o,t,n={},i=Object.keys(e);for(t=0;t<i.length;t++)o=i[t],r.indexOf(o)>=0||(n[o]=e[o]);return n}(e,r);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(t=0;t<i.length;t++)o=i[t],r.indexOf(o)>=0||Object.prototype.propertyIsEnumerable.call(e,o)&&(n[o]=e[o])}return n}var c=t.createContext({}),p=function(e){var r=t.useContext(c),o=r;return e&&(o="function"==typeof e?e(r):a(a({},r),e)),o},s=function(e){var r=p(e.components);return t.createElement(c.Provider,{value:r},e.children)},u="mdxType",g={inlineCode:"code",wrapper:function(e){var r=e.children;return t.createElement(t.Fragment,{},r)}},f=t.forwardRef((function(e,r){var o=e.components,n=e.mdxType,i=e.originalType,c=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),u=p(o),f=n,y=u["".concat(c,".").concat(f)]||u[f]||g[f]||i;return o?t.createElement(y,a(a({ref:r},s),{},{components:o})):t.createElement(y,a({ref:r},s))}));function y(e,r){var o=arguments,n=r&&r.mdxType;if("string"==typeof e||n){var i=o.length,a=new Array(i);a[0]=f;var l={};for(var c in r)hasOwnProperty.call(r,c)&&(l[c]=r[c]);l.originalType=e,l[u]="string"==typeof e?e:n,a[1]=l;for(var p=2;p<i;p++)a[p]=o[p];return t.createElement.apply(null,a)}return t.createElement.apply(null,o)}f.displayName="MDXCreateElement"},20099:(e,r,o)=>{o.r(r),o.d(r,{assets:()=>c,contentTitle:()=>a,default:()=>g,frontMatter:()=>i,metadata:()=>l,toc:()=>p});var t=o(58168),n=(o(96540),o(15680));const i={id:"geo-replication",title:"Geo-replication"},a=void 0,l={unversionedId:"admin/geo-replication",id:"version-4.17.1/admin/geo-replication",title:"Geo-replication",description:"Geo-replication is the replication of data across BookKeeper clusters. In order to enable geo-replication for a group of BookKeeper clusters,",source:"@site/versioned_docs/version-4.17.1/admin/geo-replication.md",sourceDirName:"admin",slug:"/admin/geo-replication",permalink:"/docs/admin/geo-replication",draft:!1,tags:[],version:"4.17.1",frontMatter:{id:"geo-replication",title:"Geo-replication"}},c={},p=[{value:"Global ZooKeeper",id:"global-zookeeper",level:2},{value:"Geo-replication across three clusters",id:"geo-replication-across-three-clusters",level:3},{value:"Region-aware placement policy",id:"region-aware-placement-policy",level:2},{value:"Autorecovery",id:"autorecovery",level:2}],s={toc:p},u="wrapper";function g(e){let{components:r,...o}=e;return(0,n.yg)(u,(0,t.A)({},s,o,{components:r,mdxType:"MDXLayout"}),(0,n.yg)("p",null,(0,n.yg)("em",{parentName:"p"},"Geo-replication")," is the replication of data across BookKeeper clusters. In order to enable geo-replication for a group of BookKeeper clusters,"),(0,n.yg)("h2",{id:"global-zookeeper"},"Global ZooKeeper"),(0,n.yg)("p",null,"Setting up a global ZooKeeper quorum is a lot like setting up a cluster-specific quorum. The crucial difference is that"),(0,n.yg)("h3",{id:"geo-replication-across-three-clusters"},"Geo-replication across three clusters"),(0,n.yg)("p",null,"Let's say that you want to set up geo-replication across clusters in regions A, B, and C. First, the BookKeeper clusters in each region must have their own local (cluster-specific) ZooKeeper quorum."),(0,n.yg)("blockquote",null,(0,n.yg)("p",{parentName:"blockquote"},"BookKeeper clusters use global ZooKeeper only for metadata storage. Traffic from bookies to ZooKeeper should thus be fairly light in general.")),(0,n.yg)("p",null,"The crucial difference between using cluster-specific ZooKeeper and global ZooKeeper is that bookies is that you need to point all bookies to use the global ZooKeeper setup."),(0,n.yg)("h2",{id:"region-aware-placement-policy"},"Region-aware placement policy"),(0,n.yg)("h2",{id:"autorecovery"},"Autorecovery"))}g.isMDXComponent=!0}}]);