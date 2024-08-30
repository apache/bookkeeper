"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[1249],{15680:(e,n,t)=>{t.d(n,{xA:()=>u,yg:()=>b});var r=t(96540);function o(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function l(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);n&&(r=r.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,r)}return t}function a(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?l(Object(t),!0).forEach((function(n){o(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):l(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function i(e,n){if(null==e)return{};var t,r,o=function(e,n){if(null==e)return{};var t,r,o={},l=Object.keys(e);for(r=0;r<l.length;r++)t=l[r],n.indexOf(t)>=0||(o[t]=e[t]);return o}(e,n);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)t=l[r],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var c=r.createContext({}),s=function(e){var n=r.useContext(c),t=n;return e&&(t="function"==typeof e?e(n):a(a({},n),e)),t},u=function(e){var n=s(e.components);return r.createElement(c.Provider,{value:n},e.children)},p="mdxType",y={inlineCode:"code",wrapper:function(e){var n=e.children;return r.createElement(r.Fragment,{},n)}},d=r.forwardRef((function(e,n){var t=e.components,o=e.mdxType,l=e.originalType,c=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),p=s(t),d=o,b=p["".concat(c,".").concat(d)]||p[d]||y[d]||l;return t?r.createElement(b,a(a({ref:n},u),{},{components:t})):r.createElement(b,a({ref:n},u))}));function b(e,n){var t=arguments,o=n&&n.mdxType;if("string"==typeof e||o){var l=t.length,a=new Array(l);a[0]=d;var i={};for(var c in n)hasOwnProperty.call(n,c)&&(i[c]=n[c]);i.originalType=e,i[p]="string"==typeof e?e:o,a[1]=i;for(var s=2;s<l;s++)a[s]=t[s];return r.createElement.apply(null,a)}return r.createElement.apply(null,t)}d.displayName="MDXCreateElement"},55569:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>c,contentTitle:()=>a,default:()=>y,frontMatter:()=>l,metadata:()=>i,toc:()=>s});var r=t(9668),o=(t(96540),t(15680));const l={id:"run-locally",title:"Run bookies locally"},a=void 0,i={unversionedId:"getting-started/run-locally",id:"version-4.6.2/getting-started/run-locally",title:"Run bookies locally",description:"Bookies are individual BookKeeper servers. You can run an ensemble of bookies locally on a single machine using the localbookie command of the bookkeeper CLI tool and specifying the number of bookies you'd like to include in the ensemble.",source:"@site/versioned_docs/version-4.6.2/getting-started/run-locally.md",sourceDirName:"getting-started",slug:"/getting-started/run-locally",permalink:"/docs/4.6.2/getting-started/run-locally",draft:!1,tags:[],version:"4.6.2",frontMatter:{id:"run-locally",title:"Run bookies locally"},sidebar:"version-4.6.2/docsSidebar",previous:{title:"BookKeeper installation",permalink:"/docs/4.6.2/getting-started/installation"},next:{title:"BookKeeper concepts and architecture",permalink:"/docs/4.6.2/getting-started/concepts"}},c={},s=[],u={toc:s},p="wrapper";function y(e){let{components:n,...t}=e;return(0,o.yg)(p,(0,r.A)({},u,t,{components:n,mdxType:"MDXLayout"}),(0,o.yg)("p",null,"Bookies are individual BookKeeper servers. You can run an ensemble of bookies locally on a single machine using the ",(0,o.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-localbookie"},(0,o.yg)("inlineCode",{parentName:"a"},"localbookie"))," command of the ",(0,o.yg)("inlineCode",{parentName:"p"},"bookkeeper")," CLI tool and specifying the number of bookies you'd like to include in the ensemble."),(0,o.yg)("p",null,"This would start up an ensemble with 10 bookies:"),(0,o.yg)("pre",null,(0,o.yg)("code",{parentName:"pre",className:"language-shell"},"$ bookkeeper-server/bin/bookkeeper localbookie 10\n")),(0,o.yg)("blockquote",null,(0,o.yg)("p",{parentName:"blockquote"},"When you start up an ensemble using ",(0,o.yg)("inlineCode",{parentName:"p"},"localbookie"),", all bookies run in a single JVM process.")))}y.isMDXComponent=!0}}]);