"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[6197],{3905:function(e,n,t){t.d(n,{Zo:function(){return s},kt:function(){return d}});var r=t(67294);function o(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function l(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);n&&(r=r.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,r)}return t}function i(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?l(Object(t),!0).forEach((function(n){o(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):l(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function a(e,n){if(null==e)return{};var t,r,o=function(e,n){if(null==e)return{};var t,r,o={},l=Object.keys(e);for(r=0;r<l.length;r++)t=l[r],n.indexOf(t)>=0||(o[t]=e[t]);return o}(e,n);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)t=l[r],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var c=r.createContext({}),u=function(e){var n=r.useContext(c),t=n;return e&&(t="function"==typeof e?e(n):i(i({},n),e)),t},s=function(e){var n=u(e.components);return r.createElement(c.Provider,{value:n},e.children)},p="mdxType",f={inlineCode:"code",wrapper:function(e){var n=e.children;return r.createElement(r.Fragment,{},n)}},b=r.forwardRef((function(e,n){var t=e.components,o=e.mdxType,l=e.originalType,c=e.parentName,s=a(e,["components","mdxType","originalType","parentName"]),p=u(t),b=o,d=p["".concat(c,".").concat(b)]||p[b]||f[b]||l;return t?r.createElement(d,i(i({ref:n},s),{},{components:t})):r.createElement(d,i({ref:n},s))}));function d(e,n){var t=arguments,o=n&&n.mdxType;if("string"==typeof e||o){var l=t.length,i=new Array(l);i[0]=b;var a={};for(var c in n)hasOwnProperty.call(n,c)&&(a[c]=n[c]);a.originalType=e,a[p]="string"==typeof e?e:o,i[1]=a;for(var u=2;u<l;u++)i[u]=t[u];return r.createElement.apply(null,i)}return r.createElement.apply(null,t)}b.displayName="MDXCreateElement"},80053:function(e,n,t){t.r(n),t.d(n,{assets:function(){return c},contentTitle:function(){return i},default:function(){return f},frontMatter:function(){return l},metadata:function(){return a},toc:function(){return u}});var r=t(83117),o=(t(67294),t(3905));const l={id:"run-locally",title:"Run bookies locally"},i=void 0,a={unversionedId:"getting-started/run-locally",id:"version-4.17.0/getting-started/run-locally",title:"Run bookies locally",description:"Bookies are individual BookKeeper servers. You can run an ensemble of bookies locally on a single machine using the localbookie command of the bookkeeper CLI tool and specifying the number of bookies you'd like to include in the ensemble.",source:"@site/versioned_docs/version-4.17.0/getting-started/run-locally.md",sourceDirName:"getting-started",slug:"/getting-started/run-locally",permalink:"/docs/getting-started/run-locally",draft:!1,tags:[],version:"4.17.0",frontMatter:{id:"run-locally",title:"Run bookies locally"}},c={},u=[],s={toc:u},p="wrapper";function f(e){let{components:n,...t}=e;return(0,o.kt)(p,(0,r.Z)({},s,t,{components:n,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"Bookies are individual BookKeeper servers. You can run an ensemble of bookies locally on a single machine using the ",(0,o.kt)("a",{parentName:"p",href:"../reference/cli#bookkeeper-localbookie"},(0,o.kt)("inlineCode",{parentName:"a"},"localbookie"))," command of the ",(0,o.kt)("inlineCode",{parentName:"p"},"bookkeeper")," CLI tool and specifying the number of bookies you'd like to include in the ensemble."),(0,o.kt)("p",null,"This would start up an ensemble with 10 bookies:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper localbookie 10\n")),(0,o.kt)("blockquote",null,(0,o.kt)("p",{parentName:"blockquote"},"When you start up an ensemble using ",(0,o.kt)("inlineCode",{parentName:"p"},"localbookie"),", all bookies run in a single JVM process.")))}f.isMDXComponent=!0}}]);