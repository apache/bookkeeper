"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[5670],{15680:(e,t,r)=>{r.d(t,{xA:()=>g,yg:()=>y});var a=r(96540);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function l(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var p=a.createContext({}),s=function(e){var t=a.useContext(p),r=t;return e&&(r="function"==typeof e?e(t):l(l({},t),e)),r},g=function(e){var t=s(e.components);return a.createElement(p.Provider,{value:t},e.children)},c="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},u=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,o=e.originalType,p=e.parentName,g=i(e,["components","mdxType","originalType","parentName"]),c=s(r),u=n,y=c["".concat(p,".").concat(u)]||c[u]||d[u]||o;return r?a.createElement(y,l(l({ref:t},g),{},{components:r})):a.createElement(y,l({ref:t},g))}));function y(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=r.length,l=new Array(o);l[0]=u;var i={};for(var p in t)hasOwnProperty.call(t,p)&&(i[p]=t[p]);i.originalType=e,i[c]="string"==typeof e?e:n,l[1]=i;for(var s=2;s<o;s++)l[s]=r[s];return a.createElement.apply(null,l)}return a.createElement.apply(null,r)}u.displayName="MDXCreateElement"},8658:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>p,contentTitle:()=>l,default:()=>d,frontMatter:()=>o,metadata:()=>i,toc:()=>s});var a=r(58168),n=(r(96540),r(15680));const o={id:"installation",title:"BookKeeper installation"},l=void 0,i={unversionedId:"getting-started/installation",id:"version-4.15.5/getting-started/installation",title:"BookKeeper installation",description:"You can install BookKeeper either by downloading a GZipped tarball package, using the Docker image or cloning the BookKeeper repository.",source:"@site/versioned_docs/version-4.15.5/getting-started/installation.md",sourceDirName:"getting-started",slug:"/getting-started/installation",permalink:"/docs/4.15.5/getting-started/installation",draft:!1,tags:[],version:"4.15.5",frontMatter:{id:"installation",title:"BookKeeper installation"},sidebar:"docsSidebar",previous:{title:"Apache BookKeeper 4.15.5",permalink:"/docs/4.15.5/overview/"},next:{title:"Run bookies locally",permalink:"/docs/4.15.5/getting-started/run-locally"}},p={},s=[{value:"Requirements",id:"requirements",level:2},{value:"Download",id:"download",level:2},{value:"Clone",id:"clone",level:2},{value:"Build using Gradle",id:"build-using-gradle",level:2},{value:"Package directory",id:"package-directory",level:2}],g={toc:s},c="wrapper";function d(e){let{components:t,...r}=e;return(0,n.yg)(c,(0,a.A)({},g,r,{components:t,mdxType:"MDXLayout"}),(0,n.yg)("p",null,"You can install BookKeeper either by ",(0,n.yg)("a",{parentName:"p",href:"#download"},"downloading")," a ",(0,n.yg)("a",{parentName:"p",href:"http://www.gzip.org/"},"GZipped")," tarball package, using the ",(0,n.yg)("a",{parentName:"p",href:"https://hub.docker.com/r/apache/bookkeeper/tags"},"Docker image")," or ",(0,n.yg)("a",{parentName:"p",href:"#clone"},"cloning")," the BookKeeper repository."),(0,n.yg)("h2",{id:"requirements"},"Requirements"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("a",{parentName:"li",href:"https://www.opengroup.org/membership/forums/platform/unix"},"Unix environment")),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("a",{parentName:"li",href:"http://www.oracle.com/technetwork/java/javase/downloads/index.html"},"Java Development Kit 1.8")," or later")),(0,n.yg)("h2",{id:"download"},"Download"),(0,n.yg)("p",null,"You can download Apache BookKeeper releases from the ",(0,n.yg)("a",{parentName:"p",href:"/releases"},"Download page"),"."),(0,n.yg)("h2",{id:"clone"},"Clone"),(0,n.yg)("p",null,"To build BookKeeper from source, clone the repository from the ",(0,n.yg)("a",{parentName:"p",href:"https://github.com/apache/bookkeeper"},"GitHub mirror"),":"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ git clone https://github.com/apache/bookkeeper\n")),(0,n.yg)("h2",{id:"build-using-gradle"},"Build using Gradle"),(0,n.yg)("p",null,"Once you have the BookKeeper on your local machine, either by ",(0,n.yg)("a",{parentName:"p",href:"#download"},"downloading")," or ",(0,n.yg)("a",{parentName:"p",href:"#clone"},"cloning")," it, you can then build BookKeeper from source using Gradle:"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ ./gradlew build -x signDistTar -x test\n")),(0,n.yg)("p",null,"To run all the tests:"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ ./gradlew test -x signDistTar\n")),(0,n.yg)("h2",{id:"package-directory"},"Package directory"),(0,n.yg)("p",null,"The BookKeeper project contains several subfolders that you should be aware of:"),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:"left"},"Subfolder"),(0,n.yg)("th",{parentName:"tr",align:"left"},"Contains"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-server"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-server"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"The BookKeeper server and client")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-benchmark"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-benchmark"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"A benchmarking suite for measuring BookKeeper performance")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-stats"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"A BookKeeper stats library")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats-providers"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-stats-providers"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"BookKeeper stats providers")))))}d.isMDXComponent=!0}}]);