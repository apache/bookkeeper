"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[5900],{15680:(e,t,a)=>{a.d(t,{xA:()=>m,yg:()=>c});var r=a(96540);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},o=Object.keys(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var p=r.createContext({}),g=function(e){var t=r.useContext(p),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},m=function(e){var t=g(e.components);return r.createElement(p.Provider,{value:t},e.children)},d="mdxType",s={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},y=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,o=e.originalType,p=e.parentName,m=i(e,["components","mdxType","originalType","parentName"]),d=g(a),y=n,c=d["".concat(p,".").concat(y)]||d[y]||s[y]||o;return a?r.createElement(c,l(l({ref:t},m),{},{components:a})):r.createElement(c,l({ref:t},m))}));function c(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=a.length,l=new Array(o);l[0]=y;var i={};for(var p in t)hasOwnProperty.call(t,p)&&(i[p]=t[p]);i.originalType=e,i[d]="string"==typeof e?e:n,l[1]=i;for(var g=2;g<o;g++)l[g]=a[g];return r.createElement.apply(null,l)}return r.createElement.apply(null,a)}y.displayName="MDXCreateElement"},44417:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>p,contentTitle:()=>l,default:()=>s,frontMatter:()=>o,metadata:()=>i,toc:()=>g});var r=a(58168),n=(a(96540),a(15680));const o={id:"installation",title:"BookKeeper installation"},l=void 0,i={unversionedId:"getting-started/installation",id:"version-4.7.3/getting-started/installation",title:"BookKeeper installation",description:"You can install BookKeeper either by downloading a GZipped tarball package or cloning the BookKeeper repository.",source:"@site/versioned_docs/version-4.7.3/getting-started/installation.md",sourceDirName:"getting-started",slug:"/getting-started/installation",permalink:"/docs/4.7.3/getting-started/installation",draft:!1,tags:[],version:"4.7.3",frontMatter:{id:"installation",title:"BookKeeper installation"},sidebar:"version-4.7.3/docsSidebar",previous:{title:"Apache BookKeeper 4.7.3",permalink:"/docs/4.7.3/overview/"},next:{title:"Run bookies locally",permalink:"/docs/4.7.3/getting-started/run-locally"}},p={},g=[{value:"Requirements",id:"requirements",level:2},{value:"Download",id:"download",level:2},{value:"Clone",id:"clone",level:2},{value:"Build using Maven",id:"build-using-maven",level:2},{value:"Useful Maven commands",id:"useful-maven-commands",level:3},{value:"Package directory",id:"package-directory",level:2}],m={toc:g},d="wrapper";function s(e){let{components:t,...a}=e;return(0,n.yg)(d,(0,r.A)({},m,a,{components:t,mdxType:"MDXLayout"}),(0,n.yg)("p",null,"You can install BookKeeper either by ",(0,n.yg)("a",{parentName:"p",href:"#download"},"downloading")," a ",(0,n.yg)("a",{parentName:"p",href:"http://www.gzip.org/"},"GZipped")," tarball package or ",(0,n.yg)("a",{parentName:"p",href:"#clone"},"cloning")," the BookKeeper repository."),(0,n.yg)("h2",{id:"requirements"},"Requirements"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("a",{parentName:"li",href:"https://www.opengroup.org/membership/forums/platform/unix"},"Unix environment")),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("a",{parentName:"li",href:"http://www.oracle.com/technetwork/java/javase/downloads/index.html"},"Java Development Kit 1.6")," or later"),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("a",{parentName:"li",href:"https://maven.apache.org/install.html"},"Maven 3.0")," or later")),(0,n.yg)("h2",{id:"download"},"Download"),(0,n.yg)("p",null,"You can download Apache BookKeeper releases from one of many ",(0,n.yg)("a",{parentName:"p",href:"http://www.apache.org/dyn/closer.cgi/bookkeeper"},"Apache mirrors"),". "),(0,n.yg)("h2",{id:"clone"},"Clone"),(0,n.yg)("p",null,"To build BookKeeper from source, clone the repository, either from the ",(0,n.yg)("a",{parentName:"p",href:"https://github.com/apache/bookkeeper"},"GitHub mirror")," or from the ",(0,n.yg)("a",{parentName:"p",href:"https://git.apache.org/bookkeeper.git"},"Apache repository"),":"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"# From the GitHub mirror\n$ git clone https://github.com/apache/bookkeeper\n\n# From Apache directly\n$ git clone git://git.apache.org/bookkeeper.git/\n")),(0,n.yg)("h2",{id:"build-using-maven"},"Build using Maven"),(0,n.yg)("p",null,"Once you have the BookKeeper on your local machine, either by ",(0,n.yg)("a",{parentName:"p",href:"#download"},"downloading")," or ",(0,n.yg)("a",{parentName:"p",href:"#clone"},"cloning")," it, you can then build BookKeeper from source using Maven:"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ mvn package\n")),(0,n.yg)("blockquote",null,(0,n.yg)("p",{parentName:"blockquote"},"You can skip tests by adding the ",(0,n.yg)("inlineCode",{parentName:"p"},"-DskipTests")," flag when running ",(0,n.yg)("inlineCode",{parentName:"p"},"mvn package"),".")),(0,n.yg)("h3",{id:"useful-maven-commands"},"Useful Maven commands"),(0,n.yg)("p",null,"Some other useful Maven commands beyond ",(0,n.yg)("inlineCode",{parentName:"p"},"mvn package"),":"),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:"left"},"Command"),(0,n.yg)("th",{parentName:"tr",align:"left"},"Action"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn clean")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Removes build artifacts")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn compile")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Compiles JAR files from Java sources")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn compile spotbugs:spotbugs")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Compile using the Maven ",(0,n.yg)("a",{parentName:"td",href:"https://github.com/spotbugs/spotbugs-maven-plugin"},"SpotBugs")," plugin")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn install")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Install the BookKeeper JAR locally in your local Maven cache (usually in the ",(0,n.yg)("inlineCode",{parentName:"td"},"~/.m2")," directory)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn deploy")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Deploy the BookKeeper JAR to the Maven repo (if you have the proper credentials)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn verify")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Performs a wide variety of verification and validation tasks")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn apache-rat:check")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Run Maven using the ",(0,n.yg)("a",{parentName:"td",href:"http://creadur.apache.org/rat/apache-rat-plugin/"},"Apache Rat")," plugin")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn compile javadoc:aggregate")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Build Javadocs locally")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("inlineCode",{parentName:"td"},"mvn package assembly:single")),(0,n.yg)("td",{parentName:"tr",align:"left"},"Build a complete distribution using the Maven ",(0,n.yg)("a",{parentName:"td",href:"http://maven.apache.org/plugins/maven-assembly-plugin/"},"Assembly")," plugin")))),(0,n.yg)("h2",{id:"package-directory"},"Package directory"),(0,n.yg)("p",null,"The BookKeeper project contains several subfolders that you should be aware of:"),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:"left"},"Subfolder"),(0,n.yg)("th",{parentName:"tr",align:"left"},"Contains"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-server"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-server"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"The BookKeeper server and client")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-benchmark"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-benchmark"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"A benchmarking suite for measuring BookKeeper performance")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-stats"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"A BookKeeper stats library")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:"left"},(0,n.yg)("a",{parentName:"td",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-stats-providers"},(0,n.yg)("inlineCode",{parentName:"a"},"bookkeeper-stats-providers"))),(0,n.yg)("td",{parentName:"tr",align:"left"},"BookKeeper stats providers")))))}s.isMDXComponent=!0}}]);