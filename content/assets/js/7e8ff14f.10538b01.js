"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[1751],{3905:function(e,t,n){n.d(t,{Zo:function(){return m},kt:function(){return k}});var o=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function r(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,o,a=function(e,t){if(null==e)return{};var n,o,a={},i=Object.keys(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=o.createContext({}),p=function(e){var t=o.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):r(r({},t),e)),n},m=function(e){var t=p(e.components);return o.createElement(s.Provider,{value:t},e.children)},c="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},u=o.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,m=l(e,["components","mdxType","originalType","parentName"]),c=p(n),u=a,k=c["".concat(s,".").concat(u)]||c[u]||d[u]||i;return n?o.createElement(k,r(r({ref:t},m),{},{components:n})):o.createElement(k,r({ref:t},m))}));function k(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,r=new Array(i);r[0]=u;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[c]="string"==typeof e?e:a,r[1]=l;for(var p=2;p<i;p++)r[p]=n[p];return o.createElement.apply(null,r)}return o.createElement.apply(null,n)}u.displayName="MDXCreateElement"},48123:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return r},default:function(){return c},frontMatter:function(){return i},metadata:function(){return l},toc:function(){return s}});var o=n(83117),a=(n(67294),n(3905));const i={},r="BP-27: New BookKeeper CLI",l={type:"mdx",permalink:"/bps/BP-27-new-bookkeeper-cli",source:"@site/src/pages/bps/BP-27-new-bookkeeper-cli.md",title:"BP-27: New BookKeeper CLI",description:"Motivation",frontMatter:{}},s=[{value:"Motivation",id:"motivation",level:3},{value:"Public Interfaces",id:"public-interfaces",level:3},{value:"CommandGroup and Command",id:"commandgroup-and-command",level:4},{value:"Proposed Changes",id:"proposed-changes",level:3},{value:"Compatibility, Deprecation, and Migration Plan",id:"compatibility-deprecation-and-migration-plan",level:3},{value:"Test Plan",id:"test-plan",level:3},{value:"Rejected Alternatives",id:"rejected-alternatives",level:3}],p={toc:s},m="wrapper";function c(e){let{components:t,...n}=e;return(0,a.kt)(m,(0,o.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"bp-27-new-bookkeeper-cli"},"BP-27: New BookKeeper CLI"),(0,a.kt)("h3",{id:"motivation"},"Motivation"),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"BookieShell")," is the current bookkeeper cli for interacting and operating a bookkeeper cluster. However, this class is getting bigger with more commands added to it. It is facing a few problems for maintenance and extensibility."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"All commands sit in one gaint shell class. It is hard to tell if a command is used for managing a bookie only or if a command is used for managing a cluster."),(0,a.kt)("li",{parentName:"ul"},"Lack of unit tests. This class has very few test coverage. Most of the commands (introduced in early days) don't have a unit test."),(0,a.kt)("li",{parentName:"ul"},"Lack of extensibility. If a new function component (for example, dlog) is introduced, it is a bit hard to extend this CLI to have commands for new function component.")),(0,a.kt)("p",null,"All these problems lead to the proposal here. This proposal is to propose refactoring/redesigning the bookkeeper CLI to allow better managebility for maintenance, better test coverage and better extensibility for new function components."),(0,a.kt)("h3",{id:"public-interfaces"},"Public Interfaces"),(0,a.kt)("p",null,"This proposal will not change existing ",(0,a.kt)("inlineCode",{parentName:"p"},"BookieShell"),". All functionalities will remain as same when using ",(0,a.kt)("inlineCode",{parentName:"p"},"bin/bookkeeper shell"),".\nInstead a new module ",(0,a.kt)("inlineCode",{parentName:"p"},"bookkeeper-tools")," will be introduced for developing the new BookKeeper CLI and a new script ",(0,a.kt)("inlineCode",{parentName:"p"},"bin/bookkeeper-cli")," for executing CLI commands."),(0,a.kt)("p",null,"The new bookkeeper CLI follows the pattern that pulsar-admin is using. The CLI commandline format would be:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"$ bookkeeper-cli [general options] <command-group> <command> [options of command]\n")),(0,a.kt)("h4",{id:"commandgroup-and-command"},"CommandGroup and Command"),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"<command-group>")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"<command>")," are introduced for categorizing the commands into groups. So the commands within same group have same operation scope (e.g. whether a command is applied to a cluster or a command is applied to a bookie)."),(0,a.kt)("p",null,"When a new function component is introduced, all its related commands can be managed in its own command group and register the group to CLI. This would allow flexible extensibility and make maintenance easier and clearer."),(0,a.kt)("p",null,"The proposed command groups are:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},'"cluster": commands that operate on a cluster'),(0,a.kt)("li",{parentName:"ul"},'"bookie": commands that operate on a single bookie'),(0,a.kt)("li",{parentName:"ul"},'"metadata": commands that operate with metadata store'),(0,a.kt)("li",{parentName:"ul"},'"client": commands that use a bookkeeper client for interacting with a cluster.')),(0,a.kt)("p",null,"Example Outputs for the new BookKeeper CLI:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Show all command groups: ",(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeper-cli --help"))),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"Usage: bookkeeper-cli [options] [command] [command options]\n  Options:\n    -c, --conf\n       Bookie Configuration File\n    -h, --help\n       Show this help message\n       Default: false\n  Commands:\n    bookie      Commands on operating a single bookie\n      Usage: bookie [options]\n\n    client      Commands that interact with a cluster\n      Usage: client [options]\n\n    cluster      Commands that operate a cluster\n      Usage: cluster [options]\n\n    metadata      Commands that interact with metadata storage\n      Usage: metadata [options]\n")),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Show commands under ",(0,a.kt)("inlineCode",{parentName:"li"},"cluster"),": ",(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeper-cli cluster --help"))),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"Usage: bookie-shell cluster [options] [command] [command options]\n  Commands:\n    listbookies      List the bookies, which are running as either readwrite or readonly mode.\n      Usage: listbookies [options]\n        Options:\n          -ro, --readonly\n             Print readonly bookies\n             Default: false\n          -rw, --readwrite\n             Print readwrite bookies\n             Default: false\n")),(0,a.kt)("h3",{id:"proposed-changes"},"Proposed Changes"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Introduced a new module called ",(0,a.kt)("inlineCode",{parentName:"li"},"bookkeeper-tools")," for developing the new CLI."),(0,a.kt)("li",{parentName:"ul"},"The new CLI will use ",(0,a.kt)("a",{parentName:"li",href:"http://jcommander.org"},"JCommander")," for parse command line parameters: better on supporting this proposal commandline syntax."),(0,a.kt)("li",{parentName:"ul"},"All the actual logic of the commands will be organized under ",(0,a.kt)("inlineCode",{parentName:"li"},"org.apache.bookkeeper.tools.cli.commands"),". Each command group has its own subpackage and each command will be a class file under that command-group subpackage.\nDoing this provides better testability, since the command logic is limited in one file rather than in a gaint shell class. Proposed layout can be found ",(0,a.kt)("a",{parentName:"li",href:"https://github.com/apache/bookkeeper/tree/master/bookkeeper-server/src/main/java/org/apache/bookkeeper/tools/cli/commands"},"here"),"."),(0,a.kt)("li",{parentName:"ul"},"For each command: the logic of a command will be moved out of ",(0,a.kt)("inlineCode",{parentName:"li"},"BookieShell")," to its own class ",(0,a.kt)("inlineCode",{parentName:"li"},"org.apache.bookkeeper.tools.cli.commands.<command-group>.<CommandClass>.java"),". The old BookieShell will use the new Command class and delegate the actual logic.")),(0,a.kt)("p",null,"An initial prototype is available: ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/sijie/bookkeeper/tree/bookie_shell_refactor"},"https://github.com/sijie/bookkeeper/tree/bookie_shell_refactor")),(0,a.kt)("h3",{id:"compatibility-deprecation-and-migration-plan"},"Compatibility, Deprecation, and Migration Plan"),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"bin/bookkeeper shell")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"bin/bookkeeper-cli")," will co-exist for a few releases. After ",(0,a.kt)("inlineCode",{parentName:"p"},"bin/bookkeeper-cli")," takes over all the functionalities of ",(0,a.kt)("inlineCode",{parentName:"p"},"BookieShell"),", we will consider deprecating the old ",(0,a.kt)("inlineCode",{parentName:"p"},"BookieShell"),"."),(0,a.kt)("p",null,"So no compatibility concern at this moment."),(0,a.kt)("h3",{id:"test-plan"},"Test Plan"),(0,a.kt)("p",null,"When a command is moved from BookieShell to ",(0,a.kt)("inlineCode",{parentName:"p"},"org.apache.bookkeeper.tools.cli"),", several unit tests should be added:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Unit tests for the command logic itself."),(0,a.kt)("li",{parentName:"ul"},"Unit tests in new CLI for this command."),(0,a.kt)("li",{parentName:"ul"},"Unit tests in old BookieShell for this command.")),(0,a.kt)("h3",{id:"rejected-alternatives"},"Rejected Alternatives"),(0,a.kt)("p",null,"Another proposal is to redesign the bookkeeper CLI using admin REST api. This is not considered at this moment because some of the commands are not well supported in admin REST api (for example, metaformat, bookieformat, and most of the commands\nused for troubleshooting individual bookies). If we want to support a CLI using admin REST api, we can have a separate CLI called ",(0,a.kt)("inlineCode",{parentName:"p"},"bookkeeper-rest-ci")," to use admin REST api for operating the cluster."))}c.isMDXComponent=!0}}]);