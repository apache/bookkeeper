"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[5053],{15680:(e,o,a)=>{a.d(o,{xA:()=>s,yg:()=>u});var n=a(96540);function r(e,o,a){return o in e?Object.defineProperty(e,o,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[o]=a,e}function t(e,o){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);o&&(n=n.filter((function(o){return Object.getOwnPropertyDescriptor(e,o).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var o=1;o<arguments.length;o++){var a=null!=arguments[o]?arguments[o]:{};o%2?t(Object(a),!0).forEach((function(o){r(e,o,a[o])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):t(Object(a)).forEach((function(o){Object.defineProperty(e,o,Object.getOwnPropertyDescriptor(a,o))}))}return e}function l(e,o){if(null==e)return{};var a,n,r=function(e,o){if(null==e)return{};var a,n,r={},t=Object.keys(e);for(n=0;n<t.length;n++)a=t[n],o.indexOf(a)>=0||(r[a]=e[a]);return r}(e,o);if(Object.getOwnPropertySymbols){var t=Object.getOwnPropertySymbols(e);for(n=0;n<t.length;n++)a=t[n],o.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var p=n.createContext({}),g=function(e){var o=n.useContext(p),a=o;return e&&(a="function"==typeof e?e(o):i(i({},o),e)),a},s=function(e){var o=g(e.components);return n.createElement(p.Provider,{value:o},e.children)},d="mdxType",m={inlineCode:"code",wrapper:function(e){var o=e.children;return n.createElement(n.Fragment,{},o)}},c=n.forwardRef((function(e,o){var a=e.components,r=e.mdxType,t=e.originalType,p=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),d=g(a),c=r,u=d["".concat(p,".").concat(c)]||d[c]||m[c]||t;return a?n.createElement(u,i(i({ref:o},s),{},{components:a})):n.createElement(u,i({ref:o},s))}));function u(e,o){var a=arguments,r=o&&o.mdxType;if("string"==typeof e||r){var t=a.length,i=new Array(t);i[0]=c;var l={};for(var p in o)hasOwnProperty.call(o,p)&&(l[p]=o[p]);l.originalType=e,l[d]="string"==typeof e?e:r,i[1]=l;for(var g=2;g<t;g++)i[g]=a[g];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}c.displayName="MDXCreateElement"},19015:(e,o,a)=>{a.r(o),a.d(o,{assets:()=>p,contentTitle:()=>i,default:()=>m,frontMatter:()=>t,metadata:()=>l,toc:()=>g});var n=a(9668),r=(a(96540),a(15680));const t={id:"bookies",title:"BookKeeper administration"},i=void 0,l={unversionedId:"admin/bookies",id:"version-4.15.5/admin/bookies",title:"BookKeeper administration",description:"This document is a guide to deploying, administering, and maintaining BookKeeper. It also discusses best practices and common problems.",source:"@site/versioned_docs/version-4.15.5/admin/bookies.md",sourceDirName:"admin",slug:"/admin/bookies",permalink:"/docs/4.15.5/admin/bookies",draft:!1,tags:[],version:"4.15.5",frontMatter:{id:"bookies",title:"BookKeeper administration"},sidebar:"docsSidebar",previous:{title:"Deploying Apache BookKeeper on Kubernetes",permalink:"/docs/4.15.5/deployment/kubernetes"},next:{title:"Using AutoRecovery",permalink:"/docs/4.15.5/admin/autorecovery"}},p={},g=[{value:"Requirements",id:"requirements",level:2},{value:"Performance",id:"performance",level:3},{value:"ZooKeeper",id:"zookeeper",level:3},{value:"Starting and stopping bookies",id:"starting-and-stopping-bookies",level:2},{value:"Local bookies",id:"local-bookies",level:3},{value:"Configuring bookies",id:"configuring-bookies",level:2},{value:"Logging",id:"logging",level:2},{value:"Upgrading",id:"upgrading",level:2},{value:"Upgrade pattern",id:"upgrade-pattern",level:3},{value:"Formatting",id:"formatting",level:2},{value:"AutoRecovery",id:"autorecovery",level:2},{value:"Missing disks or directories",id:"missing-disks-or-directories",level:2}],s={toc:g},d="wrapper";function m(e){let{components:o,...a}=e;return(0,r.yg)(d,(0,n.A)({},s,a,{components:o,mdxType:"MDXLayout"}),(0,r.yg)("p",null,"This document is a guide to deploying, administering, and maintaining BookKeeper. It also discusses best practices and common problems."),(0,r.yg)("h2",{id:"requirements"},"Requirements"),(0,r.yg)("p",null,"A typical BookKeeper installation consists of an ensemble of bookies and a ZooKeeper quorum. The exact number of bookies depends on the quorum mode that you choose, desired throughput, and the number of clients using the installation simultaneously."),(0,r.yg)("p",null,"The minimum number of bookies depends on the type of installation:"),(0,r.yg)("ul",null,(0,r.yg)("li",{parentName:"ul"},"For ",(0,r.yg)("em",{parentName:"li"},"self-verifying")," entries you should run at least three bookies. In this mode, clients store a message authentication code along with each entry."),(0,r.yg)("li",{parentName:"ul"},"For ",(0,r.yg)("em",{parentName:"li"},"generic")," entries you should run at least four")),(0,r.yg)("p",null,"There is no upper limit on the number of bookies that you can run in a single ensemble."),(0,r.yg)("h3",{id:"performance"},"Performance"),(0,r.yg)("p",null,"To achieve optimal performance, BookKeeper requires each server to have at least two disks. It's possible to run a bookie with a single disk but performance will be significantly degraded."),(0,r.yg)("h3",{id:"zookeeper"},"ZooKeeper"),(0,r.yg)("p",null,"There is no constraint on the number of ZooKeeper nodes you can run with BookKeeper. A single machine running ZooKeeper in ",(0,r.yg)("a",{parentName:"p",href:"https://zookeeper.apache.org/doc/current/zookeeperStarted.html#sc_InstallingSingleMode"},"standalone mode")," is sufficient for BookKeeper, although for the sake of higher resilience we recommend running ZooKeeper in ",(0,r.yg)("a",{parentName:"p",href:"https://zookeeper.apache.org/doc/current/zookeeperStarted.html#sc_RunningReplicatedZooKeeper"},"quorum mode")," with multiple servers."),(0,r.yg)("h2",{id:"starting-and-stopping-bookies"},"Starting and stopping bookies"),(0,r.yg)("p",null,"You can run bookies either in the foreground or in the background, using ",(0,r.yg)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Nohup"},"nohup"),". You can also run ",(0,r.yg)("a",{parentName:"p",href:"#local-bookie"},"local bookies")," for development purposes."),(0,r.yg)("p",null,"To start a bookie in the foreground, use the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-bookie"},(0,r.yg)("inlineCode",{parentName:"a"},"bookie"))," command of the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper"},(0,r.yg)("inlineCode",{parentName:"a"},"bookkeeper"))," CLI tool:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper bookie\n")),(0,r.yg)("p",null,"To start a bookie in the background, use the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-daemon.sh"},(0,r.yg)("inlineCode",{parentName:"a"},"bookkeeper-daemon.sh"))," script and run ",(0,r.yg)("inlineCode",{parentName:"p"},"start bookie"),":"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper-daemon.sh start bookie\n")),(0,r.yg)("h3",{id:"local-bookies"},"Local bookies"),(0,r.yg)("p",null,"The instructions above showed you how to run bookies intended for production use. If you'd like to experiment with ensembles of bookies locally, you can use the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-localbookie"},(0,r.yg)("inlineCode",{parentName:"a"},"localbookie"))," command of the ",(0,r.yg)("inlineCode",{parentName:"p"},"bookkeeper")," CLI tool and specify the number of bookies you'd like to run."),(0,r.yg)("p",null,"This would spin up a local ensemble of 6 bookies:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper localbookie 6\n")),(0,r.yg)("blockquote",null,(0,r.yg)("p",{parentName:"blockquote"},"When you run a local bookie ensemble, all bookies run in a single JVM process.")),(0,r.yg)("h2",{id:"configuring-bookies"},"Configuring bookies"),(0,r.yg)("p",null,"There's a wide variety of parameters that you can set in the bookie configuration file in ",(0,r.yg)("inlineCode",{parentName:"p"},"bookkeeper-server/conf/bk_server.conf")," of your ",(0,r.yg)("a",{parentName:"p",href:"../reference/config"},"BookKeeper installation"),". A full listing can be found in ",(0,r.yg)("a",{parentName:"p",href:"../reference/config"},"Bookie configuration"),"."),(0,r.yg)("p",null,"Some of the more important parameters to be aware of:"),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:"left"},"Parameter"),(0,r.yg)("th",{parentName:"tr",align:"left"},"Description"),(0,r.yg)("th",{parentName:"tr",align:"left"},"Default"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"bookiePort")),(0,r.yg)("td",{parentName:"tr",align:"left"},"The TCP port that the bookie listens on"),(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"3181"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"zkServers")),(0,r.yg)("td",{parentName:"tr",align:"left"},"A comma-separated list of ZooKeeper servers in ",(0,r.yg)("inlineCode",{parentName:"td"},"hostname:port")," format"),(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"localhost:2181"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"journalDirectory")),(0,r.yg)("td",{parentName:"tr",align:"left"},"The directory where the ",(0,r.yg)("a",{parentName:"td",href:"../getting-started/concepts#log-device"},"log device")," stores the bookie's write-ahead log (WAL)"),(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"/tmp/bk-txn"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"ledgerDirectories")),(0,r.yg)("td",{parentName:"tr",align:"left"},"The directories where the ",(0,r.yg)("a",{parentName:"td",href:"../getting-started/concepts#ledger-device"},"ledger device")," stores the bookie's ledger entries (as a comma-separated list)"),(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"/tmp/bk-data"))))),(0,r.yg)("blockquote",null,(0,r.yg)("p",{parentName:"blockquote"},"Ideally, the directories specified ",(0,r.yg)("inlineCode",{parentName:"p"},"journalDirectory")," and ",(0,r.yg)("inlineCode",{parentName:"p"},"ledgerDirectories")," should be on difference devices.")),(0,r.yg)("h2",{id:"logging"},"Logging"),(0,r.yg)("p",null,"BookKeeper uses ",(0,r.yg)("a",{parentName:"p",href:"http://www.slf4j.org/"},"slf4j")," for logging, with ",(0,r.yg)("a",{parentName:"p",href:"https://logging.apache.org/log4j/2.x/"},"log4j")," bindings enabled by default."),(0,r.yg)("p",null,"To enable logging for a bookie, create a ",(0,r.yg)("inlineCode",{parentName:"p"},"log4j.properties")," file and point the ",(0,r.yg)("inlineCode",{parentName:"p"},"BOOKIE_LOG_CONF")," environment variable to the configuration file. Here's an example:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ export BOOKIE_LOG_CONF=/some/path/log4j.properties\n$ bin/bookkeeper bookie\n")),(0,r.yg)("h2",{id:"upgrading"},"Upgrading"),(0,r.yg)("p",null,"From time to time you may need to make changes to the filesystem layout of bookies---changes that are incompatible with previous versions of BookKeeper and require that directories used with previous versions are upgraded. If a filesystem upgrade is required when updating BookKeeper, the bookie will fail to start and return an error like this:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre"},"2017-05-25 10:41:50,494 - ERROR - [main:Bookie@246] - Directory layout version is less than 3, upgrade needed\n")),(0,r.yg)("p",null,"BookKeeper provides a utility for upgrading the filesystem. You can perform an upgrade using the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-upgrade"},(0,r.yg)("inlineCode",{parentName:"a"},"upgrade"))," command of the ",(0,r.yg)("inlineCode",{parentName:"p"},"bookkeeper")," CLI tool. When running ",(0,r.yg)("inlineCode",{parentName:"p"},"bookkeeper upgrade")," you need to specify one of three flags:"),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:"left"},"Flag"),(0,r.yg)("th",{parentName:"tr",align:"left"},"Action"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"--upgrade")),(0,r.yg)("td",{parentName:"tr",align:"left"},"Performs an upgrade")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"--rollback")),(0,r.yg)("td",{parentName:"tr",align:"left"},"Performs a rollback to the initial filesystem version")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:"left"},(0,r.yg)("inlineCode",{parentName:"td"},"--finalize")),(0,r.yg)("td",{parentName:"tr",align:"left"},"Marks the upgrade as complete")))),(0,r.yg)("h3",{id:"upgrade-pattern"},"Upgrade pattern"),(0,r.yg)("p",null,"A standard upgrade pattern is to run an upgrade..."),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper upgrade --upgrade\n")),(0,r.yg)("p",null,"...then check that everything is working normally, then kill the bookie. If everything is okay, finalize the upgrade..."),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper upgrade --finalize\n")),(0,r.yg)("p",null,"...and then restart the server:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper bookie\n")),(0,r.yg)("p",null,"If something has gone wrong, you can always perform a rollback:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper upgrade --rollback\n")),(0,r.yg)("h2",{id:"formatting"},"Formatting"),(0,r.yg)("p",null,"You can format bookie metadata in ZooKeeper using the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-shell-metaformat"},(0,r.yg)("inlineCode",{parentName:"a"},"metaformat"))," command of the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#the-bookkeeper-shell"},"BookKeeper shell"),"."),(0,r.yg)("p",null,"By default, formatting is done in interactive mode, which prompts you to confirm the format operation if old data exists. You can disable confirmation using the ",(0,r.yg)("inlineCode",{parentName:"p"},"-nonInteractive")," flag. If old data does exist, the format operation will abort ",(0,r.yg)("em",{parentName:"p"},"unless")," you set the ",(0,r.yg)("inlineCode",{parentName:"p"},"-force")," flag. Here's an example:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell metaformat\n")),(0,r.yg)("p",null,"You can format the local filesystem data on a bookie using the ",(0,r.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-shell-bookieformat"},(0,r.yg)("inlineCode",{parentName:"a"},"bookieformat"))," command on each bookie. Here's an example:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell bookieformat\n")),(0,r.yg)("blockquote",null,(0,r.yg)("p",{parentName:"blockquote"},"The ",(0,r.yg)("inlineCode",{parentName:"p"},"-force")," and ",(0,r.yg)("inlineCode",{parentName:"p"},"-nonInteractive")," flags are also available for the ",(0,r.yg)("inlineCode",{parentName:"p"},"bookieformat")," command.")),(0,r.yg)("h2",{id:"autorecovery"},"AutoRecovery"),(0,r.yg)("p",null,"For a guide to AutoRecovery in BookKeeper, see ",(0,r.yg)("a",{parentName:"p",href:"autorecovery"},"this doc"),"."),(0,r.yg)("h2",{id:"missing-disks-or-directories"},"Missing disks or directories"),(0,r.yg)("p",null,"Accidentally replacing disks or removing directories can cause a bookie to fail while trying to read a ledger fragment that, according to the ledger metadata, exists on the bookie. For this reason, when a bookie is started for the first time, its disk configuration is fixed for the lifetime of that bookie. Any change to its disk configuration, such as a crashed disk or an accidental configuration change, will result in the bookie being unable to start. That will throw an error like this:"),(0,r.yg)("pre",null,(0,r.yg)("code",{parentName:"pre"},"2017-05-29 18:19:13,790 - ERROR - [main:BookieServer314] \u2013 Exception running bookie server : @\norg.apache.bookkeeper.bookie.BookieException$InvalidCookieException\n.......at org.apache.bookkeeper.bookie.Cookie.verify(Cookie.java:82)\n.......at org.apache.bookkeeper.bookie.Bookie.checkEnvironment(Bookie.java:275)\n.......at org.apache.bookkeeper.bookie.Bookie.<init>(Bookie.java:351)\n")),(0,r.yg)("p",null,"If the change was the result of an accidental configuration change, the change can be reverted and the bookie can be restarted. However, if the change ",(0,r.yg)("em",{parentName:"p"},"cannot")," be reverted, such as is the case when you want to add a new disk or replace a disk, the bookie must be wiped and then all its data re-replicated onto it."),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},(0,r.yg)("p",{parentName:"li"},"Increment the ",(0,r.yg)("a",{parentName:"p",href:"../reference/config#bookiePort"},(0,r.yg)("inlineCode",{parentName:"a"},"bookiePort"))," parameter in the ",(0,r.yg)("a",{parentName:"p",href:"../reference/config"},(0,r.yg)("inlineCode",{parentName:"a"},"bk_server.conf")))),(0,r.yg)("li",{parentName:"ol"},(0,r.yg)("p",{parentName:"li"},"Ensure that all directories specified by ",(0,r.yg)("a",{parentName:"p",href:"../reference/config#journalDirectory"},(0,r.yg)("inlineCode",{parentName:"a"},"journalDirectory"))," and ",(0,r.yg)("a",{parentName:"p",href:"../reference/config#ledgerDirectories"},(0,r.yg)("inlineCode",{parentName:"a"},"ledgerDirectories"))," are empty.")),(0,r.yg)("li",{parentName:"ol"},(0,r.yg)("p",{parentName:"li"},(0,r.yg)("a",{parentName:"p",href:"#starting-and-stopping-bookies"},"Start the bookie"),".")),(0,r.yg)("li",{parentName:"ol"},(0,r.yg)("p",{parentName:"li"},"Run the following command to re-replicate the data:"),(0,r.yg)("pre",{parentName:"li"},(0,r.yg)("code",{parentName:"pre",className:"language-bash"},"$ bin/bookkeeper shell recover <oldbookie> \n")),(0,r.yg)("p",{parentName:"li"},"The ZooKeeper server, old bookie, and new bookie, are all identified by their external IP and ",(0,r.yg)("inlineCode",{parentName:"p"},"bookiePort")," (3181 by default). Here's an example:"),(0,r.yg)("pre",{parentName:"li"},(0,r.yg)("code",{parentName:"pre",className:"language-bash"},"$ bin/bookkeeper shell recover  192.168.1.10:3181\n")),(0,r.yg)("p",{parentName:"li"},"See the ",(0,r.yg)("a",{parentName:"p",href:"autorecovery"},"AutoRecovery")," documentation for more info on the re-replication process."))))}m.isMDXComponent=!0}}]);