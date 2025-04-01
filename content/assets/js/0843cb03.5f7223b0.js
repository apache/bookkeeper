"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[4336],{15680:(e,a,l)=>{l.d(a,{xA:()=>d,yg:()=>m});var t=l(96540);function n(e,a,l){return a in e?Object.defineProperty(e,a,{value:l,enumerable:!0,configurable:!0,writable:!0}):e[a]=l,e}function r(e,a){var l=Object.keys(e);if(Object.getOwnPropertySymbols){var t=Object.getOwnPropertySymbols(e);a&&(t=t.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),l.push.apply(l,t)}return l}function o(e){for(var a=1;a<arguments.length;a++){var l=null!=arguments[a]?arguments[a]:{};a%2?r(Object(l),!0).forEach((function(a){n(e,a,l[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(l)):r(Object(l)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(l,a))}))}return e}function g(e,a){if(null==e)return{};var l,t,n=function(e,a){if(null==e)return{};var l,t,n={},r=Object.keys(e);for(t=0;t<r.length;t++)l=r[t],a.indexOf(l)>=0||(n[l]=e[l]);return n}(e,a);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(t=0;t<r.length;t++)l=r[t],a.indexOf(l)>=0||Object.prototype.propertyIsEnumerable.call(e,l)&&(n[l]=e[l])}return n}var i=t.createContext({}),s=function(e){var a=t.useContext(i),l=a;return e&&(l="function"==typeof e?e(a):o(o({},a),e)),l},d=function(e){var a=s(e.components);return t.createElement(i.Provider,{value:a},e.children)},p="mdxType",y={inlineCode:"code",wrapper:function(e){var a=e.children;return t.createElement(t.Fragment,{},a)}},u=t.forwardRef((function(e,a){var l=e.components,n=e.mdxType,r=e.originalType,i=e.parentName,d=g(e,["components","mdxType","originalType","parentName"]),p=s(l),u=n,m=p["".concat(i,".").concat(u)]||p[u]||y[u]||r;return l?t.createElement(m,o(o({ref:a},d),{},{components:l})):t.createElement(m,o({ref:a},d))}));function m(e,a){var l=arguments,n=a&&a.mdxType;if("string"==typeof e||n){var r=l.length,o=new Array(r);o[0]=u;var g={};for(var i in a)hasOwnProperty.call(a,i)&&(g[i]=a[i]);g.originalType=e,g[p]="string"==typeof e?e:n,o[1]=g;for(var s=2;s<r;s++)o[s]=l[s];return t.createElement.apply(null,o)}return t.createElement.apply(null,l)}u.displayName="MDXCreateElement"},64058:(e,a,l)=>{l.r(a),l.d(a,{assets:()=>i,contentTitle:()=>o,default:()=>y,frontMatter:()=>r,metadata:()=>g,toc:()=>s});var t=l(58168),n=(l(96540),l(15680));const r={id:"cli",title:"BookKeeper CLI tool reference"},o=void 0,g={unversionedId:"reference/cli",id:"version-4.7.3/reference/cli",title:"BookKeeper CLI tool reference",description:"bookkeeper command",source:"@site/versioned_docs/version-4.7.3/reference/cli.md",sourceDirName:"reference",slug:"/reference/cli",permalink:"/docs/4.7.3/reference/cli",draft:!1,tags:[],version:"4.7.3",frontMatter:{id:"cli",title:"BookKeeper CLI tool reference"},sidebar:"version-4.7.3/docsSidebar",previous:{title:"BookKeeper configuration",permalink:"/docs/4.7.3/reference/config"}},i={},s=[{value:"<code>bookkeeper</code> command",id:"bookkeeper-command",level:2},{value:"Environment variables",id:"environment-variables",level:4},{value:"Commands",id:"commands",level:4},{value:"bookie",id:"bookkeeper-shell-bookie",level:3},{value:"Usage",id:"usage",level:5},{value:"localbookie",id:"bookkeeper-shell-localbookie",level:3},{value:"Usage",id:"usage-1",level:5},{value:"autorecovery",id:"bookkeeper-shell-autorecovery",level:3},{value:"Usage",id:"usage-2",level:5},{value:"upgrade",id:"bookkeeper-shell-upgrade",level:3},{value:"Usage",id:"usage-3",level:5},{value:"shell",id:"bookkeeper-shell-shell",level:3},{value:"Usage",id:"usage-4",level:5},{value:"help",id:"bookkeeper-shell-help",level:3},{value:"Usage",id:"usage-5",level:5},{value:"BookKeeper shell",id:"bookkeeper-shell",level:2},{value:"autorecovery",id:"bookkeeper-shell-autorecovery",level:3},{value:"Usage",id:"usage-6",level:5},{value:"bookieformat",id:"bookkeeper-shell-bookieformat",level:3},{value:"Usage",id:"usage-7",level:5},{value:"initbookie",id:"bookkeeper-shell-initbookie",level:3},{value:"Usage",id:"usage-8",level:5},{value:"bookieinfo",id:"bookkeeper-shell-bookieinfo",level:3},{value:"Usage",id:"usage-9",level:5},{value:"bookiesanity",id:"bookkeeper-shell-bookiesanity",level:3},{value:"Usage",id:"usage-10",level:5},{value:"decommissionbookie",id:"bookkeeper-shell-decommissionbookie",level:3},{value:"Usage",id:"usage-11",level:5},{value:"deleteledger",id:"bookkeeper-shell-deleteledger",level:3},{value:"Usage",id:"usage-12",level:5},{value:"endpointinfo",id:"bookkeeper-shell-endpointinfo",level:3},{value:"Usage",id:"usage-13",level:5},{value:"expandstorage",id:"bookkeeper-shell-expandstorage",level:3},{value:"Usage",id:"usage-14",level:5},{value:"help",id:"bookkeeper-shell-help",level:3},{value:"Usage",id:"usage-15",level:5},{value:"lastmark",id:"bookkeeper-shell-lastmark",level:3},{value:"Usage",id:"usage-16",level:5},{value:"ledger",id:"bookkeeper-shell-ledger",level:3},{value:"Usage",id:"usage-17",level:5},{value:"ledgermetadata",id:"bookkeeper-shell-ledgermetadata",level:3},{value:"Usage",id:"usage-18",level:5},{value:"listbookies",id:"bookkeeper-shell-listbookies",level:3},{value:"Usage",id:"usage-19",level:5},{value:"listfilesondisc",id:"bookkeeper-shell-listfilesondisc",level:3},{value:"Usage",id:"usage-20",level:5},{value:"listledgers",id:"bookkeeper-shell-listledgers",level:3},{value:"Usage",id:"usage-21",level:5},{value:"listunderreplicated",id:"bookkeeper-shell-listunderreplicated",level:3},{value:"Usage",id:"usage-22",level:5},{value:"metaformat",id:"bookkeeper-shell-metaformat",level:3},{value:"Usage",id:"usage-23",level:5},{value:"initnewcluster",id:"bookkeeper-shell-initnewcluster",level:3},{value:"Usage",id:"usage-24",level:5},{value:"nukeexistingcluster",id:"bookkeeper-shell-nukeexistingcluster",level:3},{value:"Usage",id:"usage-25",level:5},{value:"lostbookierecoverydelay",id:"bookkeeper-shell-lostbookierecoverydelay",level:3},{value:"Usage",id:"usage-26",level:5},{value:"readjournal",id:"bookkeeper-shell-readjournal",level:3},{value:"Usage",id:"usage-27",level:5},{value:"readledger",id:"bookkeeper-shell-readledger",level:3},{value:"Usage",id:"usage-28",level:5},{value:"readlog",id:"bookkeeper-shell-readlog",level:3},{value:"Usage",id:"usage-29",level:5},{value:"recover",id:"bookkeeper-shell-recover",level:3},{value:"Usage",id:"usage-30",level:5},{value:"simpletest",id:"bookkeeper-shell-simpletest",level:3},{value:"Usage",id:"usage-31",level:5},{value:"triggeraudit",id:"bookkeeper-shell-triggeraudit",level:3},{value:"Usage",id:"usage-32",level:5},{value:"updatecookie",id:"bookkeeper-shell-updatecookie",level:3},{value:"Usage",id:"usage-33",level:5},{value:"updateledgers",id:"bookkeeper-shell-updateledgers",level:3},{value:"Usage",id:"usage-34",level:5},{value:"updateBookieInLedger",id:"bookkeeper-shell-updateBookieInLedger",level:3},{value:"Usage",id:"usage-35",level:5},{value:"whoisauditor",id:"bookkeeper-shell-whoisauditor",level:3},{value:"Usage",id:"usage-36",level:5},{value:"whatisinstanceid",id:"bookkeeper-shell-whatisinstanceid",level:3},{value:"Usage",id:"usage-37",level:5},{value:"convert-to-db-storage",id:"bookkeeper-shell-convert-to-db-storage",level:3},{value:"Usage",id:"usage-38",level:5},{value:"convert-to-interleaved-storage",id:"bookkeeper-shell-convert-to-interleaved-storage",level:3},{value:"Usage",id:"usage-39",level:5},{value:"rebuild-db-ledger-locations-index",id:"bookkeeper-shell-rebuild-db-ledger-locations-index",level:3},{value:"Usage",id:"usage-40",level:5}],d={toc:s},p="wrapper";function y(e){let{components:a,...l}=e;return(0,n.yg)(p,(0,t.A)({},d,l,{components:a,mdxType:"MDXLayout"}),(0,n.yg)("h2",{id:"bookkeeper-command"},(0,n.yg)("inlineCode",{parentName:"h2"},"bookkeeper")," command"),(0,n.yg)("p",null,"Manages bookies."),(0,n.yg)("h4",{id:"environment-variables"},"Environment variables"),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Environment variable"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"),(0,n.yg)("th",{parentName:"tr",align:null},"Default"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"BOOKIE_LOG_CONF")),(0,n.yg)("td",{parentName:"tr",align:null},"The Log4j configuration file."),(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"${bookkeeperHome}/bookkeeper-server/conf/log4j.properties"))),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"BOOKIE_CONF")),(0,n.yg)("td",{parentName:"tr",align:null},"The configuration file for the bookie."),(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"${bookkeeperHome}/bookkeeper-server/conf/bk_server.conf"))),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"BOOKIE_EXTRA_CLASSPATH")),(0,n.yg)("td",{parentName:"tr",align:null},"Extra paths to add to BookKeeper's ",(0,n.yg)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/Classpath_(Java)"},"classpath"),"."),(0,n.yg)("td",{parentName:"tr",align:null})),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"ENTRY_FORMATTER_CLASS")),(0,n.yg)("td",{parentName:"tr",align:null},"The entry formatter class used to format entries."),(0,n.yg)("td",{parentName:"tr",align:null})),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"BOOKIE_PID_DIR")),(0,n.yg)("td",{parentName:"tr",align:null},"The directory where the bookie server PID file is stored."),(0,n.yg)("td",{parentName:"tr",align:null})),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},(0,n.yg)("inlineCode",{parentName:"td"},"BOOKIE_STOP_TIMEOUT")),(0,n.yg)("td",{parentName:"tr",align:null},"The wait time before forcefully killing the bookie server instance if stopping it is not successful."),(0,n.yg)("td",{parentName:"tr",align:null})))),(0,n.yg)("h4",{id:"commands"},"Commands"),(0,n.yg)("h3",{id:"bookkeeper-shell-bookie"},"bookie"),(0,n.yg)("p",null,"Starts up a bookie."),(0,n.yg)("h5",{id:"usage"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper bookie\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-localbookie"},"localbookie"),(0,n.yg)("p",null,"Starts up an ensemble of N bookies in a single JVM process. Typically used for local experimentation and development."),(0,n.yg)("h5",{id:"usage-1"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper localbookie \\ \n    N\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-autorecovery"},"autorecovery"),(0,n.yg)("p",null,"Runs the autorecovery service."),(0,n.yg)("h5",{id:"usage-2"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper autorecovery\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-upgrade"},"upgrade"),(0,n.yg)("p",null,"Upgrades the bookie's filesystem."),(0,n.yg)("h5",{id:"usage-3"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper upgrade \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"--upgrade"),(0,n.yg)("td",{parentName:"tr",align:null},"Upgrade the filesystem.")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"--rollback"),(0,n.yg)("td",{parentName:"tr",align:null},"Rollback the filesystem to a previous version.")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"--finalize"),(0,n.yg)("td",{parentName:"tr",align:null},"Mark the upgrade as complete.")))),(0,n.yg)("h3",{id:"bookkeeper-shell-shell"},"shell"),(0,n.yg)("p",null,"Runs the bookie's shell for admin commands."),(0,n.yg)("h5",{id:"usage-4"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-help"},"help"),(0,n.yg)("p",null,"Displays the help message for the ",(0,n.yg)("inlineCode",{parentName:"p"},"bookkeeper")," tool."),(0,n.yg)("h5",{id:"usage-5"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper help\n")),(0,n.yg)("h2",{id:"bookkeeper-shell"},"BookKeeper shell"),(0,n.yg)("h3",{id:"bookkeeper-shell-autorecovery"},"autorecovery"),(0,n.yg)("p",null,"Enable or disable autorecovery in the cluster."),(0,n.yg)("h5",{id:"usage-6"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell autorecovery \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-enable"),(0,n.yg)("td",{parentName:"tr",align:null},"Enable autorecovery of underreplicated ledgers")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-disable"),(0,n.yg)("td",{parentName:"tr",align:null},"Disable autorecovery of underreplicated ledgers")))),(0,n.yg)("h3",{id:"bookkeeper-shell-bookieformat"},"bookieformat"),(0,n.yg)("p",null,"Format the current server contents."),(0,n.yg)("h5",{id:"usage-7"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell bookieformat \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-nonInteractive"),(0,n.yg)("td",{parentName:"tr",align:null},"Whether to confirm if old data exists.")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-force"),(0,n.yg)("td",{parentName:"tr",align:null},"If ","[nonInteractive]"," is specified, then whether to force delete the old data without prompt..?")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-deleteCookie"),(0,n.yg)("td",{parentName:"tr",align:null},"Delete its cookie on zookeeper")))),(0,n.yg)("h3",{id:"bookkeeper-shell-initbookie"},"initbookie"),(0,n.yg)("p",null,"Initialize new bookie, by making sure that the journalDir, ledgerDirs and\nindexDirs are empty and there is no registered Bookie with this BookieId."),(0,n.yg)("p",null,"If there is data present in current bookie server, the init operation will fail. If you want to format\nthe bookie server, use ",(0,n.yg)("inlineCode",{parentName:"p"},"bookieformat"),"."),(0,n.yg)("h5",{id:"usage-8"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell initbookie\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-bookieinfo"},"bookieinfo"),(0,n.yg)("p",null,"Retrieve bookie info such as free and total disk space."),(0,n.yg)("h5",{id:"usage-9"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell bookieinfo\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-bookiesanity"},"bookiesanity"),(0,n.yg)("p",null,"Sanity test for local bookie. Create ledger and write/read entries on the local bookie."),(0,n.yg)("h5",{id:"usage-10"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell bookiesanity \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-entries N"),(0,n.yg)("td",{parentName:"tr",align:null},"Total entries to be added for the test (default 10)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-timeout N"),(0,n.yg)("td",{parentName:"tr",align:null},"Timeout for write/read operations in seconds (default 1)")))),(0,n.yg)("h3",{id:"bookkeeper-shell-decommissionbookie"},"decommissionbookie"),(0,n.yg)("p",null,"Force trigger the Audittask and make sure all the ledgers stored in the decommissioning bookie are replicated."),(0,n.yg)("h5",{id:"usage-11"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell decommissionbookie\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-deleteledger"},"deleteledger"),(0,n.yg)("p",null,"Delete a ledger"),(0,n.yg)("h5",{id:"usage-12"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell deleteledger \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-ledgerid N"),(0,n.yg)("td",{parentName:"tr",align:null},"Ledger ID")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-force"),(0,n.yg)("td",{parentName:"tr",align:null},"Whether to force delete the Ledger without prompt..?")))),(0,n.yg)("h3",{id:"bookkeeper-shell-endpointinfo"},"endpointinfo"),(0,n.yg)("p",null,"Get endpoints of a Bookie."),(0,n.yg)("h5",{id:"usage-13"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell endpointinfo\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-expandstorage"},"expandstorage"),(0,n.yg)("p",null,"Add new empty ledger/index directories. Update the directories info in the conf file before running the command."),(0,n.yg)("h5",{id:"usage-14"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell expandstorage\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-help"},"help"),(0,n.yg)("p",null,"Displays the help message."),(0,n.yg)("h5",{id:"usage-15"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell help\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-lastmark"},"lastmark"),(0,n.yg)("p",null,"Print last log marker."),(0,n.yg)("h5",{id:"usage-16"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell lastmark\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-ledger"},"ledger"),(0,n.yg)("p",null,"Dump ledger index entries into readable format."),(0,n.yg)("h5",{id:"usage-17"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell ledger \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-m LEDGER_ID"),(0,n.yg)("td",{parentName:"tr",align:null},"Print meta information")))),(0,n.yg)("h3",{id:"bookkeeper-shell-ledgermetadata"},"ledgermetadata"),(0,n.yg)("p",null,"Print the metadata for a ledger."),(0,n.yg)("h5",{id:"usage-18"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell ledgermetadata \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-ledgerid N"),(0,n.yg)("td",{parentName:"tr",align:null},"Ledger ID")))),(0,n.yg)("h3",{id:"bookkeeper-shell-listbookies"},"listbookies"),(0,n.yg)("p",null,"List the bookies, which are running as either readwrite or readonly mode."),(0,n.yg)("h5",{id:"usage-19"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell listbookies \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-readwrite"),(0,n.yg)("td",{parentName:"tr",align:null},"Print readwrite bookies")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-readonly"),(0,n.yg)("td",{parentName:"tr",align:null},"Print readonly bookies")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-hostnames"),(0,n.yg)("td",{parentName:"tr",align:null},"Also print hostname of the bookie")))),(0,n.yg)("h3",{id:"bookkeeper-shell-listfilesondisc"},"listfilesondisc"),(0,n.yg)("p",null,"List the files in JournalDirectory/LedgerDirectories/IndexDirectories."),(0,n.yg)("h5",{id:"usage-20"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell listfilesondisc \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-journal"),(0,n.yg)("td",{parentName:"tr",align:null},"Print list of journal files")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-entrylog"),(0,n.yg)("td",{parentName:"tr",align:null},"Print list of entryLog files")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-index"),(0,n.yg)("td",{parentName:"tr",align:null},"Print list of index files")))),(0,n.yg)("h3",{id:"bookkeeper-shell-listledgers"},"listledgers"),(0,n.yg)("p",null,"List all ledgers in the cluster (this may take a long time)."),(0,n.yg)("h5",{id:"usage-21"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell listledgers \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-meta"),(0,n.yg)("td",{parentName:"tr",align:null},"Print metadata")))),(0,n.yg)("h3",{id:"bookkeeper-shell-listunderreplicated"},"listunderreplicated"),(0,n.yg)("p",null,"List ledgers marked as underreplicated, with optional options to specify missing replica (BookieId) and to exclude missing replica."),(0,n.yg)("h5",{id:"usage-22"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell listunderreplicated \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-missingreplica N"),(0,n.yg)("td",{parentName:"tr",align:null},"Bookie Id of missing replica")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-excludingmissingreplica N"),(0,n.yg)("td",{parentName:"tr",align:null},"Bookie Id of missing replica to ignore")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-printmissingreplica"),(0,n.yg)("td",{parentName:"tr",align:null},"Whether to print missingreplicas list?")))),(0,n.yg)("h3",{id:"bookkeeper-shell-metaformat"},"metaformat"),(0,n.yg)("p",null,"Format Bookkeeper metadata in Zookeeper. This command is deprecated since 4.7.0,\nin favor of using ",(0,n.yg)("inlineCode",{parentName:"p"},"initnewcluster")," for initializing a new cluster and ",(0,n.yg)("inlineCode",{parentName:"p"},"nukeexistingcluster")," for nuking an existing cluster."),(0,n.yg)("h5",{id:"usage-23"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell metaformat \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-nonInteractive"),(0,n.yg)("td",{parentName:"tr",align:null},"Whether to confirm if old data exists..?")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-force"),(0,n.yg)("td",{parentName:"tr",align:null},"If ","[nonInteractive]"," is specified, then whether to force delete the old data without prompt.")))),(0,n.yg)("h3",{id:"bookkeeper-shell-initnewcluster"},"initnewcluster"),(0,n.yg)("p",null,"Initializes a new bookkeeper cluster. If initnewcluster fails then try nuking\nexisting cluster by running nukeexistingcluster before running initnewcluster again"),(0,n.yg)("h5",{id:"usage-24"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell initnewcluster\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-nukeexistingcluster"},"nukeexistingcluster"),(0,n.yg)("p",null,"Nuke bookkeeper cluster by deleting metadata"),(0,n.yg)("h5",{id:"usage-25"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell nukeexistingcluster \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-zkledgersrootpath"),(0,n.yg)("td",{parentName:"tr",align:null},"zookeeper ledgers rootpath")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-instanceid"),(0,n.yg)("td",{parentName:"tr",align:null},"instance id")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-force"),(0,n.yg)("td",{parentName:"tr",align:null},"If instanceid is not specified, then whether to force nuke the metadata without validating instanceid")))),(0,n.yg)("h3",{id:"bookkeeper-shell-lostbookierecoverydelay"},"lostbookierecoverydelay"),(0,n.yg)("p",null,"Setter and Getter for LostBookieRecoveryDelay value (in seconds) in Zookeeper."),(0,n.yg)("h5",{id:"usage-26"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell lostbookierecoverydelay \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-get"),(0,n.yg)("td",{parentName:"tr",align:null},"Get LostBookieRecoveryDelay value (in seconds)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-set N"),(0,n.yg)("td",{parentName:"tr",align:null},"Set LostBookieRecoveryDelay value (in seconds)")))),(0,n.yg)("h3",{id:"bookkeeper-shell-readjournal"},"readjournal"),(0,n.yg)("p",null,"Scan a journal file and format the entries into readable format."),(0,n.yg)("h5",{id:"usage-27"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell readjournal \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-msg JOURNAL_ID"),(0,n.yg)("td",{parentName:"tr",align:null},"JOURNAL_FILENAME")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-dir"),(0,n.yg)("td",{parentName:"tr",align:null},"Journal directory (needed if more than one journal configured)")))),(0,n.yg)("h3",{id:"bookkeeper-shell-readledger"},"readledger"),(0,n.yg)("p",null,"Read a range of entries from a ledger."),(0,n.yg)("h5",{id:"usage-28"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell readledger \\ \n    <ledger_id> [<start_entry_id> [<end_entry_id>]]\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-readlog"},"readlog"),(0,n.yg)("p",null,"Scan an entry file and format the entries into readable format."),(0,n.yg)("h5",{id:"usage-29"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell readlog \\ \n    <entry_log_id | entry_log_file_name> \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-msg"),(0,n.yg)("td",{parentName:"tr",align:null},"Print message body")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-ledgerid N"),(0,n.yg)("td",{parentName:"tr",align:null},"Ledger ID")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-entryid N"),(0,n.yg)("td",{parentName:"tr",align:null},"Entry ID")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-startpos N"),(0,n.yg)("td",{parentName:"tr",align:null},"Start Position")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-endpos"),(0,n.yg)("td",{parentName:"tr",align:null},"End Position")))),(0,n.yg)("h3",{id:"bookkeeper-shell-recover"},"recover"),(0,n.yg)("p",null,"Recover the ledger data for failed bookie."),(0,n.yg)("h5",{id:"usage-30"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell recover \\ \n    <bookieSrc[,bookieSrc,...]> \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-deleteCookie"),(0,n.yg)("td",{parentName:"tr",align:null},"Delete cookie node for the bookie.")))),(0,n.yg)("h3",{id:"bookkeeper-shell-simpletest"},"simpletest"),(0,n.yg)("p",null,"Simple test to create a ledger and write entries to it."),(0,n.yg)("h5",{id:"usage-31"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell simpletest \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-ensemble N"),(0,n.yg)("td",{parentName:"tr",align:null},"Ensemble size (default 3)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-writeQuorum N"),(0,n.yg)("td",{parentName:"tr",align:null},"Write quorum size (default 2)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"ackQuorum N"),(0,n.yg)("td",{parentName:"tr",align:null},"Ack quorum size (default 2)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-numEntries N"),(0,n.yg)("td",{parentName:"tr",align:null},"Entries to write (default 1000)")))),(0,n.yg)("h3",{id:"bookkeeper-shell-triggeraudit"},"triggeraudit"),(0,n.yg)("p",null,"Force trigger the Audit by resetting the lostBookieRecoveryDelay."),(0,n.yg)("h5",{id:"usage-32"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell triggeraudit\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-updatecookie"},"updatecookie"),(0,n.yg)("p",null,"Update bookie id in cookie."),(0,n.yg)("h5",{id:"usage-33"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell updatecookie \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-bookieId <hostname"),(0,n.yg)("td",{parentName:"tr",align:null},"ip>")))),(0,n.yg)("h3",{id:"bookkeeper-shell-updateledgers"},"updateledgers"),(0,n.yg)("p",null,"Update bookie id in ledgers (this may take a long time)."),(0,n.yg)("h5",{id:"usage-34"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell updateledgers \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-bookieId <hostname"),(0,n.yg)("td",{parentName:"tr",align:null},"ip>")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-updatespersec N"),(0,n.yg)("td",{parentName:"tr",align:null},"Number of ledgers updating per second (default 5 per sec)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-limit N"),(0,n.yg)("td",{parentName:"tr",align:null},"Maximum number of ledgers to update (default no limit)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-verbose"),(0,n.yg)("td",{parentName:"tr",align:null},"Print status of the ledger updation (default false)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-printprogress N"),(0,n.yg)("td",{parentName:"tr",align:null},"Print messages on every configured seconds if verbose turned on (default 10 secs)")))),(0,n.yg)("h3",{id:"bookkeeper-shell-updateBookieInLedger"},"updateBookieInLedger"),(0,n.yg)("p",null,"Replace srcBookie with destBookie in ledger metadata. (this may take a long time).\nUseful when Host-reip or data-migration. In that case, shutdown bookie process in src-bookie,\nuse this command to update ledger metadata by replacing src-bookie to dest-bookie where data has been copied/moved.\nStart the bookie process on dest-bookie and dest-bookie will serve copied ledger data from src-bookie."),(0,n.yg)("h5",{id:"usage-35"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell updateBookieInLedger \\ \n    <options>\n")),(0,n.yg)("table",null,(0,n.yg)("thead",{parentName:"table"},(0,n.yg)("tr",{parentName:"thead"},(0,n.yg)("th",{parentName:"tr",align:null},"Flag"),(0,n.yg)("th",{parentName:"tr",align:null},"Description"))),(0,n.yg)("tbody",{parentName:"table"},(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-srcBookie BOOKIE_ID"),(0,n.yg)("td",{parentName:"tr",align:null},"Source Bookie Id")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-destBookie BOOKIE_ID"),(0,n.yg)("td",{parentName:"tr",align:null},"Destination Bookie Id")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-updatespersec N"),(0,n.yg)("td",{parentName:"tr",align:null},"Number of ledgers updating per second (default 5 per sec)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-limit N"),(0,n.yg)("td",{parentName:"tr",align:null},"Maximum number of ledgers to update (default no limit)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-verbose"),(0,n.yg)("td",{parentName:"tr",align:null},"Print status of the ledger updation (default false)")),(0,n.yg)("tr",{parentName:"tbody"},(0,n.yg)("td",{parentName:"tr",align:null},"-printprogress N"),(0,n.yg)("td",{parentName:"tr",align:null},"Print messages on every configured seconds if verbose turned on (default 10 secs)")))),(0,n.yg)("h3",{id:"bookkeeper-shell-whoisauditor"},"whoisauditor"),(0,n.yg)("p",null,"Print the node which holds the auditor lock"),(0,n.yg)("h5",{id:"usage-36"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell whoisauditor\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-whatisinstanceid"},"whatisinstanceid"),(0,n.yg)("p",null,"Print the instanceid of the cluster"),(0,n.yg)("h5",{id:"usage-37"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell whatisinstanceid\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-convert-to-db-storage"},"convert-to-db-storage"),(0,n.yg)("p",null,"Convert bookie indexes from InterleavedStorage to DbLedgerStorage format"),(0,n.yg)("h5",{id:"usage-38"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell convert-to-db-storage\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-convert-to-interleaved-storage"},"convert-to-interleaved-storage"),(0,n.yg)("p",null,"Convert bookie indexes from DbLedgerStorage to InterleavedStorage format"),(0,n.yg)("h5",{id:"usage-39"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell convert-to-interleaved-storage\n")),(0,n.yg)("h3",{id:"bookkeeper-shell-rebuild-db-ledger-locations-index"},"rebuild-db-ledger-locations-index"),(0,n.yg)("p",null,"Rebuild DbLedgerStorage locations index"),(0,n.yg)("h5",{id:"usage-40"},"Usage"),(0,n.yg)("pre",null,(0,n.yg)("code",{parentName:"pre",className:"language-shell"},"$ bin/bookkeeper shell rebuild-db-ledger-locations-index\n")))}y.isMDXComponent=!0}}]);