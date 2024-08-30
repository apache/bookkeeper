"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[6640],{15680:(e,t,a)=>{a.d(t,{xA:()=>s,yg:()=>c});var n=a(96540);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},l=Object.keys(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var g=n.createContext({}),d=function(e){var t=n.useContext(g),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},s=function(e){var t=d(e.components);return n.createElement(g.Provider,{value:t},e.children)},u="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},y=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,l=e.originalType,g=e.parentName,s=o(e,["components","mdxType","originalType","parentName"]),u=d(a),y=r,c=u["".concat(g,".").concat(y)]||u[y]||p[y]||l;return a?n.createElement(c,i(i({ref:t},s),{},{components:a})):n.createElement(c,i({ref:t},s))}));function c(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=a.length,i=new Array(l);i[0]=y;var o={};for(var g in t)hasOwnProperty.call(t,g)&&(o[g]=t[g]);o.originalType=e,o[u]="string"==typeof e?e:r,i[1]=o;for(var d=2;d<l;d++)i[d]=a[d];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}y.displayName="MDXCreateElement"},53637:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>g,contentTitle:()=>i,default:()=>p,frontMatter:()=>l,metadata:()=>o,toc:()=>d});var n=a(9668),r=(a(96540),a(15680));const l={id:"upgrade",title:"Upgrade"},i=void 0,o={unversionedId:"admin/upgrade",id:"version-4.17.1/admin/upgrade",title:"Upgrade",description:"If you have questions about upgrades (or need help), please feel free to reach out to us by mailing list or Slack Channel.",source:"@site/versioned_docs/version-4.17.1/admin/upgrade.md",sourceDirName:"admin",slug:"/admin/upgrade",permalink:"/docs/admin/upgrade",draft:!1,tags:[],version:"4.17.1",frontMatter:{id:"upgrade",title:"Upgrade"},sidebar:"docsSidebar",previous:{title:"Metric collection",permalink:"/docs/admin/metrics"},next:{title:"BookKeeper Admin REST API",permalink:"/docs/admin/http"}},g={},d=[{value:"Overview",id:"overview",level:2},{value:"Canary",id:"canary",level:2},{value:"Rollback Canaries",id:"rollback-canaries",level:3},{value:"Upgrade Steps",id:"upgrade-steps",level:2},{value:"Upgrade Bookies",id:"upgrade-bookies",level:3},{value:"Upgrade Guides",id:"upgrade-guides",level:2},{value:"4.6.x to 4.7.0 upgrade",id:"46x-to-470-upgrade",level:3},{value:"Common Configuration Changes",id:"common-configuration-changes",level:4},{value:"New Settings",id:"new-settings",level:5},{value:"Deprecated Settings",id:"deprecated-settings",level:5},{value:"Changed Settings",id:"changed-settings",level:5},{value:"Server Configuration Changes",id:"server-configuration-changes",level:4},{value:"New Settings",id:"new-settings-1",level:5},{value:"Deprecated Settings",id:"deprecated-settings-1",level:5},{value:"Changed Settings",id:"changed-settings-1",level:5},{value:"Client Configuration Changes",id:"client-configuration-changes",level:4},{value:"New Settings",id:"new-settings-2",level:5},{value:"Deprecated Settings",id:"deprecated-settings-2",level:5},{value:"Changed Settings",id:"changed-settings-2",level:5},{value:"4.7.x to 4.8.X upgrade",id:"47x-to-48x-upgrade",level:3},{value:"4.5.x to 4.6.x upgrade",id:"45x-to-46x-upgrade",level:3},{value:"4.4.x to 4.5.x upgrade",id:"44x-to-45x-upgrade",level:3}],s={toc:d},u="wrapper";function p(e){let{components:t,...a}=e;return(0,r.yg)(u,(0,n.A)({},s,a,{components:t,mdxType:"MDXLayout"}),(0,r.yg)("blockquote",null,(0,r.yg)("p",{parentName:"blockquote"},"If you have questions about upgrades (or need help), please feel free to reach out to us by ",(0,r.yg)("a",{parentName:"p",href:"/community/mailing-lists"},"mailing list")," or ",(0,r.yg)("a",{parentName:"p",href:"/community/slack"},"Slack Channel"),".")),(0,r.yg)("h2",{id:"overview"},"Overview"),(0,r.yg)("p",null,"Consider the below guidelines in preparation for upgrading."),(0,r.yg)("ul",null,(0,r.yg)("li",{parentName:"ul"},"Always back up all your configuration files before upgrading."),(0,r.yg)("li",{parentName:"ul"},"Read through the documentation and draft an upgrade plan that matches your specific requirements and environment before starting the upgrade process.\nPut differently, don't start working through the guide on a live cluster. Read guide entirely, make a plan, then execute the plan."),(0,r.yg)("li",{parentName:"ul"},"Pay careful consideration to the order in which components are upgraded. In general, you need to upgrade bookies first and then upgrade your clients."),(0,r.yg)("li",{parentName:"ul"},"If autorecovery is running along with bookies, you need to pay attention to the upgrade sequence."),(0,r.yg)("li",{parentName:"ul"},"Read the release notes carefully for each release. They contain not only information about noteworthy features, but also changes to configurations\nthat may impact your upgrade."),(0,r.yg)("li",{parentName:"ul"},"Always upgrade one or a small set of bookies to canary new version before upgraing all bookies in your cluster.")),(0,r.yg)("h2",{id:"canary"},"Canary"),(0,r.yg)("p",null,"It is wise to canary an upgraded version in one or small set of bookies before upgrading all bookies in your live cluster."),(0,r.yg)("p",null,"You can follow below steps on how to canary a upgraded version:"),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},"Stop a Bookie."),(0,r.yg)("li",{parentName:"ol"},"Upgrade the binary and configuration."),(0,r.yg)("li",{parentName:"ol"},"Start the Bookie in ",(0,r.yg)("inlineCode",{parentName:"li"},"ReadOnly")," mode. This can be used to verify if the Bookie of this new version can run well for read workload."),(0,r.yg)("li",{parentName:"ol"},"Once the Bookie is running at ",(0,r.yg)("inlineCode",{parentName:"li"},"ReadOnly")," mode successfully for a while, restart the Bookie in ",(0,r.yg)("inlineCode",{parentName:"li"},"Write/Read")," mode."),(0,r.yg)("li",{parentName:"ol"},"After step 4, the Bookie will serve both write and read traffic.")),(0,r.yg)("h3",{id:"rollback-canaries"},"Rollback Canaries"),(0,r.yg)("p",null,"If problems occur during canarying an upgraded version, you can simply take down the problematic Bookie node. The remain bookies in the old cluster\nwill repair this problematic bookie node by autorecovery. Nothing needs to be worried about."),(0,r.yg)("h2",{id:"upgrade-steps"},"Upgrade Steps"),(0,r.yg)("p",null,"Once you determined a version is safe to upgrade in a few nodes in your cluster, you can perform following steps to upgrade all bookies in your cluster."),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},"Determine if autorecovery is running along with bookies. If yes, check if the clients (either new clients with new binary or old clients with new configurations)\nare allowed to talk to old bookies; if clients are not allowed to talk to old bookies, please ",(0,r.yg)("a",{parentName:"li",href:"../reference/cli/#autorecovery-1"},"disable autorecovery")," during upgrade."),(0,r.yg)("li",{parentName:"ol"},"Decide on performing a rolling upgrade or a downtime upgrade."),(0,r.yg)("li",{parentName:"ol"},"Upgrade all Bookies (more below)"),(0,r.yg)("li",{parentName:"ol"},"If autorecovery was disabled during upgrade, ",(0,r.yg)("a",{parentName:"li",href:"../reference/cli/#autorecovery-1"},"enable autorecovery"),"."),(0,r.yg)("li",{parentName:"ol"},"After all bookies are upgraded, build applications that use ",(0,r.yg)("inlineCode",{parentName:"li"},"BookKeeper client")," against the new bookkeeper libraries and deploy the new versions.")),(0,r.yg)("h3",{id:"upgrade-bookies"},"Upgrade Bookies"),(0,r.yg)("p",null,"In a rolling upgrade scenario, upgrade one Bookie at a time. In a downtime upgrade scenario, take the entire cluster down, upgrade each Bookie, then start the cluster."),(0,r.yg)("p",null,"For each Bookie:"),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},"Stop the bookie."),(0,r.yg)("li",{parentName:"ol"},"Upgrade the software (either new binary or new configuration)"),(0,r.yg)("li",{parentName:"ol"},"Start the bookie.")),(0,r.yg)("h2",{id:"upgrade-guides"},"Upgrade Guides"),(0,r.yg)("p",null,"We describes the general upgrade method in Apache BookKeeper as above. We will cover the details for individual versions."),(0,r.yg)("h3",{id:"46x-to-470-upgrade"},"4.6.x to 4.7.0 upgrade"),(0,r.yg)("p",null,"There isn't any protocol related backward compabilities changes in 4.7.0. So you can follow the general upgrade sequence to upgrade from 4.6.x to 4.7.0."),(0,r.yg)("p",null,"However, we list a list of changes that you might want to know."),(0,r.yg)("h4",{id:"common-configuration-changes"},"Common Configuration Changes"),(0,r.yg)("p",null,"This section documents the common configuration changes that applied for both clients and servers."),(0,r.yg)("h5",{id:"new-settings"},"New Settings"),(0,r.yg)("p",null,"Following settings are newly added in 4.7.0."),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:null},"Name"),(0,r.yg)("th",{parentName:"tr",align:null},"Default Value"),(0,r.yg)("th",{parentName:"tr",align:null},"Description"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"allowShadedLedgerManagerFactoryClass"),(0,r.yg)("td",{parentName:"tr",align:null},"false"),(0,r.yg)("td",{parentName:"tr",align:null},"The allows bookkeeper client to connect to a bookkeeper cluster using a shaded ledger manager factory")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"shadedLedgerManagerFactoryClassPrefix"),(0,r.yg)("td",{parentName:"tr",align:null},(0,r.yg)("inlineCode",{parentName:"td"},"dlshade.")),(0,r.yg)("td",{parentName:"tr",align:null},"The shaded ledger manager factory prefix. This is used when ",(0,r.yg)("inlineCode",{parentName:"td"},"allowShadedLedgerManagerFactoryClass")," is set to true")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"metadataServiceUri"),(0,r.yg)("td",{parentName:"tr",align:null},"null"),(0,r.yg)("td",{parentName:"tr",align:null},"metadata service uri that bookkeeper is used for loading corresponding metadata driver and resolving its metadata service location")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"permittedStartupUsers"),(0,r.yg)("td",{parentName:"tr",align:null},"null"),(0,r.yg)("td",{parentName:"tr",align:null},"The list of users are permitted to run the bookie process. Any users can run the bookie process if it is not set")))),(0,r.yg)("h5",{id:"deprecated-settings"},"Deprecated Settings"),(0,r.yg)("p",null,"There are no common settings deprecated at 4.7.0."),(0,r.yg)("h5",{id:"changed-settings"},"Changed Settings"),(0,r.yg)("p",null,"There are no common settings whose default value are changed at 4.7.0."),(0,r.yg)("h4",{id:"server-configuration-changes"},"Server Configuration Changes"),(0,r.yg)("h5",{id:"new-settings-1"},"New Settings"),(0,r.yg)("p",null,"Following settings are newly added in 4.7.0."),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:null},"Name"),(0,r.yg)("th",{parentName:"tr",align:null},"Default Value"),(0,r.yg)("th",{parentName:"tr",align:null},"Description"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"verifyMetadataOnGC"),(0,r.yg)("td",{parentName:"tr",align:null},"false"),(0,r.yg)("td",{parentName:"tr",align:null},"Whether the bookie is configured to double check the ledgers' metadata prior to garbage collecting them")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"auditorLedgerVerificationPercentage"),(0,r.yg)("td",{parentName:"tr",align:null},"0"),(0,r.yg)("td",{parentName:"tr",align:null},"The percentage of a ledger (fragment)'s entries will be verified by Auditor before claiming a ledger (fragment) is missing")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"numHighPriorityWorkerThreads"),(0,r.yg)("td",{parentName:"tr",align:null},"8"),(0,r.yg)("td",{parentName:"tr",align:null},"The number of threads that should be used for high priority requests (i.e. recovery reads and adds, and fencing). If zero, reads are handled by Netty threads directly.")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"useShortHostName"),(0,r.yg)("td",{parentName:"tr",align:null},"false"),(0,r.yg)("td",{parentName:"tr",align:null},"Whether the bookie should use short hostname or ",(0,r.yg)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/Fully_qualified_domain_name"},"FQDN")," hostname for registration and ledger metadata when useHostNameAsBookieID is enabled.")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"minUsableSizeForEntryLogCreation"),(0,r.yg)("td",{parentName:"tr",align:null},"1.2 * ",(0,r.yg)("inlineCode",{parentName:"td"},"logSizeLimit")),(0,r.yg)("td",{parentName:"tr",align:null},"Minimum safe usable size to be available in ledger directory for bookie to create entry log files (in bytes).")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"minUsableSizeForHighPriorityWrites"),(0,r.yg)("td",{parentName:"tr",align:null},"1.2 * ",(0,r.yg)("inlineCode",{parentName:"td"},"logSizeLimit")),(0,r.yg)("td",{parentName:"tr",align:null},"Minimum safe usable size to be available in ledger directory for bookie to accept high priority writes even it is in readonly mode.")))),(0,r.yg)("h5",{id:"deprecated-settings-1"},"Deprecated Settings"),(0,r.yg)("p",null,"Following settings are deprecated since 4.7.0."),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:null},"Name"),(0,r.yg)("th",{parentName:"tr",align:null},"Description"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"registrationManagerClass"),(0,r.yg)("td",{parentName:"tr",align:null},"The registration manager class used by server to discover registration manager. It is replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"metadataServiceUri"),".")))),(0,r.yg)("h5",{id:"changed-settings-1"},"Changed Settings"),(0,r.yg)("p",null,"The default values of following settings are changed since 4.7.0."),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:null},"Name"),(0,r.yg)("th",{parentName:"tr",align:null},"Old Default Value"),(0,r.yg)("th",{parentName:"tr",align:null},"New Default Value"),(0,r.yg)("th",{parentName:"tr",align:null},"Notes"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"numLongPollWorkerThreads"),(0,r.yg)("td",{parentName:"tr",align:null},"10"),(0,r.yg)("td",{parentName:"tr",align:null},"0"),(0,r.yg)("td",{parentName:"tr",align:null},"If the number of threads is zero or negative, bookie can fallback to use read threads for long poll. This allows not creating threads if application doesn't use long poll feature.")))),(0,r.yg)("h4",{id:"client-configuration-changes"},"Client Configuration Changes"),(0,r.yg)("h5",{id:"new-settings-2"},"New Settings"),(0,r.yg)("p",null,"Following settings are newly added in 4.7.0."),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:null},"Name"),(0,r.yg)("th",{parentName:"tr",align:null},"Default Value"),(0,r.yg)("th",{parentName:"tr",align:null},"Description"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"maxNumEnsembleChanges"),(0,r.yg)("td",{parentName:"tr",align:null},"Integer.MAX","_","VALUE"),(0,r.yg)("td",{parentName:"tr",align:null},"The max allowed ensemble change number before sealing a ledger on failures")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"timeoutMonitorIntervalSec"),(0,r.yg)("td",{parentName:"tr",align:null},"min(",(0,r.yg)("inlineCode",{parentName:"td"},"addEntryTimeoutSec"),", ",(0,r.yg)("inlineCode",{parentName:"td"},"addEntryQuorumTimeoutSec"),", ",(0,r.yg)("inlineCode",{parentName:"td"},"readEntryTimeoutSec"),")"),(0,r.yg)("td",{parentName:"tr",align:null},"The interval between successive executions of the operation timeout monitor, in seconds")),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"ensemblePlacementPolicyOrderSlowBookies"),(0,r.yg)("td",{parentName:"tr",align:null},"false"),(0,r.yg)("td",{parentName:"tr",align:null},"Flag to enable/disable reordering slow bookies in placement policy")))),(0,r.yg)("h5",{id:"deprecated-settings-2"},"Deprecated Settings"),(0,r.yg)("p",null,"Following settings are deprecated since 4.7.0."),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:null},"Name"),(0,r.yg)("th",{parentName:"tr",align:null},"Description"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"clientKeyStoreType"),(0,r.yg)("td",{parentName:"tr",align:null},"Replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"tlsKeyStoreType"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"clientKeyStore"),(0,r.yg)("td",{parentName:"tr",align:null},"Replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"tlsKeyStore"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"clientKeyStorePasswordPath"),(0,r.yg)("td",{parentName:"tr",align:null},"Replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"tlsKeyStorePasswordPath"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"clientTrustStoreType"),(0,r.yg)("td",{parentName:"tr",align:null},"Replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"tlsTrustStoreType"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"clientTrustStore"),(0,r.yg)("td",{parentName:"tr",align:null},"Replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"tlsTrustStore"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"clientTrustStorePasswordPath"),(0,r.yg)("td",{parentName:"tr",align:null},"Replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"tlsTrustStorePasswordPath"))),(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"registrationClientClass"),(0,r.yg)("td",{parentName:"tr",align:null},"The registration client class used by client to discover registration service. It is replaced by ",(0,r.yg)("inlineCode",{parentName:"td"},"metadataServiceUri"),".")))),(0,r.yg)("h5",{id:"changed-settings-2"},"Changed Settings"),(0,r.yg)("p",null,"The default values of following settings are changed since 4.7.0."),(0,r.yg)("table",null,(0,r.yg)("thead",{parentName:"table"},(0,r.yg)("tr",{parentName:"thead"},(0,r.yg)("th",{parentName:"tr",align:null},"Name"),(0,r.yg)("th",{parentName:"tr",align:null},"Old Default Value"),(0,r.yg)("th",{parentName:"tr",align:null},"New Default Value"),(0,r.yg)("th",{parentName:"tr",align:null},"Notes"))),(0,r.yg)("tbody",{parentName:"table"},(0,r.yg)("tr",{parentName:"tbody"},(0,r.yg)("td",{parentName:"tr",align:null},"enableDigestTypeAutodetection"),(0,r.yg)("td",{parentName:"tr",align:null},"false"),(0,r.yg)("td",{parentName:"tr",align:null},"true"),(0,r.yg)("td",{parentName:"tr",align:null},"Autodetect the digest type and passwd when opening a ledger. It will ignore the provided digest type, but still verify the provided passwd.")))),(0,r.yg)("h3",{id:"47x-to-48x-upgrade"},"4.7.x to 4.8.X upgrade"),(0,r.yg)("p",null,"In 4.8.x a new feature is added to persist explicitLac in FileInfo and explicitLac entry in Journal. (Note: Currently this feature is not available if your ledgerStorageClass is DbLedgerStorage, ISSUE #1533 is going to address it) Hence current journal format version is bumped to 6 and current FileInfo header version is bumped to 1. But since default config values of 'journalFormatVersionToWrite' and 'fileInfoFormatVersionToWrite' are set to older versions, this feature is off by default. To enable this feature those config values should be set to current versions. Once this is enabled then we cannot rollback to previous Bookie versions (4.7.x and older), since older version code would not be able to deal with explicitLac entry in Journal file while replaying journal and also reading Header of Index files / FileInfo would fail reading Index files with newer FileInfo version. So in summary, it is a non-rollbackable feature and it applies even if explicitLac is not being used."),(0,r.yg)("h3",{id:"45x-to-46x-upgrade"},"4.5.x to 4.6.x upgrade"),(0,r.yg)("p",null,"There isn't any protocol related backward compabilities changes in 4.6.x. So you can follow the general upgrade sequence to upgrade from 4.5.x to 4.6.x."),(0,r.yg)("h3",{id:"44x-to-45x-upgrade"},"4.4.x to 4.5.x upgrade"),(0,r.yg)("p",null,"There isn't any protocol related backward compabilities changes in 4.5.0. So you can follow the general upgrade sequence to upgrade from 4.4.x to 4.5.x.\nHowever, we list a list of things that you might want to know."),(0,r.yg)("ol",null,(0,r.yg)("li",{parentName:"ol"},"4.5.x upgrades netty from 3.x to 4.x. The memory usage pattern might be changed a bit. Netty 4 uses more direct memory. Please pay attention to your memory usage\nand adjust the JVM settings accordingly."),(0,r.yg)("li",{parentName:"ol"},(0,r.yg)("inlineCode",{parentName:"li"},"multi journals")," is a non-rollbackable feature. If you configure a bookie to use multiple journals on 4.5.x you can not roll the bookie back to use 4.4.x. You have\nto take a bookie out and recover it if you want to rollback to 4.4.x.")),(0,r.yg)("p",null,"If you are planning to upgrade a non-secured cluster to a secured cluster enabling security features in 4.5.0, please read ",(0,r.yg)("a",{parentName:"p",href:"../security/overview"},"BookKeeper Security")," for more details."))}p.isMDXComponent=!0}}]);