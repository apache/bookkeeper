"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[4325],{15680:(e,r,t)=>{t.d(r,{xA:()=>p,yg:()=>g});var o=t(96540);function a(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function n(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);r&&(o=o.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,o)}return t}function i(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?n(Object(t),!0).forEach((function(r){a(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):n(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function l(e,r){if(null==e)return{};var t,o,a=function(e,r){if(null==e)return{};var t,o,a={},n=Object.keys(e);for(o=0;o<n.length;o++)t=n[o],r.indexOf(t)>=0||(a[t]=e[t]);return a}(e,r);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(o=0;o<n.length;o++)t=n[o],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var c=o.createContext({}),s=function(e){var r=o.useContext(c),t=r;return e&&(t="function"==typeof e?e(r):i(i({},r),e)),t},p=function(e){var r=s(e.components);return o.createElement(c.Provider,{value:r},e.children)},h="mdxType",d={inlineCode:"code",wrapper:function(e){var r=e.children;return o.createElement(o.Fragment,{},r)}},u=o.forwardRef((function(e,r){var t=e.components,a=e.mdxType,n=e.originalType,c=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),h=s(t),u=a,g=h["".concat(c,".").concat(u)]||h[u]||d[u]||n;return t?o.createElement(g,i(i({ref:r},p),{},{components:t})):o.createElement(g,i({ref:r},p))}));function g(e,r){var t=arguments,a=r&&r.mdxType;if("string"==typeof e||a){var n=t.length,i=new Array(n);i[0]=u;var l={};for(var c in r)hasOwnProperty.call(r,c)&&(l[c]=r[c]);l.originalType=e,l[h]="string"==typeof e?e:a,i[1]=l;for(var s=2;s<n;s++)i[s]=t[s];return o.createElement.apply(null,i)}return o.createElement.apply(null,t)}u.displayName="MDXCreateElement"},17867:(e,r,t)=>{t.r(r),t.d(r,{assets:()=>c,contentTitle:()=>i,default:()=>d,frontMatter:()=>n,metadata:()=>l,toc:()=>s});var o=t(9668),a=(t(96540),t(15680));const n={id:"autorecovery",title:"Using AutoRecovery"},i=void 0,l={unversionedId:"admin/autorecovery",id:"version-4.11.1/admin/autorecovery",title:"Using AutoRecovery",description:"When a bookie crashes, all ledgers on that bookie become under-replicated. In order to bring all ledgers in your BookKeeper cluster back to full replication, you'll need to recover the data from any offline bookies. There are two ways to recover bookies' data:",source:"@site/versioned_docs/version-4.11.1/admin/autorecovery.md",sourceDirName:"admin",slug:"/admin/autorecovery",permalink:"/docs/4.11.1/admin/autorecovery",draft:!1,tags:[],version:"4.11.1",frontMatter:{id:"autorecovery",title:"Using AutoRecovery"},sidebar:"version-4.11.1/docsSidebar",previous:{title:"BookKeeper administration",permalink:"/docs/4.11.1/admin/bookies"},next:{title:"Metric collection",permalink:"/docs/4.11.1/admin/metrics"}},c={},s=[{value:"Manual recovery",id:"manual-recovery",level:2},{value:"The manual recovery process",id:"the-manual-recovery-process",level:3},{value:"AutoRecovery",id:"autorecovery",level:2},{value:"Running AutoRecovery",id:"running-autorecovery",level:2},{value:"Configuration",id:"configuration",level:2},{value:"Disable AutoRecovery",id:"disable-autorecovery",level:2},{value:"AutoRecovery architecture",id:"autorecovery-architecture",level:2},{value:"Auditor",id:"auditor",level:3},{value:"Replication Worker",id:"replication-worker",level:3},{value:"The rereplication process",id:"the-rereplication-process",level:3}],p={toc:s},h="wrapper";function d(e){let{components:r,...t}=e;return(0,a.yg)(h,(0,o.A)({},p,t,{components:r,mdxType:"MDXLayout"}),(0,a.yg)("p",null,"When a bookie crashes, all ledgers on that bookie become under-replicated. In order to bring all ledgers in your BookKeeper cluster back to full replication, you'll need to ",(0,a.yg)("em",{parentName:"p"},"recover")," the data from any offline bookies. There are two ways to recover bookies' data:"),(0,a.yg)("ol",null,(0,a.yg)("li",{parentName:"ol"},"Using ",(0,a.yg)("a",{parentName:"li",href:"#manual-recovery"},"manual recovery")),(0,a.yg)("li",{parentName:"ol"},"Automatically, using ",(0,a.yg)("a",{parentName:"li",href:"#autorecovery"},(0,a.yg)("em",{parentName:"a"},"AutoRecovery")))),(0,a.yg)("h2",{id:"manual-recovery"},"Manual recovery"),(0,a.yg)("p",null,"You can manually recover failed bookies using the ",(0,a.yg)("a",{parentName:"p",href:"../reference/cli"},(0,a.yg)("inlineCode",{parentName:"a"},"bookkeeper"))," command-line tool. You need to specify:"),(0,a.yg)("ul",null,(0,a.yg)("li",{parentName:"ul"},"the ",(0,a.yg)("inlineCode",{parentName:"li"},"shell recover")," option "),(0,a.yg)("li",{parentName:"ul"},"the IP and port for the failed bookie")),(0,a.yg)("p",null,"Here's an example:"),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-bash"},"$ bin/bookkeeper shell recover \\\n  192.168.1.10:3181      # IP and port for the failed bookie\n")),(0,a.yg)("p",null,"If you wish, you can also specify which ledgers you'd like to recover. Here's an example:"),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-bash"},"$ bin/bookkeeper shell recover \\\n  192.168.1.10:3181 \\    # IP and port for the failed bookie\n  --ledger ledgerID      # ledgerID which you want to recover \n")),(0,a.yg)("h3",{id:"the-manual-recovery-process"},"The manual recovery process"),(0,a.yg)("p",null,"When you initiate a manual recovery process, the following happens:"),(0,a.yg)("ol",null,(0,a.yg)("li",{parentName:"ol"},"The client (the process running ) reads the metadata of active ledgers from ZooKeeper."),(0,a.yg)("li",{parentName:"ol"},"The ledgers that contain fragments from the failed bookie in their ensemble are selected."),(0,a.yg)("li",{parentName:"ol"},"A recovery process is initiated for each ledger in this list and the rereplication process is run for each ledger."),(0,a.yg)("li",{parentName:"ol"},"Once all the ledgers are marked as fully replicated, bookie recovery is finished.")),(0,a.yg)("h2",{id:"autorecovery"},"AutoRecovery"),(0,a.yg)("p",null,"AutoRecovery is a process that:"),(0,a.yg)("ul",null,(0,a.yg)("li",{parentName:"ul"},"automatically detects when a bookie in your BookKeeper cluster has become unavailable and then"),(0,a.yg)("li",{parentName:"ul"},"rereplicates all the ledgers that were stored on that bookie.")),(0,a.yg)("p",null,"AutoRecovery can be run in two ways:"),(0,a.yg)("ol",null,(0,a.yg)("li",{parentName:"ol"},"On dedicated nodes in your BookKeeper cluster"),(0,a.yg)("li",{parentName:"ol"},"On the same machines on which your bookies are running")),(0,a.yg)("h2",{id:"running-autorecovery"},"Running AutoRecovery"),(0,a.yg)("p",null,"You can start up AutoRecovery using the ",(0,a.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-autorecovery"},(0,a.yg)("inlineCode",{parentName:"a"},"autorecovery"))," command of the ",(0,a.yg)("a",{parentName:"p",href:"../reference/cli"},(0,a.yg)("inlineCode",{parentName:"a"},"bookkeeper"))," CLI tool."),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-bash"},"$ bin/bookkeeper autorecovery\n")),(0,a.yg)("blockquote",null,(0,a.yg)("p",{parentName:"blockquote"},"The most important thing to ensure when starting up AutoRecovery is that the ZooKeeper connection string specified by the ",(0,a.yg)("a",{parentName:"p",href:"../reference/config#zkServers"},(0,a.yg)("inlineCode",{parentName:"a"},"zkServers"))," parameter points to the right ZooKeeper cluster.")),(0,a.yg)("p",null,"If you start up AutoRecovery on a machine that is already running a bookie, then the AutoRecovery process will run alongside the bookie on a separate thread."),(0,a.yg)("p",null,"You can also start up AutoRecovery on a fresh machine if you'd like to create a dedicated cluster of AutoRecovery nodes."),(0,a.yg)("h2",{id:"configuration"},"Configuration"),(0,a.yg)("p",null,"There are a handful of AutoRecovery-related configs in the ",(0,a.yg)("a",{parentName:"p",href:"../reference/config"},(0,a.yg)("inlineCode",{parentName:"a"},"bk_server.conf"))," configuration file. For a listing of those configs, see ",(0,a.yg)("a",{parentName:"p",href:"../reference/config#autorecovery-settings"},"AutoRecovery settings"),"."),(0,a.yg)("h2",{id:"disable-autorecovery"},"Disable AutoRecovery"),(0,a.yg)("p",null,"You can disable AutoRecovery at any time, for example during maintenance. Disabling AutoRecovery ensures that bookies' data isn't unnecessarily rereplicated when the bookie is only taken down for a short period of time, for example when the bookie is being updated or the configuration if being changed."),(0,a.yg)("p",null,"You can disable AutoRecover using the ",(0,a.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-shell-autorecovery"},(0,a.yg)("inlineCode",{parentName:"a"},"bookkeeper"))," CLI tool:"),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-bash"},"$ bin/bookkeeper shell autorecovery -disable\n")),(0,a.yg)("p",null,"Once disabled, you can reenable AutoRecovery using the ",(0,a.yg)("a",{parentName:"p",href:"../reference/cli#bookkeeper-shell-autorecovery"},(0,a.yg)("inlineCode",{parentName:"a"},"enable"))," shell command:"),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-bash"},"$ bin/bookkeeper shell autorecovery -enable\n")),(0,a.yg)("h2",{id:"autorecovery-architecture"},"AutoRecovery architecture"),(0,a.yg)("p",null,"AutoRecovery has two components:"),(0,a.yg)("ol",null,(0,a.yg)("li",{parentName:"ol"},"The ",(0,a.yg)("a",{parentName:"li",href:"#auditor"},(0,a.yg)("strong",{parentName:"a"},"auditor"))," (see the ",(0,a.yg)("a",{parentName:"li",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/replication/Auditor.html"},(0,a.yg)("inlineCode",{parentName:"a"},"Auditor"))," class) is a singleton node that watches bookies to see if they fail and creates rereplication tasks for the ledgers on failed bookies."),(0,a.yg)("li",{parentName:"ol"},"The ",(0,a.yg)("a",{parentName:"li",href:"#replication-worker"},(0,a.yg)("strong",{parentName:"a"},"replication worker"))," (see the ",(0,a.yg)("a",{parentName:"li",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/replication/ReplicationWorker.html"},(0,a.yg)("inlineCode",{parentName:"a"},"ReplicationWorker"))," class) runs on each bookie and executes rereplication tasks provided by the auditor.")),(0,a.yg)("p",null,"Both of these components run as threads in the ",(0,a.yg)("a",{parentName:"p",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/replication/AutoRecoveryMain"},(0,a.yg)("inlineCode",{parentName:"a"},"AutoRecoveryMain"))," process, which runs on each bookie in the cluster. All recovery nodes participate in leader election---using ZooKeeper---to decide which node becomes the auditor. Nodes that fail to become the auditor watch the elected auditor and run an election process again if they see that the auditor node has failed."),(0,a.yg)("h3",{id:"auditor"},"Auditor"),(0,a.yg)("p",null,"The auditor watches all bookies in the cluster that are registered with ZooKeeper. Bookies register with ZooKeeper at startup. If the bookie crashes or is killed, the bookie's registration in ZooKeeper disappears and the auditor is notified of the change in the list of registered bookies."),(0,a.yg)("p",null,"When the auditor sees that a bookie has disappeared, it immediately scans the complete ledger list to find ledgers that have data stored on the failed bookie. Once it has a list of ledgers for that bookie, the auditor will publish a rereplication task for each ledger under the ",(0,a.yg)("inlineCode",{parentName:"p"},"/underreplicated/")," ",(0,a.yg)("a",{parentName:"p",href:"https://zookeeper.apache.org/doc/current/zookeeperOver.html"},"znode")," in ZooKeeper."),(0,a.yg)("h3",{id:"replication-worker"},"Replication Worker"),(0,a.yg)("p",null,"Each replication worker watches for tasks being published by the auditor on the ",(0,a.yg)("inlineCode",{parentName:"p"},"/underreplicated/")," znode in ZooKeeper. When a new task appears, the replication worker will try to get a lock on it. If it cannot acquire the lock, it will try the next entry. The locks are implemented using ZooKeeper ephemeral znodes."),(0,a.yg)("p",null,"The replication worker will scan through the rereplication task's ledger for fragments of which its local bookie is not a member. When it finds fragments matching this criterion, it will replicate the entries of that fragment to the local bookie. If, after this process, the ledger is fully replicated, the ledgers entry under /underreplicated/ is deleted, and the lock is released. If there is a problem replicating, or there are still fragments in the ledger which are still underreplicated (due to the local bookie already being part of the ensemble for the fragment), then the lock is simply released."),(0,a.yg)("p",null,"If the replication worker finds a fragment which needs rereplication, but does not have a defined endpoint (i.e. the final fragment of a ledger currently being written to), it will wait for a grace period before attempting rereplication. If the fragment needing rereplication still does not have a defined endpoint, the ledger is fenced and rereplication then takes place."),(0,a.yg)("p",null,"This avoids the situation in which a client is writing to a ledger and one of the bookies goes down, but the client has not written an entry to that bookie before rereplication takes place. The client could continue writing to the old fragment, even though the ensemble for the fragment had changed. This could lead to data loss. Fencing prevents this scenario from happening. In the normal case, the client will try to write to the failed bookie within the grace period, and will have started a new fragment before rereplication starts."),(0,a.yg)("p",null,"You can configure this grace period using the ",(0,a.yg)("a",{parentName:"p",href:"../reference/config#openLedgerRereplicationGracePeriod"},(0,a.yg)("inlineCode",{parentName:"a"},"openLedgerRereplicationGracePeriod"))," parameter."),(0,a.yg)("h3",{id:"the-rereplication-process"},"The rereplication process"),(0,a.yg)("p",null,"The ledger rereplication process happens in these steps:"),(0,a.yg)("ol",null,(0,a.yg)("li",{parentName:"ol"},"The client goes through all ledger fragments in the ledger, selecting those that contain the failed bookie."),(0,a.yg)("li",{parentName:"ol"},"A recovery process is initiated for each ledger fragment in this list.",(0,a.yg)("ol",{parentName:"li"},(0,a.yg)("li",{parentName:"ol"},"The client selects a bookie to which all entries in the ledger fragment will be replicated; In the case of autorecovery, this will always be the local bookie."),(0,a.yg)("li",{parentName:"ol"},"The client reads entries that belong to the ledger fragment from other bookies in the ensemble and writes them to the selected bookie."),(0,a.yg)("li",{parentName:"ol"},"Once all entries have been replicated, the zookeeper metadata for the fragment is updated to reflect the new ensemble."),(0,a.yg)("li",{parentName:"ol"},"The fragment is marked as fully replicated in the recovery tool."))),(0,a.yg)("li",{parentName:"ol"},"Once all ledger fragments are marked as fully replicated, the ledger is marked as fully replicated.")))}d.isMDXComponent=!0}}]);