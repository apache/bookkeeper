"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[1715],{15680:(e,n,a)=>{a.d(n,{xA:()=>p,yg:()=>y});var r=a(96540);function t(e,n,a){return n in e?Object.defineProperty(e,n,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[n]=a,e}function d(e,n){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);n&&(r=r.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),a.push.apply(a,r)}return a}function l(e){for(var n=1;n<arguments.length;n++){var a=null!=arguments[n]?arguments[n]:{};n%2?d(Object(a),!0).forEach((function(n){t(e,n,a[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):d(Object(a)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(a,n))}))}return e}function i(e,n){if(null==e)return{};var a,r,t=function(e,n){if(null==e)return{};var a,r,t={},d=Object.keys(e);for(r=0;r<d.length;r++)a=d[r],n.indexOf(a)>=0||(t[a]=e[a]);return t}(e,n);if(Object.getOwnPropertySymbols){var d=Object.getOwnPropertySymbols(e);for(r=0;r<d.length;r++)a=d[r],n.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(t[a]=e[a])}return t}var o=r.createContext({}),s=function(e){var n=r.useContext(o),a=n;return e&&(a="function"==typeof e?e(n):l(l({},n),e)),a},p=function(e){var n=s(e.components);return r.createElement(o.Provider,{value:n},e.children)},g="mdxType",c={inlineCode:"code",wrapper:function(e){var n=e.children;return r.createElement(r.Fragment,{},n)}},u=r.forwardRef((function(e,n){var a=e.components,t=e.mdxType,d=e.originalType,o=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),g=s(a),u=t,y=g["".concat(o,".").concat(u)]||g[u]||c[u]||d;return a?r.createElement(y,l(l({ref:n},p),{},{components:a})):r.createElement(y,l({ref:n},p))}));function y(e,n){var a=arguments,t=n&&n.mdxType;if("string"==typeof e||t){var d=a.length,l=new Array(d);l[0]=u;var i={};for(var o in n)hasOwnProperty.call(n,o)&&(i[o]=n[o]);i.originalType=e,i[g]="string"==typeof e?e:t,l[1]=i;for(var s=2;s<d;s++)l[s]=a[s];return r.createElement.apply(null,l)}return r.createElement.apply(null,a)}u.displayName="MDXCreateElement"},99338:(e,n,a)=>{a.r(n),a.d(n,{assets:()=>o,contentTitle:()=>l,default:()=>c,frontMatter:()=>d,metadata:()=>i,toc:()=>s});var r=a(9668),t=(a(96540),a(15680));const d={id:"ledger-adv-api",title:"The Advanced Ledger API"},l=void 0,i={unversionedId:"api/ledger-adv-api",id:"version-4.10.0/api/ledger-adv-api",title:"The Advanced Ledger API",description:"In release 4.5.0, Apache BookKeeper introduces a few advanced API for advanced usage.",source:"@site/versioned_docs/version-4.10.0/api/ledger-adv-api.md",sourceDirName:"api",slug:"/api/ledger-adv-api",permalink:"/docs/4.10.0/api/ledger-adv-api",draft:!1,tags:[],version:"4.10.0",frontMatter:{id:"ledger-adv-api",title:"The Advanced Ledger API"},sidebar:"version-4.10.0/docsSidebar",previous:{title:"The Ledger API",permalink:"/docs/4.10.0/api/ledger-api"},next:{title:"DistributedLog",permalink:"/docs/4.10.0/api/distributedlog-api"}},o={},s=[{value:"LedgerHandleAdv",id:"ledgerhandleadv",level:2},{value:"Creating advanced ledgers",id:"creating-advanced-ledgers",level:3},{value:"Add Entries",id:"add-entries",level:3},{value:"Read Entries",id:"read-entries",level:3}],p={toc:s},g="wrapper";function c(e){let{components:n,...a}=e;return(0,t.yg)(g,(0,r.A)({},p,a,{components:n,mdxType:"MDXLayout"}),(0,t.yg)("p",null,"In release ",(0,t.yg)("inlineCode",{parentName:"p"},"4.5.0"),", Apache BookKeeper introduces a few advanced API for advanced usage.\nThis sections covers these advanced APIs."),(0,t.yg)("blockquote",null,(0,t.yg)("p",{parentName:"blockquote"},"Before learn the advanced API, please read ",(0,t.yg)("a",{parentName:"p",href:"ledger-api"},"Ledger API")," first.")),(0,t.yg)("h2",{id:"ledgerhandleadv"},"LedgerHandleAdv"),(0,t.yg)("p",null,(0,t.yg)("a",{parentName:"p",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/client/LedgerHandleAdv"},(0,t.yg)("inlineCode",{parentName:"a"},"LedgerHandleAdv"))," is an advanced extension of ",(0,t.yg)("a",{parentName:"p",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/client/LedgerHandle"},(0,t.yg)("inlineCode",{parentName:"a"},"LedgerHandle")),".\nIt allows user passing in an ",(0,t.yg)("inlineCode",{parentName:"p"},"entryId")," when adding an entry."),(0,t.yg)("h3",{id:"creating-advanced-ledgers"},"Creating advanced ledgers"),(0,t.yg)("p",null,"Here's an example:"),(0,t.yg)("pre",null,(0,t.yg)("code",{parentName:"pre",className:"language-java"},'byte[] passwd = "some-passwd".getBytes();\nLedgerHandleAdv handle = bkClient.createLedgerAdv(\n    3, 3, 2, // replica settings\n    DigestType.CRC32,\n    passwd);\n')),(0,t.yg)("p",null,"You can also create advanced ledgers asynchronously."),(0,t.yg)("pre",null,(0,t.yg)("code",{parentName:"pre",className:"language-java"},'class LedgerCreationCallback implements AsyncCallback.CreateCallback {\n    public void createComplete(int returnCode, LedgerHandle handle, Object ctx) {\n        System.out.println("Ledger successfully created");\n    }\n}\nclient.asyncCreateLedgerAdv(\n        3, // ensemble size\n        3, // write quorum size\n        2, // ack quorum size\n        BookKeeper.DigestType.CRC32,\n        password,\n        new LedgerCreationCallback(),\n        "some context"\n);\n')),(0,t.yg)("p",null,"Besides the APIs above, BookKeeper allows users providing ",(0,t.yg)("inlineCode",{parentName:"p"},"ledger-id")," when creating advanced ledgers."),(0,t.yg)("pre",null,(0,t.yg)("code",{parentName:"pre",className:"language-java"},'long ledgerId = ...; // the ledger id is generated externally.\n\nbyte[] passwd = "some-passwd".getBytes();\nLedgerHandleAdv handle = bkClient.createLedgerAdv(\n    ledgerId, // ledger id generated externally\n    3, 3, 2, // replica settings\n    DigestType.CRC32,\n    passwd);\n')),(0,t.yg)("blockquote",null,(0,t.yg)("p",{parentName:"blockquote"},"Please note, it is users' responsibility to provide a unique ledger id when using the API above.\nIf a ledger already exists when users try to create an advanced ledger with same ledger id,\na ",(0,t.yg)("a",{parentName:"p",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/client/BKException.BKLedgerExistException.html"},"LedgerExistsException")," is thrown by the bookkeeper client.")),(0,t.yg)("p",null,"Creating advanced ledgers can be done through a fluent API since 4.6."),(0,t.yg)("pre",null,(0,t.yg)("code",{parentName:"pre",className:"language-java"},'BookKeeper bk = ...;\n\nbyte[] passwd = "some-passwd".getBytes();\n\nWriteHandle wh = bk.newCreateLedgerOp()\n    .withDigestType(DigestType.CRC32)\n    .withPassword(passwd)\n    .withEnsembleSize(3)\n    .withWriteQuorumSize(3)\n    .withAckQuorumSize(2)\n    .makeAdv()                  // convert the create ledger builder to create ledger adv builder\n    .withLedgerId(1234L)\n    .execute()                  // execute the creation op\n    .get();                     // wait for the execution to complete\n\n')),(0,t.yg)("h3",{id:"add-entries"},"Add Entries"),(0,t.yg)("p",null,"The normal ",(0,t.yg)("a",{parentName:"p",href:"ledger-api/#adding-entries-to-ledgers"},"add entries api")," in advanced ledgers are disabled. Instead, when users want to add entries\nto advanced ledgers, an entry id is required to pass in along with the entry data when adding an entry."),(0,t.yg)("pre",null,(0,t.yg)("code",{parentName:"pre",className:"language-java"},'long entryId = ...; // entry id generated externally\n\nledger.addEntry(entryId, "Some entry data".getBytes());\n')),(0,t.yg)("p",null,"If you are using the new API, you can do as following:"),(0,t.yg)("pre",null,(0,t.yg)("code",{parentName:"pre",className:"language-java"},'WriteHandle wh = ...;\nlong entryId = ...; // entry id generated externally\n\nwh.write(entryId, "Some entry data".getBytes()).get();\n')),(0,t.yg)("p",null,"A few notes when using this API:"),(0,t.yg)("ul",null,(0,t.yg)("li",{parentName:"ul"},"The entry id has to be non-negative."),(0,t.yg)("li",{parentName:"ul"},"Clients are okay to add entries out of order."),(0,t.yg)("li",{parentName:"ul"},"However, the entries are only acknowledged in a monotonic order starting from 0.")),(0,t.yg)("h3",{id:"read-entries"},"Read Entries"),(0,t.yg)("p",null,"The read entries api in advanced ledgers remain same as ",(0,t.yg)("a",{parentName:"p",href:"ledger-api/#reading-entries-from-ledgers"},"normal ledgers"),"."))}c.isMDXComponent=!0}}]);