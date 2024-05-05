"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[7764],{3905:function(e,n,t){t.d(n,{Zo:function(){return p},kt:function(){return v}});var a=t(67294);function r(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function d(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);n&&(a=a.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,a)}return t}function i(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?d(Object(t),!0).forEach((function(n){r(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):d(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function l(e,n){if(null==e)return{};var t,a,r=function(e,n){if(null==e)return{};var t,a,r={},d=Object.keys(e);for(a=0;a<d.length;a++)t=d[a],n.indexOf(t)>=0||(r[t]=e[t]);return r}(e,n);if(Object.getOwnPropertySymbols){var d=Object.getOwnPropertySymbols(e);for(a=0;a<d.length;a++)t=d[a],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var o=a.createContext({}),s=function(e){var n=a.useContext(o),t=n;return e&&(t="function"==typeof e?e(n):i(i({},n),e)),t},p=function(e){var n=s(e.components);return a.createElement(o.Provider,{value:n},e.children)},c="mdxType",u={inlineCode:"code",wrapper:function(e){var n=e.children;return a.createElement(a.Fragment,{},n)}},g=a.forwardRef((function(e,n){var t=e.components,r=e.mdxType,d=e.originalType,o=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),c=s(t),g=r,v=c["".concat(o,".").concat(g)]||c[g]||u[g]||d;return t?a.createElement(v,i(i({ref:n},p),{},{components:t})):a.createElement(v,i({ref:n},p))}));function v(e,n){var t=arguments,r=n&&n.mdxType;if("string"==typeof e||r){var d=t.length,i=new Array(d);i[0]=g;var l={};for(var o in n)hasOwnProperty.call(n,o)&&(l[o]=n[o]);l.originalType=e,l[c]="string"==typeof e?e:r,i[1]=l;for(var s=2;s<d;s++)i[s]=t[s];return a.createElement.apply(null,i)}return a.createElement.apply(null,t)}g.displayName="MDXCreateElement"},50289:function(e,n,t){t.r(n),t.d(n,{assets:function(){return o},contentTitle:function(){return i},default:function(){return u},frontMatter:function(){return d},metadata:function(){return l},toc:function(){return s}});var a=t(83117),r=(t(67294),t(3905));const d={id:"ledger-adv-api",title:"The Advanced Ledger API"},i=void 0,l={unversionedId:"api/ledger-adv-api",id:"version-4.16.5/api/ledger-adv-api",title:"The Advanced Ledger API",description:"In release 4.5.0, Apache BookKeeper introduces a few advanced API for advanced usage.",source:"@site/versioned_docs/version-4.16.5/api/ledger-adv-api.md",sourceDirName:"api",slug:"/api/ledger-adv-api",permalink:"/docs/4.16.5/api/ledger-adv-api",draft:!1,tags:[],version:"4.16.5",frontMatter:{id:"ledger-adv-api",title:"The Advanced Ledger API"},sidebar:"docsSidebar",previous:{title:"The Ledger API",permalink:"/docs/4.16.5/api/ledger-api"},next:{title:"DistributedLog",permalink:"/docs/4.16.5/api/distributedlog-api"}},o={},s=[{value:"LedgerHandleAdv",id:"ledgerhandleadv",level:2},{value:"Creating advanced ledgers",id:"creating-advanced-ledgers",level:3},{value:"Add Entries",id:"add-entries",level:3},{value:"Read Entries",id:"read-entries",level:3}],p={toc:s},c="wrapper";function u(e){let{components:n,...t}=e;return(0,r.kt)(c,(0,a.Z)({},p,t,{components:n,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"In release ",(0,r.kt)("inlineCode",{parentName:"p"},"4.5.0"),", Apache BookKeeper introduces a few advanced API for advanced usage.\nThis sections covers these advanced APIs."),(0,r.kt)("blockquote",null,(0,r.kt)("p",{parentName:"blockquote"},"Before learn the advanced API, please read ",(0,r.kt)("a",{parentName:"p",href:"ledger-api"},"Ledger API")," first.")),(0,r.kt)("h2",{id:"ledgerhandleadv"},"LedgerHandleAdv"),(0,r.kt)("p",null,(0,r.kt)("a",{parentName:"p",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/client/LedgerHandleAdv"},(0,r.kt)("inlineCode",{parentName:"a"},"LedgerHandleAdv"))," is an advanced extension of ",(0,r.kt)("a",{parentName:"p",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/client/LedgerHandle"},(0,r.kt)("inlineCode",{parentName:"a"},"LedgerHandle")),".\nIt allows user passing in an ",(0,r.kt)("inlineCode",{parentName:"p"},"entryId")," when adding an entry."),(0,r.kt)("h3",{id:"creating-advanced-ledgers"},"Creating advanced ledgers"),(0,r.kt)("p",null,"Here's an exmaple:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'byte[] passwd = "some-passwd".getBytes();\nLedgerHandleAdv handle = bkClient.createLedgerAdv(\n    3, 3, 2, // replica settings\n    DigestType.CRC32,\n    passwd);\n')),(0,r.kt)("p",null,"You can also create advanced ledgers asynchronously."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'class LedgerCreationCallback implements AsyncCallback.CreateCallback {\n    public void createComplete(int returnCode, LedgerHandle handle, Object ctx) {\n        System.out.println("Ledger successfully created");\n    }\n}\nclient.asyncCreateLedgerAdv(\n        3, // ensemble size\n        3, // write quorum size\n        2, // ack quorum size\n        BookKeeper.DigestType.CRC32,\n        password,\n        new LedgerCreationCallback(),\n        "some context"\n);\n')),(0,r.kt)("p",null,"Besides the APIs above, BookKeeper allows users providing ",(0,r.kt)("inlineCode",{parentName:"p"},"ledger-id")," when creating advanced ledgers."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'long ledgerId = ...; // the ledger id is generated externally.\n\nbyte[] passwd = "some-passwd".getBytes();\nLedgerHandleAdv handle = bkClient.createLedgerAdv(\n    ledgerId, // ledger id generated externally\n    3, 3, 2, // replica settings\n    DigestType.CRC32,\n    passwd);\n')),(0,r.kt)("blockquote",null,(0,r.kt)("p",{parentName:"blockquote"},"Please note, it is users' responsibility to provide a unique ledger id when using the API above.\nIf a ledger already exists when users try to create an advanced ledger with same ledger id,\na ",(0,r.kt)("a",{parentName:"p",href:"https://bookkeeper.apache.org//docs/latest/api/javadoc/org/apache/bookkeeper/client/BKException.BKLedgerExistException.html"},"LedgerExistsException")," is thrown by the bookkeeper client.")),(0,r.kt)("p",null,"Creating advanced ledgers can be done throught a fluent API since 4.6."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'BookKeeper bk = ...;\n\nbyte[] passwd = "some-passwd".getBytes();\n\nWriteHandle wh = bk.newCreateLedgerOp()\n    .withDigestType(DigestType.CRC32)\n    .withPassword(passwd)\n    .withEnsembleSize(3)\n    .withWriteQuorumSize(3)\n    .withAckQuorumSize(2)\n    .makeAdv()                  // convert the create ledger builder to create ledger adv builder\n    .withLedgerId(1234L)\n    .execute()                  // execute the creation op\n    .get();                     // wait for the execution to complete\n\n')),(0,r.kt)("h3",{id:"add-entries"},"Add Entries"),(0,r.kt)("p",null,"The normal ",(0,r.kt)("a",{parentName:"p",href:"ledger-api/#adding-entries-to-ledgers"},"add entries api")," in advanced ledgers are disabled. Instead, when users want to add entries\nto advanced ledgers, an entry id is required to pass in along with the entry data when adding an entry."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'long entryId = ...; // entry id generated externally\n\nledger.addEntry(entryId, "Some entry data".getBytes());\n')),(0,r.kt)("p",null,"If you are using the new API, you can do as following:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-java"},'WriteHandle wh = ...;\nlong entryId = ...; // entry id generated externally\n\nwh.write(entryId, "Some entry data".getBytes()).get();\n')),(0,r.kt)("p",null,"A few notes when using this API:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"The entry id has to be non-negative."),(0,r.kt)("li",{parentName:"ul"},"Clients are okay to add entries out of order."),(0,r.kt)("li",{parentName:"ul"},"However, the entries are only acknowledged in a monotonic order starting from 0.")),(0,r.kt)("h3",{id:"read-entries"},"Read Entries"),(0,r.kt)("p",null,"The read entries api in advanced ledgers remain same as ",(0,r.kt)("a",{parentName:"p",href:"ledger-api/#reading-entries-from-ledgers"},"normal ledgers"),"."))}u.isMDXComponent=!0}}]);