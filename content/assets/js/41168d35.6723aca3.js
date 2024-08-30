"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[495],{15680:(e,t,o)=>{o.d(t,{xA:()=>c,yg:()=>g});var n=o(96540);function i(e,t,o){return t in e?Object.defineProperty(e,t,{value:o,enumerable:!0,configurable:!0,writable:!0}):e[t]=o,e}function r(e,t){var o=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),o.push.apply(o,n)}return o}function a(e){for(var t=1;t<arguments.length;t++){var o=null!=arguments[t]?arguments[t]:{};t%2?r(Object(o),!0).forEach((function(t){i(e,t,o[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(o)):r(Object(o)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(o,t))}))}return e}function s(e,t){if(null==e)return{};var o,n,i=function(e,t){if(null==e)return{};var o,n,i={},r=Object.keys(e);for(n=0;n<r.length;n++)o=r[n],t.indexOf(o)>=0||(i[o]=e[o]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)o=r[n],t.indexOf(o)>=0||Object.prototype.propertyIsEnumerable.call(e,o)&&(i[o]=e[o])}return i}var l=n.createContext({}),p=function(e){var t=n.useContext(l),o=t;return e&&(o="function"==typeof e?e(t):a(a({},t),e)),o},c=function(e){var t=p(e.components);return n.createElement(l.Provider,{value:t},e.children)},d="mdxType",h={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var o=e.components,i=e.mdxType,r=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),d=p(o),u=i,g=d["".concat(l,".").concat(u)]||d[u]||h[u]||r;return o?n.createElement(g,a(a({ref:t},c),{},{components:o})):n.createElement(g,a({ref:t},c))}));function g(e,t){var o=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=o.length,a=new Array(r);a[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[d]="string"==typeof e?e:i,a[1]=s;for(var p=2;p<r;p++)a[p]=o[p];return n.createElement.apply(null,a)}return n.createElement.apply(null,o)}u.displayName="MDXCreateElement"},21081:(e,t,o)=>{o.r(t),o.d(t,{contentTitle:()=>a,default:()=>d,frontMatter:()=>r,metadata:()=>s,toc:()=>l});var n=o(9668),i=(o(96540),o(15680));const r={},a="BP-38: Publish Bookie Service Info on Metadata Service",s={type:"mdx",permalink:"/bps/BP-38-bookie-endpoint-discovery",source:"@site/src/pages/bps/BP-38-bookie-endpoint-discovery.md",title:"BP-38: Publish Bookie Service Info on Metadata Service",description:"Motivation",frontMatter:{}},l=[{value:"Motivation",id:"motivation",level:3},{value:"Public Interfaces",id:"public-interfaces",level:3},{value:"Which kind of properties should be stored in BookieServiceInfo ?",id:"which-kind-of-properties-should-be-stored-in-bookieserviceinfo-",level:4},{value:"Proposed Changes",id:"proposed-changes",level:3},{value:"Compatibility, Deprecation, and Migration Plan",id:"compatibility-deprecation-and-migration-plan",level:3},{value:"Test Plan",id:"test-plan",level:3},{value:"Rejected Alternatives",id:"rejected-alternatives",level:3},{value:"Adding a new set of znodes",id:"adding-a-new-set-of-znodes",level:4},{value:"Storing information inside the Cookie",id:"storing-information-inside-the-cookie",level:4}],p={toc:l},c="wrapper";function d(e){let{components:t,...o}=e;return(0,i.yg)(c,(0,n.A)({},p,o,{components:t,mdxType:"MDXLayout"}),(0,i.yg)("h1",{id:"bp-38-publish-bookie-service-info-on-metadata-service"},"BP-38: Publish Bookie Service Info on Metadata Service"),(0,i.yg)("h3",{id:"motivation"},"Motivation"),(0,i.yg)("p",null,"Bookie server exposes several services and some of them are optional: the binary BookKeeper protocol endpoint, the HTTP service, the StreamStorage service, a Metrics endpoint."),(0,i.yg)("p",null,"Currently (in BookKeeper 4.10) the client can only discover the main Bookie endpoint:\nthe main BookKeeper binary RPC service.\nDiscovery of the TCP address is implicit, because the ",(0,i.yg)("em",{parentName:"p"},"id")," of the bookie is made of the same host:port that point to the TCP address of the Bookie service."),(0,i.yg)("p",null,"With this proposal we are now introducing a way for the Bookie to advertise the services it exposes, basically the Bookie will be able to store on the Metadata Service a structure that describes the list of  ",(0,i.yg)("em",{parentName:"p"},"available services"),"."),(0,i.yg)("p",null,"We will also allow to publish a set of additional unstructured properties in form of a key-value pair that will be useful for further implementations."),(0,i.yg)("p",null,"This information will be also useful for Monitoring and Management services as it will enable full discovery of the capabilities of all of the Bookies in a cluster just by having read access to the Metadata Service."),(0,i.yg)("h3",{id:"public-interfaces"},"Public Interfaces"),(0,i.yg)("p",null,"On the Registration API, we introduce a new data structure that describes the services\nexposed by a Bookie:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre"},'interface BookieServiceInfo {\n    class Endpoint {\n        String name;\n        String hostname;\n        int port;\n        String protocol; // "bookie-rpc", "http", "https"....      \n    }\n    List<Endpoint> endpoints;\n    Map<String, String> properties; // additional properties\n}\n\n')),(0,i.yg)("p",null,"In RegistrationClient interface we expose a new method:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre"},"CompletableFuture<Versioned<BookieServiceInfo>> getBookieServiceInfo(String bookieId)\n")),(0,i.yg)("p",null,"The client can derive bookieId from a BookieSocketAddress. He can access the list of available bookies using ",(0,i.yg)("strong",{parentName:"p"},"RegistrationClient#getAllBookies()")," and then use this new method to get the details of the services exposed by the Bookie."),(0,i.yg)("p",null,"On the Bookie side we change the RegistrationManager interface in order to let the Bookie\nadvertise the services:"),(0,i.yg)("p",null,"in RegistrationManager class the ",(0,i.yg)("strong",{parentName:"p"},"registerBookie")," method "),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre"},"void registerBookie(String bookieId, boolean readOnly)\n")),(0,i.yg)("p",null,"becomes"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre"},"void registerBookie(String bookieId, boolean readOnly, BookieServiceInfo bookieServiceInfo)\n")),(0,i.yg)("p",null,"It will be up to the implementation of RegistrationManager and RegistrationClient to serialize\nthe BookieServiceInfo structure."),(0,i.yg)("p",null,"For the ZooKeeper based implementation we are going to store such information in Protobuf binary format, as currently this is the format used for every other kind of metadata (the example here is in JSON-like for readability purposes):"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre"},'{\n    "endpoints": [\n        {\n         "name": "bookie",\n         "hostname": "localhost",\n         "port": 3181,\n         "protocol": "bookie-rpc"\n        },\n        {\n         "name": "bookie-http-server",\n         "hostname": "localhost",\n         "port": 8080,\n         "protocol": "http"\n        }\n    ],\n    properties {\n        "property": "value",\n        "property2": "value2"\n    }\n}\n')),(0,i.yg)("p",null,"Such information will be stored inside the '/REGISTRATIONPATH/available' znode (or /REGISTRATIONPATH/available/readonly' in case of readonly bookie), these paths are the same used in 4.10, but in 4.10 we are writing empty znodes."),(0,i.yg)("p",null,"The rationale around this choice is that the client is already using these znodes in order to discover available bookies services."),(0,i.yg)("p",null,"It is possible that such endpoint information change during the lifetime of a Bookie, like after rebooting a machine and the machine gets a new network address."),(0,i.yg)("p",null,"It is out of the scope of this proposal to change semantics of ledger metadata, in which we  are currently storing directly the network address of the bookies, but with this proposal we are preparing the way to have an indirection and separate the concept of Bookie ID from the Network address."),(0,i.yg)("p",null,(0,i.yg)("strong",{parentName:"p"},"Endpoint structure")),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("strong",{parentName:"li"},"name"),": this is an id for the service type, like 'bookie'"),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("strong",{parentName:"li"},"hostname"),": the hostname (or a raw IP address)"),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("strong",{parentName:"li"},"port"),": the TCP port (int)"),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("strong",{parentName:"li"},"protocol"),": the type of service (bookie-rpc, http, https)")),(0,i.yg)("p",null,(0,i.yg)("strong",{parentName:"p"},"Well known additional properties")),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("strong",{parentName:"li"},"autorecovery.enabled"),": 'true' case of the presence of the auto recovery daemon")),(0,i.yg)("p",null,"The GRPC service would use this mechanism as well."),(0,i.yg)("h4",{id:"which-kind-of-properties-should-be-stored-in-bookieserviceinfo-"},"Which kind of properties should be stored in BookieServiceInfo ?"),(0,i.yg)("p",null,"We should store here only the minimal set of information useful to reach the bookie in an efficient way.\nSo we are not storing on the Metadata Service information about the internal state of the server: if you know the address of an HTTP endpoint you can use the REST API to query the Bookie for its state."),(0,i.yg)("p",null,"These properties may change during the lifetime of a Bookie, think about a configuration (change network address) or a dynamic assigned DNS name."),(0,i.yg)("p",null,"It is better not to expose the version of the Bookie, if the client wants to use particular features of the Bookie this should be implemented on the protocol itself, not just by using the version. The version of the Bookie could be exposed on the HTTP endpoint."),(0,i.yg)("h3",{id:"proposed-changes"},"Proposed Changes"),(0,i.yg)("p",null,"See the 'Public interfaces' section."),(0,i.yg)("h3",{id:"compatibility-deprecation-and-migration-plan"},"Compatibility, Deprecation, and Migration Plan"),(0,i.yg)("p",null,"The proposed change will be compatible with all existing clients."),(0,i.yg)("p",null,"In case a new client is reading an empty znode it will assume a default configuration with a single 'bookie-rpc' endpoint, like in this example:"),(0,i.yg)("p",null,'{\n"endpoints": ','[\n{\n"name": "bookie",\n"hostname": "hostname-from-bookieid",\n"port": port-from-bookieid,\n"protocol": "bookie-rpc"\n}\n]',"\n}"),(0,i.yg)("p",null,"This information is enough in order to use the RPC service."),(0,i.yg)("h3",{id:"test-plan"},"Test Plan"),(0,i.yg)("p",null,"New unit tests will be added to cover all of the code changes.\nNo need for additional integration tests."),(0,i.yg)("h3",{id:"rejected-alternatives"},"Rejected Alternatives"),(0,i.yg)("h4",{id:"adding-a-new-set-of-znodes"},"Adding a new set of znodes"),(0,i.yg)("p",null,"For the ZooKeeper implementation we are not introducing a new znode to store BookieServiceInfo. Adding such new node will increase complexity and the usage of resources on ZooKeeper."),(0,i.yg)("h4",{id:"storing-information-inside-the-cookie"},"Storing information inside the Cookie"),(0,i.yg)("p",null,"The ",(0,i.yg)("em",{parentName:"p"},"Cookie")," stores information about the ",(0,i.yg)("em",{parentName:"p"},"identity")," of the bookie and it is expected not to change.\nIt is exceptional to rewrite the Cookie, like when adding a new data directory.\nIn some environments it is common to have a new network address after a restart or to change the configuration and enable a new service or feature, and you cannot rewrite the Cookie at each restart: by design every change to the Cookie should be manual or controlled by an external entity other than the Bookie itself."))}d.isMDXComponent=!0}}]);