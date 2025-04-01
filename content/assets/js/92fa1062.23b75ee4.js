"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[4322],{15680:(e,t,a)=>{a.d(t,{xA:()=>m,yg:()=>h});var r=a(96540);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function n(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?n(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):n(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,r,o=function(e,t){if(null==e)return{};var a,r,o={},n=Object.keys(e);for(r=0;r<n.length;r++)a=n[r],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(r=0;r<n.length;r++)a=n[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var s=r.createContext({}),p=function(e){var t=r.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},m=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},u="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},g=r.forwardRef((function(e,t){var a=e.components,o=e.mdxType,n=e.originalType,s=e.parentName,m=l(e,["components","mdxType","originalType","parentName"]),u=p(a),g=o,h=u["".concat(s,".").concat(g)]||u[g]||c[g]||n;return a?r.createElement(h,i(i({ref:t},m),{},{components:a})):r.createElement(h,i({ref:t},m))}));function h(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var n=a.length,i=new Array(n);i[0]=g;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:o,i[1]=l;for(var p=2;p<n;p++)i[p]=a[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,a)}g.displayName="MDXCreateElement"},14291:(e,t,a)=>{a.r(t),a.d(t,{contentTitle:()=>i,default:()=>u,frontMatter:()=>n,metadata:()=>l,toc:()=>s});var r=a(58168),o=(a(96540),a(15680));const n={},i="BP-20: github workflow for bookkeeper proposals",l={type:"mdx",permalink:"/bps/BP-20-github-workflow-for-bookkeeper-proposals",source:"@site/src/pages/bps/BP-20-github-workflow-for-bookkeeper-proposals.md",title:"BP-20: github workflow for bookkeeper proposals",description:"Motivation",frontMatter:{}},s=[{value:"Motivation",id:"motivation",level:3},{value:"Proposed Changes",id:"proposed-changes",level:3}],p={toc:s},m="wrapper";function u(e){let{components:t,...a}=e;return(0,o.yg)(m,(0,r.A)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,o.yg)("h1",{id:"bp-20-github-workflow-for-bookkeeper-proposals"},"BP-20: github workflow for bookkeeper proposals"),(0,o.yg)("h3",{id:"motivation"},"Motivation"),(0,o.yg)("p",null,"We have a good BP process for introducing enhancements, features. However, this process is not well integrated with our github review process, and the content of a BP\nis not used for documentation. This proposal is to propose moving the BP workflow from ASF wiki to Github. There are a couple of reasons for making this change:"),(0,o.yg)("ul",null,(0,o.yg)("li",{parentName:"ul"},"the ASF cwiki is disconnected from Github, and usually becomes out of date quickly. It isn't really caught up with the code changes.\nMost of the content (documentation, contribution/release guides) are already in website, the ASF wiki is only used for tracking BPs and community meeting notes at this point."),(0,o.yg)("li",{parentName:"ul"},"Moving BP workflow from wiki to github will leverage the same github review process as code changes. So developers are easier to review BPs and make comments."),(0,o.yg)("li",{parentName:"ul"},"The BPs can eventually be used as a basis for documentation.")),(0,o.yg)("h3",{id:"proposed-changes"},"Proposed Changes"),(0,o.yg)("p",null,"All the BPs are maintained in ",(0,o.yg)("inlineCode",{parentName:"p"},"site/bps")," directory. To make a bookkeeper proposal, a developer does following steps:"),(0,o.yg)("ol",null,(0,o.yg)("li",{parentName:"ol"},"Create an issue ",(0,o.yg)("inlineCode",{parentName:"li"},"BP-<number>: [capation of bookkeeper proposal]"),". E.g. ",(0,o.yg)("inlineCode",{parentName:"li"},"BP-1: 64 bits ledger id support"),".",(0,o.yg)("ul",{parentName:"li"},(0,o.yg)("li",{parentName:"ul"},"Take the next available BP number from this page."),(0,o.yg)("li",{parentName:"ul"},"Write a brief description about what BP is for in this issue. This issue will be the master issue for tracking the status of this BP and its implementations.\nAll the implementations of this BP should be listed and linked to this master issues."))),(0,o.yg)("li",{parentName:"ol"},"Write the proposal for this BP.",(0,o.yg)("ul",{parentName:"li"},(0,o.yg)("li",{parentName:"ul"},"Make a copy of the ",(0,o.yg)("a",{parentName:"li",href:"https://github.com/apache/bookkeeper/tree/master/site/bps/BP-template.md"},"BP-Template"),". Name the BP file as ",(0,o.yg)("inlineCode",{parentName:"li"},"BP-<number>-[caption-of-proposal].md"),".")),(0,o.yg)("pre",{parentName:"li"},(0,o.yg)("code",{parentName:"pre",className:"language-shell"},"$ cp site/bps/BP-template.md site/bps/BP-xyz-capation-of-proposal.md\n")),(0,o.yg)("ul",{parentName:"li"},(0,o.yg)("li",{parentName:"ul"},"Fill the sections listed in the BP template.",(0,o.yg)("ul",{parentName:"li"},(0,o.yg)("li",{parentName:"ul"},"issue: replace ",(0,o.yg)("inlineCode",{parentName:"li"},"<issue-number>")," with the issue number."),(0,o.yg)("li",{parentName:"ul"},'state: "Under Discussion"'),(0,o.yg)("li",{parentName:"ul"},"release: leave the release to ",(0,o.yg)("inlineCode",{parentName:"li"},"N/A"),". you can only mark a release after a BP is implemented."))))),(0,o.yg)("li",{parentName:"ol"},"Send a PR for this BP. Following the instructions in the pull request template.",(0,o.yg)("ul",{parentName:"li"},(0,o.yg)("li",{parentName:"ul"},"add ",(0,o.yg)("inlineCode",{parentName:"li"},"BP")," label to this BP"),(0,o.yg)("li",{parentName:"ul"},"don't associate this PR with any release or milestone"))),(0,o.yg)("li",{parentName:"ol"},"You can tag committers on this RP for reviewers, or start a ",(0,o.yg)("inlineCode",{parentName:"li"},"[DISCUSS]")," thread on Apache mailing list. If you are sending an email, please make sure that the subject\nof the thread is of the format ",(0,o.yg)("inlineCode",{parentName:"li"},"[DISCUSS] BP-<number>: capation of bookkeeper proposal"),"."),(0,o.yg)("li",{parentName:"ol"},"Once the BP is finalized, reviewed and approved by committers, the BP is accepted. The criteria for acceptance is ",(0,o.yg)("a",{parentName:"li",href:"https://bookkeeper.apache.org/project/bylaws"},"lazy majority"),"."),(0,o.yg)("li",{parentName:"ol"},"Committers merge the PR after a BP is accepted. The development for this BP moves forward with implementations. The BP should be updated if there is anything changed during\nimplementing it."),(0,o.yg)("li",{parentName:"ol"},"After all the implementations for a given BP are completed, a new PR should be sent for changing the state of a BP:",(0,o.yg)("ul",{parentName:"li"},(0,o.yg)("li",{parentName:"ul"},'state: "Adopted"'),(0,o.yg)("li",{parentName:"ul"},"release: set to the release that includes this BP."))),(0,o.yg)("li",{parentName:"ol"},"The final PR for changing BP state will be used as the criteria for marking a BP as completed.")))}u.isMDXComponent=!0}}]);