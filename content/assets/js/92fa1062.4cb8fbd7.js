"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[9342],{3905:function(e,t,r){r.d(t,{Zo:function(){return u},kt:function(){return f}});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),p=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),m=p(r),f=a,h=m["".concat(s,".").concat(f)]||m[f]||c[f]||o;return r?n.createElement(h,i(i({ref:t},u),{},{components:r})):n.createElement(h,i({ref:t},u))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:a,i[1]=l;for(var p=2;p<o;p++)i[p]=r[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},87398:function(e,t,r){r.r(t),r.d(t,{contentTitle:function(){return s},default:function(){return m},frontMatter:function(){return l},metadata:function(){return p},toc:function(){return u}});var n=r(87462),a=r(63366),o=(r(67294),r(3905)),i=["components"],l={},s="BP-20: github workflow for bookkeeper proposals",p={type:"mdx",permalink:"/bps/BP-20-github-workflow-for-bookkeeper-proposals",source:"@site/src/pages/bps/BP-20-github-workflow-for-bookkeeper-proposals.md",title:"BP-20: github workflow for bookkeeper proposals",description:"Motivation",frontMatter:{}},u=[{value:"Motivation",id:"motivation",level:3},{value:"Proposed Changes",id:"proposed-changes",level:3}],c={toc:u};function m(e){var t=e.components,r=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"bp-20-github-workflow-for-bookkeeper-proposals"},"BP-20: github workflow for bookkeeper proposals"),(0,o.kt)("h3",{id:"motivation"},"Motivation"),(0,o.kt)("p",null,"We have a good BP process for introducing enhancements, features. However, this process is not well integrated with our github review process, and the content of a BP\nis not used for documentation. This proposal is to propose moving the BP workflow from ASF wiki to Github. There are a couple of reasons for making this change:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"the ASF cwiki is disconnected from Github, and usually becomes out of date quickly. It isn't really caught up with the code changes.\nMost of the content (documentation, contribution/release guides) are already in website, the ASF wiki is only used for tracking BPs and community meeting notes at this point."),(0,o.kt)("li",{parentName:"ul"},"Moving BP workflow from wiki to github will leverage the same github review process as code changes. So developers are easier to review BPs and make comments."),(0,o.kt)("li",{parentName:"ul"},"The BPs can eventually be used as a basis for documentation.")),(0,o.kt)("h3",{id:"proposed-changes"},"Proposed Changes"),(0,o.kt)("p",null,"All the BPs are maintained in ",(0,o.kt)("inlineCode",{parentName:"p"},"site/bps")," directory. To make a bookkeeper proposal, a developer does following steps:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Create an issue ",(0,o.kt)("inlineCode",{parentName:"li"},"BP-<number>: [capation of bookkeeper proposal]"),". E.g. ",(0,o.kt)("inlineCode",{parentName:"li"},"BP-1: 64 bits ledger id support"),".",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"Take the next available BP number from this page."),(0,o.kt)("li",{parentName:"ul"},"Write a brief description about what BP is for in this issue. This issue will be the master issue for tracking the status of this BP and its implementations.\nAll the implementations of this BP should be listed and linked to this master issues."))),(0,o.kt)("li",{parentName:"ol"},"Write the proposal for this BP.",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"Make a copy of the ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/apache/bookkeeper/tree/master/site/bps/BP-template.md"},"BP-Template"),". Name the BP file as ",(0,o.kt)("inlineCode",{parentName:"li"},"BP-<number>-[caption-of-proposal].md"),".")),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"$ cp site/bps/BP-template.md site/bps/BP-xyz-capation-of-proposal.md\n")),(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"Fill the sections listed in the BP template.",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"issue: replace ",(0,o.kt)("inlineCode",{parentName:"li"},"<issue-number>")," with the issue number."),(0,o.kt)("li",{parentName:"ul"},'state: "Under Discussion"'),(0,o.kt)("li",{parentName:"ul"},"release: leave the release to ",(0,o.kt)("inlineCode",{parentName:"li"},"N/A"),". you can only mark a release after a BP is implemented."))))),(0,o.kt)("li",{parentName:"ol"},"Send a PR for this BP. Following the instructions in the pull request template.",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"add ",(0,o.kt)("inlineCode",{parentName:"li"},"BP")," label to this BP"),(0,o.kt)("li",{parentName:"ul"},"don't associate this PR with any release or milestone"))),(0,o.kt)("li",{parentName:"ol"},"You can tag committers on this RP for reviewers, or start a ",(0,o.kt)("inlineCode",{parentName:"li"},"[DISCUSS]")," thread on Apache mailing list. If you are sending an email, please make sure that the subject\nof the thread is of the format ",(0,o.kt)("inlineCode",{parentName:"li"},"[DISCUSS] BP-<number>: capation of bookkeeper proposal"),"."),(0,o.kt)("li",{parentName:"ol"},"Once the BP is finalized, reviewed and approved by committers, the BP is accepted. The criteria for acceptance is ",(0,o.kt)("a",{parentName:"li",href:"https://bookkeeper.apache.org/project/bylaws"},"lazy majority"),"."),(0,o.kt)("li",{parentName:"ol"},"Committers merge the PR after a BP is accepted. The development for this BP moves forward with implementations. The BP should be updated if there is anything changed during\nimplementing it."),(0,o.kt)("li",{parentName:"ol"},"After all the implementations for a given BP are completed, a new PR should be sent for changing the state of a BP:",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},'state: "Adopted"'),(0,o.kt)("li",{parentName:"ul"},"release: set to the release that includes this BP."))),(0,o.kt)("li",{parentName:"ol"},"The final PR for changing BP state will be used as the criteria for marking a BP as completed.")))}m.isMDXComponent=!0}}]);