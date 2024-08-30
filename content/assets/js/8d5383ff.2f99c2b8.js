"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[9208],{15680:(e,t,a)=>{a.d(t,{xA:()=>p,yg:()=>c});var r=a(96540);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},i=Object.keys(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var l=r.createContext({}),u=function(e){var t=r.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},p=function(e){var t=u(e.components);return r.createElement(l.Provider,{value:t},e.children)},g="mdxType",y={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,i=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),g=u(a),d=n,c=g["".concat(l,".").concat(d)]||g[d]||y[d]||i;return a?r.createElement(c,o(o({ref:t},p),{},{components:a})):r.createElement(c,o({ref:t},p))}));function c(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=a.length,o=new Array(i);o[0]=d;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[g]="string"==typeof e?e:n,o[1]=s;for(var u=2;u<i;u++)o[u]=a[u];return r.createElement.apply(null,o)}return r.createElement.apply(null,a)}d.displayName="MDXCreateElement"},98966:(e,t,a)=>{a.r(t),a.d(t,{contentTitle:()=>o,default:()=>g,frontMatter:()=>i,metadata:()=>s,toc:()=>l});var r=a(9668),n=(a(96540),a(15680));const i={},o="Issue report",s={type:"mdx",permalink:"/community/issue-report",source:"@site/src/pages/community/issue-report.md",title:"Issue report",description:"To report an issue, you will need to create a New Issue.",frontMatter:{}},l=[{value:"Before creating a new Issue:",id:"before-creating-a-new-issue",level:2},{value:"Creating a Issue:",id:"creating-a-issue",level:2},{value:"Provide useful and required information",id:"provide-useful-and-required-information",level:3},{value:"If it is a question",id:"if-it-is-a-question",level:4},{value:"If it is a <strong>FEATURE REQUEST</strong>",id:"if-it-is-a-feature-request",level:4},{value:"If it is a <strong>BUG REPORT</strong>",id:"if-it-is-a-bug-report",level:4},{value:"Use Labels",id:"use-labels",level:3},{value:"Type",id:"type",level:4},{value:"Area",id:"area",level:4},{value:"Priority",id:"priority",level:4},{value:"Status",id:"status",level:4},{value:"BookKeeper Proposal",id:"bookkeeper-proposal",level:4},{value:"Milestone and Release",id:"milestone-and-release",level:4}],u={toc:l},p="wrapper";function g(e){let{components:t,...a}=e;return(0,n.yg)(p,(0,r.A)({},u,a,{components:t,mdxType:"MDXLayout"}),(0,n.yg)("h1",{id:"issue-report"},"Issue report"),(0,n.yg)("p",null,"To report an issue, you will need to create a ",(0,n.yg)("strong",{parentName:"p"},(0,n.yg)("a",{parentName:"strong",href:"https://github.com/apache/bookkeeper/issues/new"},"New Issue")),".\nBe aware that resolving your issue may require ",(0,n.yg)("strong",{parentName:"p"},"your participation"),". Please be willing and prepared to aid the developers in finding the actual cause of the issue so that they can develop a comprehensive solution."),(0,n.yg)("h2",{id:"before-creating-a-new-issue"},"Before creating a new Issue:"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},"Search for the issue you want to report, it may already have been reported."),(0,n.yg)("li",{parentName:"ul"},"If you find a similar issue, add any new information you might have as a comment on the existing issue. If it's different enough, you might decide it needs to be reported in a new issue."),(0,n.yg)("li",{parentName:"ul"},"If an issue you recently reported was closed, and you don't agree with the reasoning for it being closed, you will need to reopen it to let us re-investigate the issue."),(0,n.yg)("li",{parentName:"ul"},"Do not reopen the tickets that are in a previously completed milestone. Instead, open a new issue.")),(0,n.yg)("h2",{id:"creating-a-issue"},"Creating a Issue:"),(0,n.yg)("p",null,"Here is an very useful article ",(0,n.yg)("a",{parentName:"p",href:"http://www.chiark.greenend.org.uk/%7Esgtatham/bugs.html"},"How to report bugs effectively")),(0,n.yg)("h3",{id:"provide-useful-and-required-information"},"Provide useful and required information"),(0,n.yg)("h4",{id:"if-it-is-a-question"},"If it is a question"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},"Please check our ",(0,n.yg)("a",{parentName:"li",href:"https://bookkeeper.apache.org/docs/next/overview/"},"documentation")," first. "),(0,n.yg)("li",{parentName:"ul"},"If you could not find an answer there, please consider asking your question in our community mailing list at ",(0,n.yg)("a",{parentName:"li",href:"mailto:dev@bookkeeper.apache.org"},"dev@bookkeeper.apache.org"),", or reach out us on our ",(0,n.yg)("a",{parentName:"li",href:"slack"},"Slack channel"),".  It would benefit other members of our community.")),(0,n.yg)("h4",{id:"if-it-is-a-feature-request"},"If it is a ",(0,n.yg)("strong",{parentName:"h4"},"FEATURE REQUEST")),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},"Please describe the feature you are requesting."),(0,n.yg)("li",{parentName:"ul"},"Indicate the importance of this issue to you (",(0,n.yg)("em",{parentName:"li"},"blocker"),", ",(0,n.yg)("em",{parentName:"li"},"must-have"),", ",(0,n.yg)("em",{parentName:"li"},"should-have"),", ",(0,n.yg)("em",{parentName:"li"},"nice-to-have"),"). Are you currently using any workarounds to address this issue?"),(0,n.yg)("li",{parentName:"ul"},"Provide any additional detail on your proposed use case for this feature."),(0,n.yg)("li",{parentName:"ul"},"If it is a ",(0,n.yg)("a",{parentName:"li",href:"https://bookkeeper.apache.org/community/bookkeeper-proposals/"},"BookKeeper Proposal"),", please label this issue as ",(0,n.yg)("inlineCode",{parentName:"li"},"BP"),".")),(0,n.yg)("h4",{id:"if-it-is-a-bug-report"},"If it is a ",(0,n.yg)("strong",{parentName:"h4"},"BUG REPORT")),(0,n.yg)("p",null,"Please describe the issue you observed:"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},"What did you do?"),(0,n.yg)("li",{parentName:"ul"},"What did you expect to see?"),(0,n.yg)("li",{parentName:"ul"},"What did you see instead?")),(0,n.yg)("h3",{id:"use-labels"},"Use Labels"),(0,n.yg)("p",null,"Issue labels help to find issue reports and recognize the status of where an issue is in the lifecycle. An issue typically has the following 2 types of labels:"),(0,n.yg)("ol",null,(0,n.yg)("li",{parentName:"ol"},(0,n.yg)("strong",{parentName:"li"},"type")," identifying its type."),(0,n.yg)("li",{parentName:"ol"},(0,n.yg)("strong",{parentName:"li"},"area")," identifying the areas it belongs to.")),(0,n.yg)("h4",{id:"type"},"Type"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"type/bug"),": The issue describes a product defect."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"type/feature"),": The issue describes a new feature, which requires extensive design and testing."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"type/question"),": The issue contains a user or contributor question requiring a response."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"type/task"),": The issue describes a new task, which requires extensive design and testing.")),(0,n.yg)("h4",{id:"area"},"Area"),(0,n.yg)("ul",null,(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/bookie"),": Code changes related to bookie storage."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/build"),": Code changes related to project build."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/client"),": Code changes related to clients."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/docker"),": Code changes related to docker builds."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/documentation"),": Code changes related to documentation (including website changes)."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/metadata"),": Code changes related to metadata management."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/protocol"),": Protocol changes."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/release"),": Release related tasks."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/security"),": Security related changes."),(0,n.yg)("li",{parentName:"ul"},(0,n.yg)("strong",{parentName:"li"},"area/tests"),": Tests related changes.")),(0,n.yg)("h4",{id:"priority"},"Priority"),(0,n.yg)("p",null,"At most of the time, it is hard to find a right ",(0,n.yg)("inlineCode",{parentName:"p"},"priority")," for issues. Currently we only have one label ",(0,n.yg)("inlineCode",{parentName:"p"},"priority/blocker")," for marking an issue as a blocker\nfor a given release. Please only mark this issue as ",(0,n.yg)("em",{parentName:"p"},"blocker")," only when it is a real blocker for a given release. If you have no idea about this, just leave\nit as empty."),(0,n.yg)("h4",{id:"status"},"Status"),(0,n.yg)("p",null,"If an issue is assigned to a contributor, that means there is already a contributor working on it. If an issue is unassigned, you can pick this up by assigning\nit to yourself (for committers), or comment on the issue saying you would like to give it a try."),(0,n.yg)("p",null,"If an issue is not an issue anymore, close it and mark it as ",(0,n.yg)("inlineCode",{parentName:"p"},"status/wontfix"),"."),(0,n.yg)("p",null,"All the issues marked as ",(0,n.yg)("inlineCode",{parentName:"p"},"status/help-needed")," are good candidates for new contributors to start with."),(0,n.yg)("h4",{id:"bookkeeper-proposal"},"BookKeeper Proposal"),(0,n.yg)("p",null,"If an issue is a ",(0,n.yg)("inlineCode",{parentName:"p"},"BookKeeper Proposal"),", label it as ",(0,n.yg)("inlineCode",{parentName:"p"},"BP"),"."),(0,n.yg)("h4",{id:"milestone-and-release"},"Milestone and Release"),(0,n.yg)("p",null,"If you want some features merge into a given milestone or release, please associate the issue with a given milestone or release."),(0,n.yg)("p",null,"If you have no idea, just leave them empty. The committers will manage them for you."),(0,n.yg)("p",null,"Thank you for contributing to Apache BookKeeper!"))}g.isMDXComponent=!0}}]);