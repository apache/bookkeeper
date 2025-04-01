"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[9708],{15680:(e,n,t)=>{t.d(n,{xA:()=>p,yg:()=>d});var i=t(96540);function a(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function o(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);n&&(i=i.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,i)}return t}function r(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?o(Object(t),!0).forEach((function(n){a(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function l(e,n){if(null==e)return{};var t,i,a=function(e,n){if(null==e)return{};var t,i,a={},o=Object.keys(e);for(i=0;i<o.length;i++)t=o[i],n.indexOf(t)>=0||(a[t]=e[t]);return a}(e,n);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(i=0;i<o.length;i++)t=o[i],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var g=i.createContext({}),s=function(e){var n=i.useContext(g),t=n;return e&&(t="function"==typeof e?e(n):r(r({},n),e)),t},p=function(e){var n=s(e.components);return i.createElement(g.Provider,{value:n},e.children)},c="mdxType",u={inlineCode:"code",wrapper:function(e){var n=e.children;return i.createElement(i.Fragment,{},n)}},f=i.forwardRef((function(e,n){var t=e.components,a=e.mdxType,o=e.originalType,g=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),c=s(t),f=a,d=c["".concat(g,".").concat(f)]||c[f]||u[f]||o;return t?i.createElement(d,r(r({ref:n},p),{},{components:t})):i.createElement(d,r({ref:n},p))}));function d(e,n){var t=arguments,a=n&&n.mdxType;if("string"==typeof e||a){var o=t.length,r=new Array(o);r[0]=f;var l={};for(var g in n)hasOwnProperty.call(n,g)&&(l[g]=n[g]);l.originalType=e,l[c]="string"==typeof e?e:a,r[1]=l;for(var s=2;s<o;s++)r[s]=t[s];return i.createElement.apply(null,r)}return i.createElement.apply(null,t)}f.displayName="MDXCreateElement"},24067:(e,n,t)=>{t.r(n),t.d(n,{contentTitle:()=>r,default:()=>c,frontMatter:()=>o,metadata:()=>l,toc:()=>g});var i=t(58168),a=(t(96540),t(15680));const o={},r="BP-37: Improve configuration management for better documentation",l={type:"mdx",permalink:"/bps/BP-37-conf-documentation",source:"@site/src/pages/bps/BP-37-conf-documentation.md",title:"BP-37: Improve configuration management for better documentation",description:"Motivation",frontMatter:{}},g=[{value:"Motivation",id:"motivation",level:3},{value:"Public Interfaces",id:"public-interfaces",level:3},{value:"Proposed Changes",id:"proposed-changes",level:3},{value:"Example",id:"example",level:3},{value:"Compatibility, Deprecation, and Migration Plan",id:"compatibility-deprecation-and-migration-plan",level:3},{value:"Test Plan",id:"test-plan",level:3},{value:"Rejected Alternatives",id:"rejected-alternatives",level:3}],s={toc:g},p="wrapper";function c(e){let{components:n,...t}=e;return(0,a.yg)(p,(0,i.A)({},s,t,{components:n,mdxType:"MDXLayout"}),(0,a.yg)("h1",{id:"bp-37-improve-configuration-management-for-better-documentation"},"BP-37: Improve configuration management for better documentation"),(0,a.yg)("h3",{id:"motivation"},"Motivation"),(0,a.yg)("p",null,"One common task in developing bookkeeper is to make sure all the configuration\nsettings are well documented, and the configuration file we ship in each release\nis in-sync with the code itself."),(0,a.yg)("p",null,"However maintaining things in-sync is non-trivial. This proposal is exploring\na new way to manage configuration settings for better documentation."),(0,a.yg)("h3",{id:"public-interfaces"},"Public Interfaces"),(0,a.yg)("ol",null,(0,a.yg)("li",{parentName:"ol"},"Introduced ",(0,a.yg)("inlineCode",{parentName:"li"},"ConfigKey")," for defining a configuration key. A configuration key\nwill include information, such as required/optional, deprecated, documentation\nand etc.")),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-java"},'public class ConfigKey {\n    /**\n     * Flag indicates whether the setting is required.\n     */\n    @Default\n    private boolean required = false;\n\n    /**\n     * Name of the configuration setting.\n     */\n    private String name;\n\n    /**\n     * Type of the configuration setting.\n     */\n    @Default\n    private Type type = Type.STRING;\n\n    /**\n     * Description of the configuration setting.\n     */\n    @Default\n    private String description = "";\n\n    /**\n     * Documentation of the configuration setting.\n     */\n    @Default\n    private String documentation = "";\n\n    /**\n     * Default value as a string representation.\n     */\n    @Default\n    private Object defaultValue = null;\n\n    /**\n     * The list of options for this setting.\n     */\n    @Default\n    private List<String> optionValues = Collections.emptyList();\n\n    /**\n     * The validator used for validating configuration value.\n     */\n    @Default\n    private Validator validator = NullValidator.of();\n\n    /**\n     * The key-group to group settings together.\n     */\n    @Default\n    private ConfigKeyGroup group = ConfigKeyGroup.DEFAULT;\n\n    /**\n     * The order of the setting in the key-group.\n     */\n    @Default\n    private int orderInGroup = Integer.MIN_VALUE;\n\n    /**\n     * The list of settings dependents on this setting.\n     */\n    @Default\n    private List<String> dependents = Collections.emptyList();\n\n    /**\n     * Whether this setting is deprecated or not.\n     */\n    @Default\n    private boolean deprecated = false;\n\n    /**\n     * The config key that deprecates this key.\n     */\n    @Default\n    private String deprecatedByConfigKey = "";\n\n    /**\n     * The version when this settings was deprecated.\n     */\n    @Default\n    private String deprecatedSince = "";\n\n    /**\n     * The version when this setting was introduced.\n     */\n    @Default\n    private String since = "";\n}\n')),(0,a.yg)("ol",{start:2},(0,a.yg)("li",{parentName:"ol"},"Introduced ",(0,a.yg)("inlineCode",{parentName:"li"},"ConfigKeyGroup")," for grouping configuration keys together. ")),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-java"},'public class ConfigKeyGroup {\n    /**\n     * Name of the key group.\n     */\n    private String name;\n\n    /**\n     * Description of the key group.\n     */\n    @Default\n    private String description = "";\n\n    /**\n     * The list of sub key-groups of this key group.\n     */\n    @Default\n    private List<String> children = Collections.emptyList();\n\n    /**\n     * The order of the key-group in a configuration.\n     */\n    @Default\n    private int order = Integer.MIN_VALUE;\n}\n')),(0,a.yg)("h3",{id:"proposed-changes"},"Proposed Changes"),(0,a.yg)("p",null,"Besides introducing ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigKey")," and ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigKeyGroup"),", this BP will also introduce a class\n",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigDef")," - it defines the keys for a configuration. "),(0,a.yg)("p",null,"The ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigDef")," will be generated via ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigDef.of(Configuration.class)"),". It will retrieve\nall the static fields of ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigKey")," defined in the configuration class and build the configuration\ndefinition."),(0,a.yg)("p",null,"The ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigDef")," will also provide a ",(0,a.yg)("inlineCode",{parentName:"p"},"save")," method for saving the configuration definition\nas a configuration file."),(0,a.yg)("h3",{id:"example"},"Example"),(0,a.yg)("p",null,"Following is an example how to use ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigKey")," and ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigKeyGroup")," to organize\nconfiguration settings."),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-java"},'// Ledger Storage Settings\n\nprivate static final ConfigKeyGroup GROUP_LEDGER_STORAGE = ConfigKeyGroup.builder("ledgerstorage")\n    .description("Ledger Storage related settings")\n    .order(10) // place a place holder here\n    .build();\n\nprotected static final String LEDGER_STORAGE_CLASS = "ledgerStorageClass";\nprotected static final ConfigKey LEDGER_STORAGE_CLASS_KEY = ConfigKey.builder(LEDGER_STORAGE_CLASS)\n    .type(Type.CLASS)\n    .description("Ledger storage implementation class")\n    .defaultValue(SortedLedgerStorage.class.getName())\n    .optionValues(Lists.newArrayList(\n        InterleavedLedgerStorage.class.getName(),\n        SortedLedgerStorage.class.getName(),\n        DbLedgerStorage.class.getName()\n    ))\n    .validator(ClassValidator.of(LedgerStorage.class))\n    .group(GROUP_LEDGER_STORAGE)\n    .build();\n')),(0,a.yg)("p",null,"Example on how to generate the ",(0,a.yg)("inlineCode",{parentName:"p"},"ConfigDef")," and use the configuration definition to\nvalidate if a configuration instance is valid."),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-java"},"// generate config def\nConfigDef configDef = ConfigDef.of(ServerConfiguration.class);\ntry {\n    configDef.validate(this);\n} catch (ConfigException e) {\n    throw new ConfigurationException(e.getMessage(), e.getCause());\n}\n")),(0,a.yg)("p",null,"Example on how to save the configuration definition to a configuration file."),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-java"},"ConfigDef configDef = ConfigDef.of(TestConfig2.class);\nString savedConf;\ntry (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {\n    configDef.save(baos);\n    savedConf = baos.toString();\n}\n")),(0,a.yg)("h3",{id:"compatibility-deprecation-and-migration-plan"},"Compatibility, Deprecation, and Migration Plan"),(0,a.yg)("p",null,"It only changes the way how we organize configuration settings and how we document them.\nIt doesn't change the public interfaces for existing configuration. So there is nothing\nto deprecate and migrate."),(0,a.yg)("h3",{id:"test-plan"},"Test Plan"),(0,a.yg)("p",null,"Existing testing is good enough to cover code changes. No new tests are needed."),(0,a.yg)("h3",{id:"rejected-alternatives"},"Rejected Alternatives"),(0,a.yg)("p",null,"Alternatively, we have to manually maintain the configuration files and update each time\nwhen a new configuration setting is added."))}c.isMDXComponent=!0}}]);