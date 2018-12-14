---
title: "BP-37: Improve configuration management for better documentation"
issue: https://github.com/apache/bookkeeper/1867
state: "Accepted"
release: "4.9.0"
---

### Motivation

One common task in developing bookkeeper is to make sure all the configuration
settings are well documented, and the configuration file we ship in each release
is in-sync with the code itself.

However maintaining things in-sync is non-trivial. This proposal is exploring
a new way to manage configuration settings for better documentation.

### Public Interfaces

1. Introduced `ConfigKey` for defining a configuration key. A configuration key
   will include informations, such as required/optional, deprecated, documentation
   and etc.

```java
public class ConfigKey {
    /**
     * Flag indicates whether the setting is required.
     */
    @Default
    private boolean required = false;

    /**
     * Name of the configuration setting.
     */
    private String name;

    /**
     * Type of the configuration setting.
     */
    @Default
    private Type type = Type.STRING;

    /**
     * Description of the configuration setting.
     */
    @Default
    private String description = "";

    /**
     * Documentation of the configuration setting.
     */
    @Default
    private String documentation = "";

    /**
     * Default value as a string representation.
     */
    @Default
    private Object defaultValue = null;

    /**
     * The list of options for this setting.
     */
    @Default
    private List<String> optionValues = Collections.emptyList();

    /**
     * The validator used for validating configuration value.
     */
    @Default
    private Validator validator = NullValidator.of();

    /**
     * The key-group to group settings together.
     */
    @Default
    private ConfigKeyGroup group = ConfigKeyGroup.DEFAULT;

    /**
     * The order of the setting in the key-group.
     */
    @Default
    private int orderInGroup = Integer.MIN_VALUE;

    /**
     * The list of settings dependents on this setting.
     */
    @Default
    private List<String> dependents = Collections.emptyList();

    /**
     * Whether this setting is deprecated or not.
     */
    @Default
    private boolean deprecated = false;

    /**
     * The config key that deprecates this key.
     */
    @Default
    private String deprecatedByConfigKey = "";

    /**
     * The version when this settings was deprecated.
     */
    @Default
    private String deprecatedSince = "";

    /**
     * The version when this setting was introduced.
     */
    @Default
    private String since = "";
}
```

2. Introduced `ConfigKeyGroup` for grouping configuration keys together. 

```java
public class ConfigKeyGroup {
    /**
     * Name of the key group.
     */
    private String name;

    /**
     * Description of the key group.
     */
    @Default
    private String description = "";

    /**
     * The list of sub key-groups of this key group.
     */
    @Default
    private List<String> children = Collections.emptyList();

    /**
     * The order of the key-group in a configuration.
     */
    @Default
    private int order = Integer.MIN_VALUE;
}
```

### Proposed Changes

Besides introducing `ConfigKey` and `ConfigKeyGroup`, this BP will also introduce a class
`ConfigDef` - it defines the keys for a configuration. 

The `ConfigDef` will be generated via `ConfigDef.of(Configuration.class)`. It will retrieve
all the static fields of `ConfigKey` defined in the configuration class and build the configuration
definition.

The `ConfigDef` will also provide a `save` method for saving the configuration definition
as a configuration file.

### Example

Following is an example how to use `ConfigKey` and `ConfigKeyGroup` to organize
configuration settings.

```java
// Ledger Storage Settings

private static final ConfigKeyGroup GROUP_LEDGER_STORAGE = ConfigKeyGroup.builder("ledgerstorage")
    .description("Ledger Storage related settings")
    .order(10) // place a place holder here
    .build();

protected static final String LEDGER_STORAGE_CLASS = "ledgerStorageClass";
protected static final ConfigKey LEDGER_STORAGE_CLASS_KEY = ConfigKey.builder(LEDGER_STORAGE_CLASS)
    .type(Type.CLASS)
    .description("Ledger storage implementation class")
    .defaultValue(SortedLedgerStorage.class.getName())
    .optionValues(Lists.newArrayList(
        InterleavedLedgerStorage.class.getName(),
        SortedLedgerStorage.class.getName(),
        DbLedgerStorage.class.getName()
    ))
    .validator(ClassValidator.of(LedgerStorage.class))
    .group(GROUP_LEDGER_STORAGE)
    .build();
```

Example on how to generate the `ConfigDef` and use the configuration definition to
validate if a configuration instance is valid.

```java
// generate config def
ConfigDef configDef = ConfigDef.of(ServerConfiguration.class);
try {
    configDef.validate(this);
} catch (ConfigException e) {
    throw new ConfigurationException(e.getMessage(), e.getCause());
}
```     

Example on how to save the configuration definition to a configuration file.

```java
ConfigDef configDef = ConfigDef.of(TestConfig2.class);
String savedConf;
try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
    configDef.save(baos);
    savedConf = baos.toString();
}
```

### Compatibility, Deprecation, and Migration Plan

It only changes the way how we organize configuration settings and how we document them.
It doesn't change the public interfaces for existing configuration. So there is nothing
to deprecate and migrate.

### Test Plan

Existing testing is good enough to cover code changes. No new tests are needed.

### Rejected Alternatives

Alternatively, we have to manually maintain the configuration files and update each time
when a new configuration setting is added. 
