---
name: Flaky test
about: Report a flaky test failure
title: 'Flaky-test: test_class.test_method'
labels: ["area/tests", "flaky-tests"]
assignees: ''
---
<!--- 

Instructions for reporting a flaky test using this issue template:

1. Replace test_class in title and body with the short test class name WITHOUT the package name.
2. Replace test_method in title and body with the test method that failed. Multiple methods are flaky, remove the content that refers to the test method.
3. Replace "relevant parts of the exception stacktrace here" with the a few lines of the stack trace that shows at least the exception message and the line of test code where the stacktrace occurred.
4. Replace "full exception stacktrace here" with the full exception stacktrace from logs. This section will be hidden by default.
5. Remove all unused fields / content to unclutter the reported issue. Remove this comment too.

-->
test_class.test_method is flaky. It fails sporadically.

```
[relevant parts of the exception stacktrace here]
```

<!-- optionally provide the full stacktrace ->
<details>
<summary>Full exception stacktrace</summary>
<code><pre>
full exception stacktrace here
</pre></code>
</details>
