# 🚀 TESTING QUICK START GUIDE

**Data:** 7 gennaio 2026  
**Status:** ✅ READY TO USE

---

## ⚡ Avvio Rapido (5 minuti)

### 1️⃣ Eseguire i Test

```bash
# Navigare al modulo di test
cd bookkeeper-tests-demo

# Eseguire tutti i test
mvn clean test

# Output atteso:
# Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
# BUILD SUCCESS
```

### 2️⃣ Generare Report HTML

```bash
# Creare Surefire HTML report
mvn surefire-report:report

# Creare JaCoCo HTML coverage report
mvn jacoco:report

# Report locations:
# - Test Report: target/reports/surefire.html
# - Coverage Report: target/site/jacoco/index.html
```

### 3️⃣ Visualizzare i Report

```bash
# Aprire test report nel browser
start target/reports/surefire.html

# Oppure aprire coverage report
start target/site/jacoco/index.html
```

---

## 📂 Struttura Moduli

### bookkeeper-tests-demo (DEMO STANDALONE)
**Perfetto per imparare e testare rapidamente**

```
bookkeeper-tests-demo/
├── pom.xml                                  # Maven configuration
├── src/test/java/org/apache/bookkeeper/
│   ├── zookeeper/
│   │   └── ExponentialBackoffRetryPolicyDemoTest.java
│   │       ├── testAllowRetryValid() ✅
│   │       ├── testAllowRetryExceeds() ✅
│   │       ├── testNextRetryWaitTime() ✅
│   │       └── testBackoffIncrease() ✅
│   │
│   └── bookie/
│       └── EntryMemTableDemoTest.java
│           ├── testInitializationEmpty() ✅
│           ├── testAddEntryIncreasesSize() ✅
│           ├── testMultipleEntriesSameLedger() ✅
│           ├── testEntriesDifferentLedgers() ✅
│           └── testSnapshotCreation() ✅
│
└── target/
    ├── jacoco.exec               # Coverage data
    ├── reports/surefire.html     # Test report
    └── site/jacoco/index.html    # Coverage report
```

### bookkeeper-server (PRODUCTION TEST SUITES)
**Per testing completo nel build principale**

```
bookkeeper-server/src/test/java/org/apache/bookkeeper/
│
├── zookeeper/
│   ├── ExponentialBackoffRetryPolicyMockStubTest.java    (10 tests)
│   ├── ExponentialBackoffRetryPolicyLLMTest.java         (15+ tests)
│   └── ExponentialBackoffRetryPolicyControlFlowTest.java (15+ tests)
│
└── bookie/
    ├── EntryMemTableMockStubTest.java                    (10 tests)
    ├── EntryMemTableLLMTest.java                         (15+ tests)
    └── EntryMemTableControlFlowTest.java                 (25+ tests)
```

---

## 🧪 Comandi Maven Comuni

### Esecuzione Test

```bash
# Eseguire tutti i test
mvn clean test

# Eseguire una test class specifica
mvn test -Dtest=ExponentialBackoffRetryPolicyDemoTest

# Eseguire un test method specifico
mvn test -Dtest=ExponentialBackoffRetryPolicyDemoTest#testAllowRetryValid

# Eseguire test in parallelo
mvn test -DparallelTestClasses=true -DthreadCount=4

# Eseguire test senza compilare
mvn test -DskipCompile=true
```

### Generare Report

```bash
# Surefire Test Report
mvn surefire-report:report
# Output: target/reports/surefire.html

# JaCoCo Coverage Report
mvn jacoco:report
# Output: target/site/jacoco/index.html

# PITest Mutation Report
mvn org.pitest:pitest-maven:mutationCoverage
# Output: target/pit-reports/index.html

# Tutti i report
mvn clean test jacoco:report surefire-report:report
```

### Build Completo

```bash
# Compilare, testare, generare report
mvn clean test jacoco:report surefire-report:report

# Compilare con install
mvn clean install -DskipTests

# Build con tutti i report
mvn clean install -DskipTests && mvn test jacoco:report
```

---

## 📊 Risultati Attesi

### Test Execution

```
✅ ExponentialBackoffRetryPolicyDemoTest
   ✓ testAllowRetryValid
   ✓ testAllowRetryExceeds
   ✓ testNextRetryWaitTime
   ✓ testBackoffIncrease
   
✅ EntryMemTableDemoTest
   ✓ testInitializationEmpty
   ✓ testAddEntryIncreasesSize
   ✓ testMultipleEntriesSameLedger
   ✓ testEntriesDifferentLedgers
   ✓ testSnapshotCreation

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tests run: 9
Failures: 0
Errors: 0
Skipped: 0
Success Rate: 100%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Report Output

#### Surefire Test Report (HTML)
```
📄 target/reports/surefire.html
├── Test Summary
│   ├── Tests Run: 9
│   ├── Passed: 9
│   ├── Failed: 0
│   └── Skipped: 0
│
├── Test Results by Class
│   ├── ExponentialBackoffRetryPolicyDemoTest
│   │   ├── Package: org.apache.bookkeeper.zookeeper
│   │   ├── Duration: 0.024s
│   │   └── Tests: 4/4 passed
│   │
│   └── EntryMemTableDemoTest
│       ├── Package: org.apache.bookkeeper.bookie
│       ├── Duration: 0.134s
│       └── Tests: 5/5 passed
│
└── Detailed Results
    ├── Each test method with status
    ├── Execution time per test
    └── Error traces (if any)
```

#### JaCoCo Coverage Report (HTML)
```
📄 target/site/jacoco/index.html
├── Coverage Summary
│   ├── Line Coverage: 50%+ (target: 50%)
│   ├── Branch Coverage: 40%+ (target: 40%)
│   └── Complexity: Low-Medium
│
├── Package Coverage
│   ├── org.apache.bookkeeper.zookeeper
│   └── org.apache.bookkeeper.bookie
│
├── Class Details
│   ├── Line-by-line coverage
│   ├── Branch coverage analysis
│   └── Uncovered code highlighting
│
└── CSV Export
    ├── Coverage data by class
    └── Trend data (if running repeatedly)
```

---

## 🔍 Interpretazione Report

### Coverage Metrics

| Metrica | Significato | Target |
|---------|-------------|--------|
| **LINE** | % di linee di codice eseguite | > 50% |
| **BRANCH** | % di branch eseguiti (if/else) | > 40% |
| **COMPLEXITY** | Complessità ciclomatica | < 15 |
| **METHOD** | % di metodi testati | > 50% |

### Color Coding in JaCoCo

- 🟢 **GREEN**: Fully covered (all paths executed)
- 🟡 **YELLOW**: Partially covered (some paths missing)
- 🔴 **RED**: Not covered (code never executed)

### Test Status Icons

- ✅ **PASSED**: Test executed successfully
- ❌ **FAILED**: Test execution failed
- ⏭️ **SKIPPED**: Test not executed
- ⚠️ **ERROR**: Unexpected error during execution

---

## 🛠️ Troubleshooting

### Test Esecuzione Fallisce

```bash
# 1. Pulire e ricompilare
mvn clean compile

# 2. Verificare dipendenze
mvn dependency:tree

# 3. Eseguire con verbose
mvn test -X

# 4. Controllare Java version
java -version

# Expected: Java 11 or higher
```

### Report non Generato

```bash
# 1. Verificare che i test siano passati
mvn test

# 2. Generare manualmente
mvn jacoco:report

# 3. Controllare target directory
dir target/

# 4. Verificare pom.xml configuration
mvn help:active-profiles
```

### Memory Issues

```bash
# Aumentare memoria JVM per Maven
set MAVEN_OPTS=-Xmx1024m -Xms512m

# Poi eseguire i test
mvn clean test
```

---

## 📈 Monitorare Progress

### Verificare Coverage Over Time

```bash
# Eseguire test e salvare report
mvn clean test jacoco:report

# Check coverage percentage
dir target\site\jacoco\index.html

# Confrontare con esecuzione precedente
# (JaCoCo memorizzerà in .jacoco/ folder)
```

### Tracking Test Results

```bash
# Salvare report in versioning
cp target/reports/surefire.html surefire-report-$(date +%Y%m%d).html

# Guardare trend nel tempo
ls -la surefire-report-*.html
```

---

## 🎯 Prossimi Passi

### 1. Integrare nel Build Principale
```bash
# Copiare test files dal demo al bookkeeper-server
cp bookkeeper-tests-demo/src/test/java/org/apache/bookkeeper/zookeeper/* \
   bookkeeper-server/src/test/java/org/apache/bookkeeper/zookeeper/

# Verificare Maven build
mvn -f bookkeeper-server/pom.xml test
```

### 2. Configurare CI/CD
```bash
# GitHub Actions workflow è già in .github/workflows/test-pipeline.yml
# Solo da pushare sul repository

# Verificare che è valido
mvn clean test
```

### 3. Estendere Test Suite
```bash
# Aggiungere più classi da testare
# Seguire lo stesso pattern: Mock/Stub, LLM, Control-Flow

# Template di classe test
# src/test/java/org/apache/bookkeeper/YourClassTest.java
```

### 4. Monitorare Coverage
```bash
# Settare threshold JaCoCo nel pom.xml
<minimum>0.80</minimum>  # Per aumentare target coverage

# Build fallirà se coverage è sotto la soglia
```

---

## 📚 Resource Link

### File di Configurazione
- [📄 pom.xml](../bookkeeper-tests-demo/pom.xml) - Maven POM
- [📄 test-pipeline.yml](../.github/workflows/test-pipeline.yml) - GitHub Actions

### Test Files
- [📄 ExponentialBackoffRetryPolicyDemoTest.java](../bookkeeper-tests-demo/src/test/java/org/apache/bookkeeper/zookeeper/ExponentialBackoffRetryPolicyDemoTest.java)
- [📄 EntryMemTableDemoTest.java](../bookkeeper-tests-demo/src/test/java/org/apache/bookkeeper/bookie/EntryMemTableDemoTest.java)

### Documentation
- [📄 TESTING_FRAMEWORK_CONFIGURATION.md](./TESTING_FRAMEWORK_CONFIGURATION.md) - Full configuration guide
- [📄 TEST_EXECUTION_REPORT.md](./TEST_EXECUTION_REPORT.md) - Detailed execution results

---

## ✅ Checklist Pre-Deploy

- [ ] Eseguire `mvn clean test` e verificare 9/9 passing
- [ ] Generare report HTML con `mvn jacoco:report surefire-report:report`
- [ ] Aprire `target/reports/surefire.html` e controllare risultati
- [ ] Aprire `target/site/jacoco/index.html` e verificare coverage
- [ ] Verificare che nessun test sia skipped o errori
- [ ] Confermare BUILD SUCCESS al termine
- [ ] Coverage LINE > 50% e BRANCH > 40%
- [ ] Testare su Java 11 e Java 17
- [ ] Preparare PR con all'interno i test files
- [ ] Configurare GitHub Actions per auto-testing

---

## 🆘 Supporto

### Errori Comuni

**Error: "No tests found"**
```bash
mvn test -DskipCompile=false
# Verificare che @Test annotation sia importata da org.junit.jupiter
```

**Error: "Coverage below threshold"**
```bash
# Aumentare soglia o aggiungere più test
# Edit: <minimum>0.50</minimum> nel pom.xml
```

**Error: "Maven build failed"**
```bash
# Pulire cache
rm -rf ~/.m2/repository
mvn clean install -DskipTests
```

---

## 🎉 Success Criteria

✅ **Tutti i test passano (9/9)**  
✅ **Coverage > 50% per line, > 40% per branch**  
✅ **Report HTML generati correttamente**  
✅ **GitHub Actions workflow funzionante**  
✅ **Nessun errore di compilazione**  
✅ **Build SUCCESS completato**  

---

**Generated:** 7 gennaio 2026  
**Version:** 1.0  
**Status:** ✅ PRODUCTION READY
