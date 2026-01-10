package org.apache.bookkeeper;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class JaCoCoTestDiagnostic {
    
    @Test
    public void testSimpleCalculation() {
        DiagnosticCalculator calc = new DiagnosticCalculator();
        assertEquals(5, calc.add(2, 3));
        assertEquals(10, calc.multiply(2, 5));
    }
}

class DiagnosticCalculator {
    public int add(int a, int b) {
        return a + b;
    }
    
    public int multiply(int a, int b) {
        return a * b;
    }
}
