/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.tools.perf.utils;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;

/**
 * A decimal format that adds padding zeros.
 */
public class PaddingDecimalFormat extends DecimalFormat {
    private int minimumLength;

    /**
     * Creates a PaddingDecimalFormat using the given pattern and minimum minimumLength and the symbols for the default
     * locale.
     */
    public PaddingDecimalFormat(String pattern, int minLength) {
        super(pattern);
        minimumLength = minLength;
    }

    /**
     * Creates a PaddingDecimalFormat using the given pattern, symbols and minimum minimumLength.
     */
    public PaddingDecimalFormat(String pattern, DecimalFormatSymbols symbols, int minLength) {
        super(pattern, symbols);
        minimumLength = minLength;
    }

    @Override
    public StringBuffer format(double number, StringBuffer toAppendTo, FieldPosition pos) {
        int initLength = toAppendTo.length();
        super.format(number, toAppendTo, pos);
        return pad(toAppendTo, initLength);
    }

    @Override
    public StringBuffer format(long number, StringBuffer toAppendTo, FieldPosition pos) {
        int initLength = toAppendTo.length();
        super.format(number, toAppendTo, pos);
        return pad(toAppendTo, initLength);
    }

    private StringBuffer pad(StringBuffer toAppendTo, int initLength) {
        int numLength = toAppendTo.length() - initLength;
        int padLength = minimumLength - numLength;
        if (padLength > 0) {
            StringBuffer pad = new StringBuffer(padLength);
            for (int i = 0; i < padLength; i++) {
                pad.append(' ');
            }
            toAppendTo.insert(initLength, pad);
        }
        return toAppendTo;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PaddingDecimalFormat)) {
            return false;
        }

        PaddingDecimalFormat other = (PaddingDecimalFormat) obj;
        return minimumLength == other.minimumLength && super.equals(obj);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + minimumLength;
    }
}
