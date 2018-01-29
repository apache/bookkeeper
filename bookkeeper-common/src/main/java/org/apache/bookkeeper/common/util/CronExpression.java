/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.common.util;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This provides cron support for java8 using java-time.
 * learned from https://github.com/frode-carlsen/cron
 *
 * <p>The support cron expressions consists of 6 mandatory fields separated by space. <br>
 * These are:
 * <code>Types: valid value range</code>
 * <code>Seconds: 0-59</code>
 * <code>Minutes: 0-59</code>
 * <td align="left"><code>Hours: 0-23</code></td>
 * <td align="left"><code>Day of month: 1-31</code></td>
 * <td align="left"><code>Month: 1-12 or JAN-DEC (note: english abbreviations)</code>
 * <td align="left"><code>Day of week: 1-7 or MON-SUN (note: english abbreviations)</code>
 * The support special character includes:'*', '?', '-', ',', '/', 'L', 'W' and '#'.
 * Note that no fields are case-sensitive. Fields are always evaluated independently,
 * but the expression doesn't match until the constraints of each field are met.
 * Overlap of intervals are not allowed. That is: for
 * Day-of-week field &quot;FRI-MON&quot; is invalid,but &quot;FRI-SUN,MON&quot; is valid
 */

public class CronExpression {

    enum CronFieldType {
        SECOND(0, 59, null), MINUTE(0, 59, null), HOUR(0, 23, null), DAY_OF_MONTH(1, 31, null), MONTH(1, 12,
        Arrays.asList("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")),
        DAY_OF_WEEK(1, 7, Arrays.asList("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"));

        final int from, to;
        final List<String> names;

        CronFieldType(int from, int to, List<String> names) {
            this.from = from;
            this.to = to;
            this.names = names;
        }
    }

    private final String expr;
    private final SimpleField secondField;
    private final SimpleField minuteField;
    private final SimpleField hourField;
    private final DayOfWeekField dayOfWeekField;
    private final SimpleField monthField;
    private final DayOfMonthField dayOfMonthField;

    public CronExpression(final String expr) {
        if (expr == null) {
            throw new IllegalArgumentException("expr is null"); //$NON-NLS-1$
        }

        this.expr = expr;

        final int expectedParts = 6;
        final String[] parts = expr.split("\\s+"); //$NON-NLS-1$
        if (parts.length != expectedParts) {
            throw new IllegalArgumentException(
                    String.format("Invalid cron expression [%s], expected %s field, got %s",
                            expr, expectedParts, parts.length));
        }

        this.secondField = new SimpleField(CronFieldType.SECOND, parts[0]);
        this.minuteField = new SimpleField(CronFieldType.MINUTE, parts[1]);
        this.hourField = new SimpleField(CronFieldType.HOUR, parts[2]);
        this.dayOfMonthField = new DayOfMonthField(parts[3]);
        this.monthField = new SimpleField(CronFieldType.MONTH, parts[4]);
        this.dayOfWeekField = new DayOfWeekField(parts[5]);
    }

    public ZonedDateTime nextTimeAfter(ZonedDateTime afterTime) {
        // will search for the next time within the next 4 years. If there is no
        // time matching, an InvalidArgumentException will be thrown (it is very
        // likely that the cron expression is invalid, like the February 30th).
        return nextTimeAfter(afterTime, afterTime.plusYears(4));
    }

    public ZonedDateTime nextTimeAfter(ZonedDateTime afterTime, long durationInMillis) {
        // will search for the next time within the next durationInMillis
        // millisecond. Be aware that the duration is specified in millis,
        // but in fact the limit is checked on a day-to-day basis.
        return nextTimeAfter(afterTime, afterTime.plus(Duration.ofMillis(durationInMillis)));
    }

    public ZonedDateTime nextTimeAfter(ZonedDateTime afterTime, ZonedDateTime dateTimeBarrier) {
        ZonedDateTime nextTime = ZonedDateTime.from(afterTime).withNano(0).plusSeconds(1).withNano(0);

        while (true) { // day of week
            while (true) { // month
                while (true) { // day of month
                    while (true) { // hour
                        while (true) { // minute
                            while (true) { // second
                                if (secondField.matches(nextTime.getSecond())) {
                                    break;
                                }
                                nextTime = nextTime.plusSeconds(1).withNano(0);
                            }
                            if (minuteField.matches(nextTime.getMinute())) {
                                break;
                            }
                            nextTime = nextTime.plusMinutes(1).withSecond(0).withNano(0);
                        }
                        if (hourField.matches(nextTime.getHour())) {
                            break;
                        }
                        nextTime = nextTime.plusHours(1).withMinute(0).withSecond(0).withNano(0);
                    }
                    if (dayOfMonthField.matches(nextTime.toLocalDate())) {
                        break;
                    }
                    nextTime = nextTime.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
                    checkIfDateTimeBarrierIsReached(nextTime, dateTimeBarrier);
                }
                if (monthField.matches(nextTime.getMonth().getValue())) {
                    break;
                }
                nextTime = nextTime.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
                checkIfDateTimeBarrierIsReached(nextTime, dateTimeBarrier);
            }
            if (dayOfWeekField.matches(nextTime.toLocalDate())) {
                break;
            }
            nextTime = nextTime.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            checkIfDateTimeBarrierIsReached(nextTime, dateTimeBarrier);
        }

        return nextTime;
    }

    private static void checkIfDateTimeBarrierIsReached(ZonedDateTime nextTime, ZonedDateTime dateTimeBarrier) {
        if (nextTime.isAfter(dateTimeBarrier)) {
            throw new IllegalArgumentException(
                    "No next execution time could be determined that is before the limit of " + dateTimeBarrier);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "<" + expr + ">";
    }

    static class FieldPart {
        private Integer from, to, increment;
        private String modifier, incrementModifier;
    }

    abstract static class BasicField {
        private static final Pattern CRON_FIELD_REGEXP = Pattern
                .compile("(?:                                                  # start of group 1\n"
                        + "   (?:(?<all>\\*)|(?<ignore>\\?)|(?<last>L))# global flag (L, ?, *)\n"
                        + " | (?<start>[0-9]{1,2}|[a-z]{3,3})          # or start number or symbol\n"
                        + "      (?:                                   # start of group 2\n"
                        + "         (?<mod>L|W)                        # modifier (L,W)\n"
                        + "       | -(?<end>[0-9]{1,2}|[a-z]{3,3})     # or end nummer or symbol (in range)\n"
                        + "      )?                                    # end of group 2\n"
                        + ")                                           # end of group 1\n"
                        + "(?:(?<incmod>/|\\#)(?<inc>[0-9]{1,7}))?     # increment and increment modifier (/ or \\#)\n",
                        Pattern.CASE_INSENSITIVE | Pattern.COMMENTS);

        final CronFieldType fieldType;
        final List<FieldPart> parts = new ArrayList<>();

        private BasicField(CronFieldType fieldType, String fieldExpr) {
            this.fieldType = fieldType;
            parse(fieldExpr);
        }

        private void parse(String fieldExpr) { // NOSONAR
            String[] rangeParts = fieldExpr.split(",");
            for (String rangePart : rangeParts) {
                Matcher m = CRON_FIELD_REGEXP.matcher(rangePart);
                if (!m.matches()) {
                    throw new IllegalArgumentException(
                            "Invalid cron field '" + rangePart + "' for field [" + fieldType + "]");
                }
                String startNummer = m.group("start");
                String modifier = m.group("mod");
                String sluttNummer = m.group("end");
                String incrementModifier = m.group("incmod");
                String increment = m.group("inc");

                FieldPart part = new FieldPart();
                part.increment = 999;
                if (startNummer != null) {
                    part.from = mapValue(startNummer);
                    part.modifier = modifier;
                    if (sluttNummer != null) {
                        part.to = mapValue(sluttNummer);
                        part.increment = 1;
                    } else if (increment != null) {
                        part.to = fieldType.to;
                    } else {
                        part.to = part.from;
                    }
                } else if (m.group("all") != null) {
                    part.from = fieldType.from;
                    part.to = fieldType.to;
                    part.increment = 1;
                } else if (m.group("ignore") != null) {
                    part.modifier = m.group("ignore");
                } else if (m.group("last") != null) {
                    part.modifier = m.group("last");
                } else {
                    throw new IllegalArgumentException("Invalid cron part: " + rangePart);
                }

                if (increment != null) {
                    part.incrementModifier = incrementModifier;
                    part.increment = Integer.valueOf(increment);
                }

                validateRange(part);
                validatePart(part);
                parts.add(part);

            }
        }

        protected void validatePart(FieldPart part) {
            if (part.modifier != null) {
                throw new IllegalArgumentException(
                        String.format("Invalid modifier [%s]", part.modifier));
            } else if (part.incrementModifier != null && !"/".equals(part.incrementModifier)) {
                throw new IllegalArgumentException(
                        String.format("Invalid increment modifier [%s]", part.incrementModifier));
            }
        }

        private void validateRange(FieldPart part) {
            if ((part.from != null && part.from < fieldType.from) || (part.to != null && part.to > fieldType.to)) {
                throw new IllegalArgumentException(
                        String.format("Invalid interval [%s-%s], must be %s<=_<=%s",
                                part.from, part.to, fieldType.from, fieldType.to));
            } else if (part.from != null && part.to != null && part.from > part.to) {
                throw new IllegalArgumentException(
                        String.format("Invalid interval [%s-%s].  Rolling periods are not supported (ex. 5-1, only 1-5)"
                                        + " since this won't give a deterministic result. Must be %s<=_<=%s",
                                part.from, part.to, fieldType.from, fieldType.to));
            }
        }

        protected Integer mapValue(String value) {
            Integer idx;
            if (fieldType.names != null && (idx =
                    fieldType.names.indexOf(value.toUpperCase(Locale.getDefault()))) >= 0) {
                return idx + 1;
            }
            return Integer.valueOf(value);
        }

        protected boolean matches(int val, FieldPart part) {
            if (val >= part.from && val <= part.to && (val - part.from) % part.increment == 0) {
                return true;
            }
            return false;
        }
    }

    static class SimpleField extends BasicField {
        SimpleField(CronFieldType fieldType, String fieldExpr) {
            super(fieldType, fieldExpr);
        }

        public boolean matches(int val) {
            if (val >= fieldType.from && val <= fieldType.to) {
                for (FieldPart part : parts) {
                    if (matches(val, part)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    static class DayOfWeekField extends BasicField {

        DayOfWeekField(String fieldExpr) {
            super(CronFieldType.DAY_OF_WEEK, fieldExpr);
        }

        boolean matches(LocalDate dato) {
            for (FieldPart part : parts) {
                if ("L".equals(part.modifier)) {
                    YearMonth ym = YearMonth.of(dato.getYear(), dato.getMonth().getValue());
                    return dato.getDayOfWeek() == DayOfWeek.of(part.from)
                            && dato.getDayOfMonth() > (ym.lengthOfMonth() - 7);
                } else if ("#".equals(part.incrementModifier)) {
                    if (dato.getDayOfWeek() == DayOfWeek.of(part.from)) {
                        int num = dato.getDayOfMonth() / 7;
                        return part.increment == (dato.getDayOfMonth() % 7 == 0 ? num : num + 1);
                    }
                    return false;
                } else if (matches(dato.getDayOfWeek().getValue(), part)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Integer mapValue(String value) {
            // Use 1-7 for weedays, but 0 will also represent sunday (linux practice)
            return "0".equals(value) ? Integer.valueOf(7) : super.mapValue(value);
        }

        @Override
        protected boolean matches(int val, FieldPart part) {
            return "?".equals(part.modifier) || super.matches(val, part);
        }

        @Override
        protected void validatePart(FieldPart part) {
            if (part.modifier != null && Arrays.asList("L", "?").indexOf(part.modifier) == -1) {
                throw new IllegalArgumentException(String.format("Invalid modifier [%s]", part.modifier));
            } else if (part.incrementModifier != null
                    && Arrays.asList("/", "#").indexOf(part.incrementModifier) == -1) {
                throw new IllegalArgumentException(
                        String.format("Invalid increment modifier [%s]", part.incrementModifier));
            }
        }
    }

    static class DayOfMonthField extends BasicField {
        DayOfMonthField(String fieldExpr) {
            super(CronFieldType.DAY_OF_MONTH, fieldExpr);
        }

        boolean matches(LocalDate dato) {
            for (FieldPart part : parts) {
                if ("L".equals(part.modifier)) {
                    YearMonth ym = YearMonth.of(dato.getYear(), dato.getMonth().getValue());
                    return dato.getDayOfMonth() == (ym.lengthOfMonth() - (part.from == null ? 0 : part.from));
                } else if ("W".equals(part.modifier)) {
                    if (dato.getDayOfWeek().getValue() <= 5) {
                        if (dato.getDayOfMonth() == part.from) {
                            return true;
                        } else if (dato.getDayOfWeek().getValue() == 5) {
                            return dato.plusDays(1).getDayOfMonth() == part.from;
                        } else if (dato.getDayOfWeek().getValue() == 1) {
                            return dato.minusDays(1).getDayOfMonth() == part.from;
                        }
                    }
                } else if (matches(dato.getDayOfMonth(), part)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void validatePart(FieldPart part) {
            if (part.modifier != null && Arrays.asList("L", "W", "?").indexOf(part.modifier) == -1) {
                throw new IllegalArgumentException(String.format("Invalid modifier [%s]", part.modifier));
            } else if (part.incrementModifier != null && !"/".equals(part.incrementModifier)) {
                throw new IllegalArgumentException(
                        String.format("Invalid increment modifier [%s]", part.incrementModifier));
            }
        }

        @Override
        protected boolean matches(int val, FieldPart part) {
            return "?".equals(part.modifier) || super.matches(val, part);
        }
    }
}
