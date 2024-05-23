package org.apache.bookkeeper.common.collections;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a range of hours.
 * <p> The range is defined as a comma-separated list of ranges. Each range is defined as a start hour and an end hour
 * separated by a dash. For example, "0-5, 10-15" represents the range of hours from 0 to 5 and 10 to 15.
 * <p> The range is inclusive, i.e. the start and end hours are included in the range.
 * <p> The range is defined in 24-hour format, i.e. the hours are in the range of 0 to 23.
 * <p> we require that the start hour is less than or equal to the end hour.
 */
public class HourRange {
    private static final String RANGE_SEPARATOR = ",";
    private static final String RANGE_PART_SEPARATOR = "-";
    private List<Range> ranges;

    public HourRange(String range) {
        ranges = new ArrayList<>();
        if (range == null || range.isEmpty()) {
            return;
        }
        String[] parts = range.split(RANGE_SEPARATOR);
        for (String part : parts) {
            String[] rangeParts = part.split(RANGE_PART_SEPARATOR);
            if (rangeParts.length != 2) {
                throw new IllegalArgumentException("Invalid range: " + part);
            }
            int startHour = Integer.parseInt(rangeParts[0]);
            int endHour = Integer.parseInt(rangeParts[1]);
            ranges.add(new Range(startHour, endHour));
        }
    }

    public boolean contains(int hour) {
        for (Range range : ranges) {
            if (range.contains(hour)) {
                return true;
            }
        }
        return false;
    }

    public static HourRange parse(String range) {
        return new HourRange(range);
    }
}

class Range {
    private int startHour;
    private int endHour;

    public Range(int startHour, int endHour) {
        if (startHour > endHour | startHour < 0 || startHour > 23 || endHour < 0 || endHour > 23) {
            throw new IllegalArgumentException("Invalid hour range: " + startHour + " - " + endHour);
        }
        this.startHour = startHour;
        this.endHour = endHour;
    }

    public boolean contains(int hour) {
        return hour >= startHour && hour <= endHour;
    }
}
