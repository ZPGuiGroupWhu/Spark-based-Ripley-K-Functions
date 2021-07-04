package org.wysrc.spatiotemporalK.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * utils for time tansformation
 */
public class TimeUtils {
    /**
     * Gets localDateTime from UNIX timestamp with default zoneId
     *
     * @param timestamp the UNIX timestamp
     * @return localDateTime
     */
    public static LocalDateTime getDateTimeFromTimestamp(long timestamp) {
        return getDateTimeFromTimestamp(timestamp, ZoneId.systemDefault());
    }

    /**
     * Gets localDateTime from UNIX timestamp
     *
     * @param timestamp the UNIX timestamp
     * @param zoneId the zone ID
     * @return localDateTime at specific zone
     */
    public static LocalDateTime getDateTimeFromTimestamp(long timestamp, ZoneId zoneId) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return LocalDateTime.ofInstant(instant, zoneId);
    }

    /**
     * Gets UNIX timestamp from localDatetime with default zoneId
     *
     * @param localDateTime the localDateTime
     * @return the UNIX timestamp
     */
    public static long getTimestampFromDateTime(LocalDateTime localDateTime) {
        return getTimestampFromDateTime(localDateTime, ZoneId.systemDefault());
    }

    /**
     * Gets UNIX timestamp from localDatetime
     *
     * @param localDateTime the localDateTime
     * @param zoneId the zone ID
     * @return the UNIX timestamp at specific zone
     */
    public static long getTimestampFromDateTime(LocalDateTime localDateTime, ZoneId zoneId) {
        Instant instant = localDateTime.atZone(zoneId).toInstant();
        return instant.toEpochMilli();
    }
}
