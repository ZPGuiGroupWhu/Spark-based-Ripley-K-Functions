package org.whu.geoai_stval.spark_K_functions.space_K.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;

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

    /**
     * Calculates temporal distance between two local datetime
     * @param dateTime1 one datetime
     * @param dateTime2 another datetime
     * @param temporalUnit temporal unit
     * @return distance at the input unit
     */
    public static long calTemporalDistance(LocalDateTime dateTime1, LocalDateTime dateTime2, TemporalUnit temporalUnit) {
        if(temporalUnit instanceof ChronoUnit) {
            ChronoUnit chronoUnit = (ChronoUnit) temporalUnit;
            switch (chronoUnit) {
                case YEARS: {
                    return Math.abs(ChronoUnit.YEARS.between(dateTime1.toLocalDate(), dateTime2.toLocalDate()));
                }
                case MONTHS: {
                    return Math.abs(ChronoUnit.MONTHS.between(dateTime1.toLocalDate(), dateTime2.toLocalDate()));
                }
                case DAYS: {
                    return Math.abs(ChronoUnit.DAYS.between(dateTime1.toLocalDate(), dateTime2.toLocalDate()));
                }
                case HOURS: {
                    return Duration.between(dateTime1, dateTime2).abs().toHours();
                }
                case MINUTES: {
                    return Duration.between(dateTime1, dateTime2).abs().toMinutes();
                }
                case SECONDS: {
                    return Duration.between(dateTime1, dateTime2).abs().getSeconds();
                }
            }
            throw new UnsupportedTemporalTypeException("Unsupported unit: " + temporalUnit);
        }
        return temporalUnit.between(dateTime1, dateTime2);
    }
}
