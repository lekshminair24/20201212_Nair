package com.playstation.takehome.utility;

import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

@Slf4j
public class DataConversionUtility {
    private static final ZoneId UTC = ZoneId.of("UTC");

    public static Long convertDateTimeToEpochSeconds(String date) {
        Long epochSeconds = null;
        try {
            OffsetDateTime dateTime = OffsetDateTime.parse(date);
            epochSeconds = dateTime.atZoneSameInstant(UTC).toEpochSecond();
        } catch (DateTimeParseException dte) {
            log.warn(dte.getMessage());
        }
        return epochSeconds;
    }

    public static Long getStartOftheDayEpochSeconds(Date date) {
        LocalDateTime dateTime = date.toLocalDate().atStartOfDay();
        return dateTime.atZone(UTC).toEpochSecond();
    }

    public static Long getEndOftheDayEpochSeconds(Date date) {
        LocalDateTime dateTime = date.toLocalDate().atTime(LocalTime.MAX);
        return dateTime.atZone(UTC).toEpochSecond();
    }

}
