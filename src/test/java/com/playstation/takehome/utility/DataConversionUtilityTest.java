package com.playstation.takehome.utility;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DataConversionUtilityTest {

    @Test
    public void testDateConversion() {
        Long expectedEpochSeconds = 1589087680l;
        assertEquals(expectedEpochSeconds, DataConversionUtility.convertDateTimeToEpochSeconds("2020-05-09T22:14:40.000-07:00"));
    }

    @Test
    public void testDateConversionInvalidFormat() {
        assertNull(DataConversionUtility.convertDateTimeToEpochSeconds("2020-05-09T22:14:40.000"));
    }

    @Test
    public void testStartOfTheDay() {
        assertEquals(Long.valueOf(1588982400),DataConversionUtility.getStartOftheDayEpochSeconds(Date.valueOf("2020-05-09")));
    }

    @Test
    public void testEndOfTheDay() {
       assertEquals(Long.valueOf(1589068799), DataConversionUtility.getEndOftheDayEpochSeconds(Date.valueOf("2020-05-09")));
    }

}


