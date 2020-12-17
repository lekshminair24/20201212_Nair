package com.playstation.takehome.spark;

import com.playstation.takehome.stats.StatsType;
import lombok.Getter;
import lombok.Setter;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;


@Getter
@Setter
@Parameters(separators = "=")
@SuppressWarnings("checkstyle:magicnumber")
public class UserPlaceStatsArgsParser {
    private static final ZoneId UTC = ZoneId.of("UTC");

    @Parameter(names = "--help", order = 0, help = true, description = "Print a usage message and exit.")
    private boolean help;

    @Parameter(names = {"--stats-type", "-s"}, order = 1, description = "Stats to run", required = true)
    private String statTypesArg;

    @Parameter(names = {"--start-date", "-sd"}, order = 2, validateWith = ValidateDateArgument.class, description = "The start date parameter")
    private String startDate;

    @Parameter(names = {"--end-date", "-ed"}, order = 3, validateWith = ValidateDateArgument.class, description = "The end date parameter")
    private String endDate;

    @Parameter(names = {"--userId", "-u"}, order = 4, description = "The userId for which the stats need to be generated")
    private String userId;


    public static class ValidateDateArgument implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            try {
                    LocalDate.parse(value);

            } catch (DateTimeParseException ex) {
                throw new ParameterException(String.format("The given date is not in a parsable format: %s", value));
            }
        }
    }

    public Date getStartDate(){
         return Date.valueOf(startDate);
    }

    public Date getEndDate(){
        return Date.valueOf(endDate);
    }

    public StatsType getStatTypes() {
        StatsType statType = StatsType.getStatType(statTypesArg);
        if (null == statType) {
            throw new ParameterException(String.format("Invalid command argument '%s' for stat types!", statTypesArg));
        }

        return statType;
    }

}