package com.playstation.takehome.stats;

import com.playstation.takehome.datamodel.UserPlaceInteraction;
import com.playstation.takehome.datamodel.UserProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.sql.Date;

import static com.playstation.takehome.utility.CommonUtility.PLACE_ID_COL;
import static com.playstation.takehome.utility.CommonUtility.USER_ID_COL;
import static com.playstation.takehome.utility.CommonUtility.VISITED_DATE_COL;
import static com.playstation.takehome.utility.DataConversionUtility.getEndOftheDayEpochSeconds;
import static com.playstation.takehome.utility.DataConversionUtility.getStartOftheDayEpochSeconds;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lag;

@Slf4j
public class UserStats {
    public static final String USER_VIEW = "userView";
    public static final String TIME_BETWEEN_VISITS_VIEW = "timeBetweenVisitsView";
    public static final String JOINED_REST_RATING_VIEW = "joinedRestRatingView";
    public static final String VISIT_EPOCH_SECOND_COL = "visitEpochSecond";
    public static final String PLACE_VIEW = "placeView";
    public static final String USER_PLACE_FACTS_VIEW = "userPlaceFactsView";
    public static final String USER_VISITED_VIEW = "userVisitedView";
    public static final String RECOMMENDED_PLACES_VIEW = "recommendedPlacesView";

    private SparkSession sparkSession;


    public UserStats(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> getAverageHoursConsecutiveVisits(Dataset<UserPlaceInteraction> userPlaceFactsDataset, Date visitDate) {
        userPlaceFactsDataset.createOrReplaceTempView(USER_VIEW);
        String filterByDateQuery = "SELECT userID,placeId,visitEpochSecond FROM %s WHERE visitEpochSecond BETWEEN %s AND %s ";
        String averageQuery = "SELECT userId, COALESCE(AVG(timeBetweenTwoVisits)/3600,-9994) as avgTimeBtwnVisitsInHours FROM %s GROUP BY userId ORDER BY userId ";

        Long startTime = getStartOftheDayEpochSeconds(visitDate);
        Long endTime = getEndOftheDayEpochSeconds(visitDate);

        Dataset<Row> userVisitFilteredByDate = sparkSession.sql(String.format(filterByDateQuery, USER_VIEW, startTime, endTime));
        WindowSpec windowSpec = Window.partitionBy("userID").orderBy(VISIT_EPOCH_SECOND_COL);
        Dataset<Row> userDatasetWithTimeBtwnTwoVisits = userVisitFilteredByDate
                .withColumn("timeBetweenTwoVisits", (userVisitFilteredByDate.col(VISIT_EPOCH_SECOND_COL).minus(lag(userVisitFilteredByDate.col(VISIT_EPOCH_SECOND_COL), 1).over(windowSpec))));
        userDatasetWithTimeBtwnTwoVisits.createOrReplaceTempView(TIME_BETWEEN_VISITS_VIEW);

        return sparkSession.sql(String.format(averageQuery, TIME_BETWEEN_VISITS_VIEW)).withColumn(VISITED_DATE_COL, lit(visitDate));

    }

    public Dataset<Row> getTopNRecommendedRestaurants(Dataset<UserPlaceInteraction> userPlaceFactsDataset, Dataset<UserProfile> userDimensions, Dataset<Row> placeDetailsWithCuisinesDataset, String userId) {
        Dataset<Row> userWithFavoriteCuisinesDataset = userDimensions.select(explode(col("favoriteCuisines")).as("favoriteCuisine")).where(userDimensions.col(USER_ID_COL).$eq$eq$eq(userId));
        String favCuisine = StringUtils.EMPTY;
        if (!userWithFavoriteCuisinesDataset.isEmpty()) {
            favCuisine = userWithFavoriteCuisinesDataset.as(Encoders.STRING()).collectAsList().get(0);
        }
        log.info("favCuisine  {} for user  {}", favCuisine, userId);
        placeDetailsWithCuisinesDataset.createOrReplaceTempView(PLACE_VIEW);
        userPlaceFactsDataset.createOrReplaceTempView(USER_PLACE_FACTS_VIEW);

        String joinQuery = "SELECT DISTINCT u.placeId,u.restRating,p.cuisines FROM %s u INNER JOIN %s p ON p.placeId=u.placeId ";
        Dataset<Row> joinedDataset = sparkSession.sql(String.format(joinQuery, USER_PLACE_FACTS_VIEW, PLACE_VIEW));

        joinedDataset.createOrReplaceTempView(JOINED_REST_RATING_VIEW);
        String topNQuery = "SELECT placeId,cuisines,restRating FROM %s WHERE cuisines IN ('%s') ORDER BY restRating DESC ";
        String topNQueryAllCuisines = "SELECT placeId,cuisines,restRating FROM (SELECT placeId ,cuisines,restRating, ROW_NUMBER() OVER (partition BY cuisines ORDER BY restRating DESC ) AS rowNum FROM %s ) x ";
        Dataset<Row> topNRestaurants = null;
        if (StringUtils.isNotEmpty(favCuisine)) {
            topNRestaurants = sparkSession.sql(String.format(topNQuery, JOINED_REST_RATING_VIEW, favCuisine));
        }
        if (topNRestaurants.isEmpty() || StringUtils.isEmpty(favCuisine)) {
            topNRestaurants = sparkSession.sql(String.format(topNQueryAllCuisines, JOINED_REST_RATING_VIEW));
        }

        return topNRestaurants.withColumn(USER_ID_COL, lit(userId));

    }


    public Dataset<Row> recommendNotVisitedPlace(Dataset<Row> recommendedPlacesDataset, Dataset<UserPlaceInteraction> userPlaceFactsDataset, String userId) {
        userPlaceFactsDataset.select(PLACE_ID_COL)
                .where(userPlaceFactsDataset.col(USER_ID_COL).$eq$eq$eq(userId))
                .dropDuplicates()
                .createOrReplaceTempView(USER_VISITED_VIEW);
        recommendedPlacesDataset.createOrReplaceTempView(RECOMMENDED_PLACES_VIEW);
        String placesNotVisitedQuery = "SELECT r.* FROM %s r LEFT ANTI JOIN %s u ON r.placeId= u.placeId ";
        return sparkSession.sql(String.format(placesNotVisitedQuery, RECOMMENDED_PLACES_VIEW, USER_VISITED_VIEW));
    }
}
