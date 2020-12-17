package com.playstation.takehome.stats;

import com.playstation.takehome.datamodel.UserPlaceInteraction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Date;

import static com.playstation.takehome.utility.DataConversionUtility.convertDateTimeToEpochSeconds;

@Slf4j
public class PlaceStats {
    public static final String PLACE_VIEW = "placeView";
    public static final String USER_VIEW = "userView";
    public static final String JOINED_VIEW = "joinedView";
    public static final String TOTAL_SALES_VIEW = "totalSalesView";
    private SparkSession sparkSession;

    public PlaceStats(SparkSession sparkSession){
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> getTopNRestaurantsByCuisine (Dataset<Row> placeDetailsWithCuisinesDataset, Dataset<UserPlaceInteraction> userPlaceFactsDataset, Date startDate, Date endDate, Integer numberOfRestaurants){

        String joinAmountWithCuisineQuery = "SELECT u.placeId,u.salesAmount,p.cuisines,u.visitEpochSecond FROM %s u INNER JOIN %s p ON p.placeId=u.placeId";
        String totalSalesQuery = "SELECT placeId,cuisines,SUM(salesAmount) AS totalSales FROM  %s %s GROUP BY placeId,cuisines ";
        String topRestaurantQuery =  "WITH TOPN AS  " +
                "(SELECT *, ROW_NUMBER() OVER (PARTITION BY cuisines ORDER BY totalSales desc ) AS RowNo  FROM %s) " +
                "SELECT placeId,cuisines,totalSales FROM TOPN WHERE RowNo <= %d order by cuisines ";

        placeDetailsWithCuisinesDataset.createOrReplaceTempView(PLACE_VIEW);
        userPlaceFactsDataset.createOrReplaceTempView(USER_VIEW);

        Dataset<Row> joinedDataset = sparkSession.sql(String.format(joinAmountWithCuisineQuery, USER_VIEW, PLACE_VIEW));
        joinedDataset.createOrReplaceTempView(JOINED_VIEW);

        String whereClause = "WHERE visitEpochSecond BETWEEN '%s' AND '%s'";
        String formattedWhereClause = StringUtils.EMPTY;
        if(null!= startDate && null != endDate){
            formattedWhereClause = String.format(whereClause,convertDateTimeToEpochSeconds(String.valueOf(startDate)),convertDateTimeToEpochSeconds(String.valueOf(endDate)));
        }
        String formattedTotalSalesQuery = String.format(totalSalesQuery,JOINED_VIEW,formattedWhereClause);
        log.info("formattedTotalSalesQuery {} ",formattedTotalSalesQuery);
        sparkSession.sql(formattedTotalSalesQuery).createOrReplaceTempView(TOTAL_SALES_VIEW);;
        return sparkSession.sql(String.format(topRestaurantQuery,TOTAL_SALES_VIEW,(numberOfRestaurants!=null) ? numberOfRestaurants:3));

    }
}
