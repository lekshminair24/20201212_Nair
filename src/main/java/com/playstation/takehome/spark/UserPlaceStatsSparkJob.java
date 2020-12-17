package com.playstation.takehome.spark;

import com.beust.jcommander.JCommander;
import com.playstation.takehome.Exception.UserPlaceException;
import com.playstation.takehome.datamodel.PlaceProfile;
import com.playstation.takehome.stats.PlaceStats;
import com.playstation.takehome.datamodel.UserProfile;
import com.playstation.takehome.datamodel.UserPlaceInteraction;
import com.playstation.takehome.stats.StatsType;
import com.playstation.takehome.stats.UserStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.sql.Date;

import static com.playstation.takehome.utility.CommonUtility.OUTPUT_PLACES_PROFILE;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_TOP3_BY_CUISINE;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_TOPN_BY_CUISINE;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_USERS_AVG_VISIT;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_USERS_PLACE_RECO;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_USERS_PLACE_YET_TO_VIST;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_USERS_PROFILE;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_USER_PLACE_INTERACTIONS;
import static com.playstation.takehome.utility.CommonUtility.PLACE_ID_COL;
import static com.playstation.takehome.utility.CommonUtility.USER_ID_COL;
import static com.playstation.takehome.utility.CommonUtility.VISITED_DATE_COL;
import static com.playstation.takehome.utility.CommonUtility.getOutputFilePath;
import static com.playstation.takehome.utility.CommonUtility.getSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;


@Slf4j
public class UserPlaceStatsSparkJob implements Serializable {
    private SparkSession sparkSession;
    private StatsType statsType;
    private Date startDate;
    private Date endDate;
    private String userId;

    public UserPlaceStatsSparkJob(UserPlaceStatsArgsParser jobArguments) {
        sparkSession = getSparkSession();
        initializeJobParameters(jobArguments);
    }

    public UserPlaceStatsSparkJob(SparkSession sparkSession, UserPlaceStatsArgsParser jobArguments) {
        this.sparkSession = sparkSession;
        initializeJobParameters(jobArguments);
    }

    public static void main(final String[] args) {
        final UserPlaceStatsArgsParser jobArguments = new UserPlaceStatsArgsParser();
        final JCommander jCommander = new JCommander(jobArguments, null, args);
        JCommander.newBuilder().addObject(jobArguments).build().parse(args);
        if (jobArguments.isHelp()) {
            jCommander.usage();
            System.exit(0);
        }
        new UserPlaceStatsSparkJob(jobArguments).run();
    }


    public void run() throws UserPlaceException {

        String outputInteractionsFilePath = getOutputFilePath(OUTPUT_USER_PLACE_INTERACTIONS);
        String usersProfileFilePath = getOutputFilePath(OUTPUT_USERS_PROFILE);
        String placesProfileFilePath = getOutputFilePath(OUTPUT_PLACES_PROFILE);

        Dataset<UserPlaceInteraction> userPlaceInteractionDataset = sparkSession.read().parquet(outputInteractionsFilePath).as(Encoders.bean(UserPlaceInteraction.class));
        Dataset<PlaceProfile> placesProfileDataset = sparkSession.read().parquet(placesProfileFilePath).as(Encoders.bean(PlaceProfile.class));
        Dataset<UserProfile> usersProfileDataset = sparkSession.read().parquet(usersProfileFilePath).as(Encoders.bean(UserProfile.class));

        Dataset<Row> placeDetailsWithCuisinesDataset = getPlaceWithServedCuisines(placesProfileDataset);
        switch (statsType) {
            case TOP3PLACEBYCUISINE:
                Dataset<Row> topThreeRestaurantsByCuisine = new PlaceStats(sparkSession).getTopNRestaurantsByCuisine(placeDetailsWithCuisinesDataset, userPlaceInteractionDataset, startDate, endDate, 3);
                topThreeRestaurantsByCuisine.show();
                topThreeRestaurantsByCuisine.repartition(1).write().mode(SaveMode.Overwrite).json(OUTPUT_TOP3_BY_CUISINE);
                break;
            case TOPNPLACEBYCUISINE:
                Dataset<Row> topNRestaurantsByCuisine = new PlaceStats(sparkSession).getTopNRestaurantsByCuisine(placeDetailsWithCuisinesDataset, userPlaceInteractionDataset, startDate, endDate, null);
                topNRestaurantsByCuisine.repartition(1).write().mode(SaveMode.Overwrite).json(OUTPUT_TOPN_BY_CUISINE);
                break;
            case AVGUSERVISIT:
                Dataset<Row> avgTimeBtwnvisits = new UserStats(sparkSession).getAverageHoursConsecutiveVisits(userPlaceInteractionDataset, startDate);
                avgTimeBtwnvisits.repartition(avgTimeBtwnvisits.col(VISITED_DATE_COL)).write().partitionBy(VISITED_DATE_COL).mode(SaveMode.Overwrite).json(OUTPUT_USERS_AVG_VISIT);
                break;
            case PLACERECOMMENDED:
                UserStats userStats = new UserStats(sparkSession);
                Dataset<Row> topNRecommendedPlace = userStats.getTopNRecommendedRestaurants(userPlaceInteractionDataset, usersProfileDataset, placeDetailsWithCuisinesDataset, userId);
                topNRecommendedPlace.repartition(topNRecommendedPlace.col(USER_ID_COL)).write().partitionBy(USER_ID_COL).mode(SaveMode.Overwrite).json(OUTPUT_USERS_PLACE_RECO);
                break;
            case PLACENOTVISITED:
                userStats = new UserStats(sparkSession);
                Dataset<Row> topNRecommendedPlaces = sparkSession.read().json().where(String.format("userId = '%s' ", userId));
                if (topNRecommendedPlaces.isEmpty()) {
                    topNRecommendedPlaces = userStats.getTopNRecommendedRestaurants(userPlaceInteractionDataset, usersProfileDataset, placeDetailsWithCuisinesDataset, userId);
                }
                Dataset<Row> recommendedPlaces = userStats.recommendNotVisitedPlace(topNRecommendedPlaces, userPlaceInteractionDataset, userId);
                recommendedPlaces.repartition(recommendedPlaces.col(USER_ID_COL)).write().partitionBy(USER_ID_COL).mode(SaveMode.Overwrite).json(OUTPUT_USERS_PLACE_YET_TO_VIST);
                break;
        }


    }

    private Dataset<Row> getPlaceWithServedCuisines(Dataset<PlaceProfile> placeDimensionsDataset) {
        return placeDimensionsDataset.select(col(PLACE_ID_COL), explode(col("servedCuisines")).as("cuisines"));
    }

    private void initializeJobParameters(UserPlaceStatsArgsParser jobArguments) {
        this.statsType = jobArguments.getStatTypes();
        this.startDate = jobArguments.getStartDate();
        this.endDate = jobArguments.getEndDate();
        this.userId = jobArguments.getUserId();
    }


}
