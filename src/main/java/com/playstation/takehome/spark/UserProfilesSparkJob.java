package com.playstation.takehome.spark;

import com.playstation.takehome.datamodel.UserProfile;
import com.playstation.takehome.datamodel.UserPlaceInteraction;
import com.playstation.takehome.utility.DataConversionUtility;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.io.Serializable;

import static com.playstation.takehome.utility.CommonUtility.OUTPUT_USERS_PROFILE;
import static com.playstation.takehome.utility.CommonUtility.OUTPUT_USER_PLACE_INTERACTIONS;
import static com.playstation.takehome.utility.CommonUtility.USER_ID_COL;
import static com.playstation.takehome.utility.CommonUtility.getFilePath;
import static com.playstation.takehome.utility.CommonUtility.getOutputFilePath;
import static com.playstation.takehome.utility.CommonUtility.getSparkSession;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

@Slf4j
public class UserProfilesSparkJob implements Serializable {
    private SparkSession sparkSession;
    private String userDetailsFilePath;

    public UserProfilesSparkJob() {
        this.sparkSession = getSparkSession();
        this.userDetailsFilePath = getFilePath("userDetails.json", UserProfilesSparkJob.class);
    }

    public UserProfilesSparkJob(String userDetailsFilePath) {
        this.sparkSession = getSparkSession();
        this.userDetailsFilePath = userDetailsFilePath;
    }

    public static void main(final String[] args) {
        new UserProfilesSparkJob().run();
    }

    public void run() {

        registerUdfs();
        Dataset<Row> userDetails = sparkSession.read().json(userDetailsFilePath);
        Dataset<UserPlaceInteraction> userPlaceInteractionsDataset = buildUserPlaceInteraction(userDetails);
        String outputInteractionsFilePath = getOutputFilePath(OUTPUT_USER_PLACE_INTERACTIONS);
        userPlaceInteractionsDataset
                .repartition(userPlaceInteractionsDataset.col(USER_ID_COL))
                .write().mode(SaveMode.Overwrite)
                .partitionBy(USER_ID_COL)
                .parquet(outputInteractionsFilePath);

        String usersProfileFilePath = getOutputFilePath(OUTPUT_USERS_PROFILE);
        Dataset<UserProfile> usersProfileDataset = buildUsersProfile(userDetails);
        usersProfileDataset
                .repartition(usersProfileDataset.col(USER_ID_COL))
                .write().mode(SaveMode.Overwrite)
                .partitionBy(USER_ID_COL)
                .parquet(usersProfileFilePath);
        sparkSession.read().parquet(usersProfileFilePath).show();
    }


    protected Dataset<UserPlaceInteraction> buildUserPlaceInteraction(Dataset<Row> userDetailsDataset) {
        Dataset<Row> userPlaceInteractionDataset = userDetailsDataset.select(col("userID"), explode(col("placeInteractionDetails")).as("placeInteraction_flat"));
        Dataset<Row> userPlaceInteractionFlattenedDataset = userPlaceInteractionDataset.select(col("userID"),
                col("placeInteraction_flat.foodRating").as("foodRating"),
                col("placeInteraction_flat.placeID").as("placeId"),
                col("placeInteraction_flat.restRating").as("restRating"),
                col("placeInteraction_flat.salesAmount").as("salesAmount"),
                col("placeInteraction_flat.serviceRating").as("serviceRating"),
                col("placeInteraction_flat.visitDate").as("visitDate")
        );
        Dataset<Row> userPlaceEpochDataset = userPlaceInteractionFlattenedDataset.withColumn("visitEpochSecond", callUDF("convertToEpochSeconds", col("visitDate")));
        return userPlaceEpochDataset.as(Encoders.bean(UserPlaceInteraction.class));
    }

    protected Dataset<UserProfile> buildUsersProfile(Dataset<Row> userDetailsDataset) {
        Dataset<Row> renamedUserDetailsDataset = userDetailsDataset
                .withColumnRenamed("birth_year", "birthYear")
                .withColumnRenamed("dress_preference", "dressPreference")
                .withColumnRenamed("drink_level", "drinkLevel")
                .withColumnRenamed("favCuisine", "favoriteCuisines")
                .withColumnRenamed("marital_status", "maritalStatus");
        return renamedUserDetailsDataset.as(Encoders.bean(UserProfile.class));
    }

    private void registerUdfs() {
        sparkSession.udf().register("convertToEpochSeconds", (UDF1<String, Long>) DataConversionUtility::convertDateTimeToEpochSeconds, DataTypes.LongType);
    }

}
