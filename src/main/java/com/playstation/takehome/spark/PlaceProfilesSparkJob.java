package com.playstation.takehome.spark;

import com.playstation.takehome.datamodel.PlaceProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

import static com.playstation.takehome.utility.CommonUtility.OUTPUT_PLACES_PROFILE;
import static com.playstation.takehome.utility.CommonUtility.getFilePath;
import static com.playstation.takehome.utility.CommonUtility.getOutputFilePath;
import static com.playstation.takehome.utility.CommonUtility.getSparkSession;

@Slf4j
public class PlaceProfilesSparkJob implements Serializable {
    public static final String PLACE_ID = "placeId";
    private SparkSession sparkSession;
    private String placeDetailsFilePath;

    public PlaceProfilesSparkJob() {
        this.sparkSession = getSparkSession();
        this.placeDetailsFilePath = getFilePath("placeDetails.json", PlaceProfilesSparkJob.class);
    }

    public PlaceProfilesSparkJob(String placeDetailsFilePath) {
        this.sparkSession = getSparkSession();
        this.placeDetailsFilePath = placeDetailsFilePath;
    }

    public static void main(final String[] args) {
        new PlaceProfilesSparkJob().run();
    }

    public void run() {
        Dataset<Row> placeDetailsDataset = sparkSession.read().json(placeDetailsFilePath);
        Dataset<PlaceProfile> placesProfileDataset = buildPlaceDimensions(placeDetailsDataset);

        String placesProfileFilePath = getOutputFilePath(OUTPUT_PLACES_PROFILE);
        placesProfileDataset
                .repartition(placesProfileDataset.col(PLACE_ID))
                .write().mode(SaveMode.Overwrite)
                .partitionBy(PLACE_ID)
                .parquet(placesProfileFilePath);

    }

    protected Dataset<PlaceProfile> buildPlaceDimensions(Dataset<Row> placeDetailsDataset) {
        return placeDetailsDataset.as(Encoders.bean(PlaceProfile.class));
    }

}
