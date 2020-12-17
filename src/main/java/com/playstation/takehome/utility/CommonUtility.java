package com.playstation.takehome.utility;

import org.apache.spark.sql.SparkSession;

import java.io.File;

public class CommonUtility {
    public static final String OUTPUT_USER_PLACE_INTERACTIONS = "output/userPlaceInteractions";
    public static final String OUTPUT_USERS_PROFILE = "output/usersProfile";
    public static final String OUTPUT_PLACES_PROFILE = "output/placesProfile";
    public static final String OUTPUT_TOP3_BY_CUISINE = "output/places/topthree";
    public static final String OUTPUT_TOPN_BY_CUISINE = "output/places/topN";
    public static final String OUTPUT_USERS_AVG_VISIT = "output/users/avgVisit";
    public static final String OUTPUT_USERS_PLACE_RECO = "output/users/placeRecommended";
    public static final String OUTPUT_USERS_PLACE_YET_TO_VIST = "output/users/placeNotVisited";

    public static final String USER_ID_COL = "userId";
    public static final String PLACE_ID_COL = "placeId";
    public static final String VISITED_DATE_COL = "visitedDate";

    public static String getFilePath(String fileName, Class clazz) {
        return clazz.getClassLoader()
                .getResource(fileName).toString()
                .replace("file:/", "file:///");
    }

    public static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("foo")
                .config("spark.master", "local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
    }
    public static String getOutputFilePath( String directoryName) {
        return "file:///" + new File(directoryName).getAbsolutePath();
    }
}
