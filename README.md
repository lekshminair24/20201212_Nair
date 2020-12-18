# 20201212_Nair
 This project process two different Json files userDetails.json  & placeDetails.json.   Files are processed to generate various stats of users and place.
 
 There are three spark Jobs:
  1. UserProfilesSparkJob   - Responsible for reading userDetails.json and  creating two parquet files. These two parquets are used as an input to generate stats by UserPlaceStatsSparkJob.
  2. PlaceProfilesSparkJob  - Responsible for reading placeDetails.json and creates a parquet file .The parquet is an input to generate stats by UserPlaceStatsSparkJob.
  3. UserPlaceStatsSparkJob - Generate the following stats for the given inputs.
  
        3.1 Top three restaurants by sales amount per cuisine for a given period.
        
            You can run using a spark-submit or as a standalone java class. The main class is 
              com.playstation.takehome.spark.UserPlaceStatsSparkJob.Parameters are :
              --stats-type=top3PlaceByCuisine  --start-date=2020-05-10 --end-date=2020-05-11 
              This will generate an output in json format.  
      
        3.2 Top N restaurants by sales amount per cuisine for a given period.
        
               You can run using a spark-submit or as a standalone java class. The main class is 
               com.playstation.takehome.spark.UserPlaceStatsSparkJob.Parameters are :
               --stats-type=topNPlaceByCuisine  --start-date=2020-05-10 --end-date=2020-05-11 --number-of-restaurants=4 
               This will generate an output in json format. 
        
        3.3 Average time in hours between two consecutive visits to any restaurant by an user.
        
               You can run using a spark-submit or as a standalone java class. The main class is 
               com.playstation.takehome.spark.UserPlaceStatsSparkJob.Parameters are :
               --stats-type=averageUserVisit --start-date=2020-05-10
               This will generate an output in json format. 
        
        3.4 Top N recommended restaurants by restRating.
        
                You can run using a spark-submit or as a standalone java class. The main class is 
                com.playstation.takehome.spark.UserPlaceStatsSparkJob.Parameters are :
                --stats-type=placeRecommended --user-id=U1066
                This will generate an output in json format. 
        
        3.5 Top N non-visited restaurants by a user based on restRating.
        
                You can run using a spark-submit or as a standalone java class. The main class is 
                com.playstation.takehome.spark.UserPlaceStatsSparkJob.Parameters are :
                --stats-type=placeNotVisited --user-id=U1066
                his will generate an output in json format. 
        
