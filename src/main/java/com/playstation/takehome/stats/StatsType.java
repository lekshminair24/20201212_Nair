package com.playstation.takehome.stats;

public enum StatsType {
    TOP3PLACEBYCUISINE("top3PlaceByCuisine"),
    TOPNPLACEBYCUISINE("topNPlaceByCuisine"),
    PLACERECOMMENDED("placeRecommended"),
    PLACENOTVISITED("placeNotVisited"),
    AVGUSERVISIT("averageUserVisit");

    private String statTypeParameter;
    StatsType(String statTypeParameter){
        this.statTypeParameter = statTypeParameter;
    }

    public static StatsType getStatType(String statTypeParameter){
        StatsType statsType = null;
        for (StatsType type: StatsType.values()){
            if(type.getStatTypeParameter().toLowerCase().equals(statTypeParameter.toLowerCase())){
                statsType = type;
            }
        }
      return statsType;
    }


    public String getStatTypeParameter() {
        return statTypeParameter;
    }


}
