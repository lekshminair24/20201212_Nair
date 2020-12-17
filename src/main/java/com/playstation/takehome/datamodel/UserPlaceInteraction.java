package com.playstation.takehome.datamodel;
import lombok.Data;
import java.io.Serializable;

@Data
public class UserPlaceInteraction implements Serializable {
    private String userId;
    private Long placeId;
    private Integer foodRating;
    private Integer restRating;
    private Double salesAmount;
    private Integer serviceRating;
    private String visitDate;
    private Long visitEpochSecond;
}
