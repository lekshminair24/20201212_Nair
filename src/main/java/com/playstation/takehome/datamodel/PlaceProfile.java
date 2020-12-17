package com.playstation.takehome.datamodel;

import lombok.Data;

import java.io.Serializable;

@Data
public class PlaceProfile implements Serializable {
    private String [] acceptedPayments;
    private OpenHours [] openHours;
    private String [] parkingType;
    private Integer placeId;
    private String [] servedCuisines;

}
@Data
 class OpenHours  implements Serializable{
    private String days;
    private String hours;
}
