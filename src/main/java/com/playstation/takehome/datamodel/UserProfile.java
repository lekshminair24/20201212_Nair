package com.playstation.takehome.datamodel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
public class UserProfile implements Serializable {
    private String activity;
    private String ambience;
    private String birthYear;
    private String budget;
    private String dressPreference;
    private String drinkLevel;
    private String [] favoriteCuisines;
    private String height;
    private String hijos;
    private String interest;
    private String latitude;
    private String longitude;
    private String maritalStatus;
    private String personality;
    private String religion;
    private String smoker;
    private String transport;
    private String userId;
    private String [] userPaymentMethods;
    private String weight;

}
