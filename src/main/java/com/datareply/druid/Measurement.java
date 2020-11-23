package com.datareply.druid;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class Measurement {
    private long timestamp;
    private double value;
    private int plugId;
    private int roomId;
    private int houseId;


}