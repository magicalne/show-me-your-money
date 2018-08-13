package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class Tick {
    private long id;
    private double open;
    private double close;
    private double low;
    private double high;
    private double amount;
    private double vol;
    private int count;
}
