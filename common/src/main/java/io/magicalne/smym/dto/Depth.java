package io.magicalne.smym.dto;

import lombok.Data;

import java.util.List;

@Data
public class Depth {

    /**
     * id : 1489464585407
     * ts : 1489464585407
     * bids : [[7964,0.0678],[7963,0.9162]]
     * asks : [[7979,0.0736],[8020,13.6584]]
     */

    private String id;
    private String ts;
    private long version;
    private List<List<Double>> bids;
    private List<List<Double>> asks;
}
