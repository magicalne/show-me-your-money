package io.magicalne.smym.dto.bitmex;

import lombok.Data;

@Data
public class AlgoTrading {
  private String symbol;
  private int contracts;
  private double leverage;
  private double imbalance;
}
