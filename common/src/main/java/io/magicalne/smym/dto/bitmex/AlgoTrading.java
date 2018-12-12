package io.magicalne.smym.dto.bitmex;

import lombok.Data;

@Data
public class AlgoTrading {
  private String make;
  private String hedge;
  private int contracts;
  private double leverage;
  private double imbalance;
}
