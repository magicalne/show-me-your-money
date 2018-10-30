package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class GridTradeConfig {
  private String symbol;
  private String qtyUnit;
  private String gridRate;
  private int gridSize;
  private double stopLoss;
}
