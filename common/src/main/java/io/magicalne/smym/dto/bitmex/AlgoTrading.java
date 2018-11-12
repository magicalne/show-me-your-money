package io.magicalne.smym.dto.bitmex;

import lombok.Data;

@Data
public class AlgoTrading {
  private String symbol;
  private int shortAmount;
  private int longAmount;
  private String pmmlPath;
  private String target;
}
