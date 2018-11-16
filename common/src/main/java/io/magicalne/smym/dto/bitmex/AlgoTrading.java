package io.magicalne.smym.dto.bitmex;

import lombok.Data;

@Data
public class AlgoTrading {
  private String symbol;
  private int contracts;
  private String pmmlPath;
  private String target;
}
