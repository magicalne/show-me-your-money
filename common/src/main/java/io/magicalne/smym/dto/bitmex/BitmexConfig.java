package io.magicalne.smym.dto.bitmex;

import lombok.Data;

import java.util.List;

@Data
public class BitmexConfig {
  private String name;
  private String version;
  private String deltaHost;
  private int deltaPort;
  private List<AlgoTrading> algoTradings;
}
