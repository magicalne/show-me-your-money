package io.magicalne.smym.dto;

import lombok.Data;

import java.util.List;

@Data
public class MarketMakingConfig {
  private String version;
  private List<GridTradeConfig> grids;
}
