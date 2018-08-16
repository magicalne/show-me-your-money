package io.magicalne.smym.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class TradeInfo {
    private BigDecimal price;
    private BigDecimal qty;
}
