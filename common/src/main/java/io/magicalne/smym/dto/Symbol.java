package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class Symbol {
    private String baseCurrency;
    private String quoteCurrency;
    private String symbol;
    private int pricePrecision;
    private int amountPrecision;
    private String symbolPartition;
}
