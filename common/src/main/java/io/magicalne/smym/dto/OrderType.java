package io.magicalne.smym.dto;

public enum OrderType {
    BUY_LIMIT("buy-limit"),
    SELL_LIMIT("sell-limit"),
    BUY_MARKET("buy-market"),
    SELL_MARKET("sell-market");

    private String type;
    OrderType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
