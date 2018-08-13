package io.magicalne.smym.dto;

public class DepthRequest {

    //交易对
    public String symbol;

    //Depth 类型 step0, step1, step2, step3, step4, step5（合并深度0-5）；step0时，不合并深度
    public String type;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}