package io.magicalne.smym.dto;

public class CreateOrderRequest {

    /**
     * 交易对，必填，例如："ethcny"，
     */
    public String symbol;

    /**
     * 账户ID，必填，例如："12345"
     */
    public String accountId;

    /**
     * 当订单类型为buy-limit,sell-limit时，表示订单数量， 当订单类型为buy-market时，表示订单总金额， 当订单类型为sell-market时，表示订单总数量
     */
    public String amount;

    /**
     * 订单价格，仅针对限价单有效，例如："1234.56"
     */
    public String price = "0.0";

    /**
     * 订单类型，取值范围"buy-market,sell-market,buy-limit,sell-limit"
     */
    public String type;

    /**
     * 订单来源，例如："api"
     */
    public String source = "com/huobi/api";
}
