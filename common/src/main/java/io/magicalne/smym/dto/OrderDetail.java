package io.magicalne.smym.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class OrderDetail {

    /**
     * id : 59378
     * symbol : ethusdt
     * account-id : 100009
     * amount : 10.1000000000
     * price : 100.1000000000
     * created-at : 1494901162595
     * type : buy-limit
     * field-amount : 10.1000000000
     * field-cash-amount : 1011.0100000000
     * field-fees : 0.0202000000
     * finished-at : 1494901400468
     * user-id : 1000
     * source : api
     * state : filled
     * canceled-at : 0
     * exchange : huobi
     * batch :
     */

    private long id;
    private String symbol;
    @JsonProperty("account-id")
    private long accountId;
    private String amount;
    private String price;
    @JsonProperty("created-at")
    private long createdAt;
    private String type;
    @JsonProperty("field-amount")
    private String fieldAmount; //amount of base
    @JsonProperty("field-cash-amount")
    private String fieldCashAmount; //amount of quote
    @JsonProperty("field-fees")
    private String fieldFees;
    @JsonProperty("finished-at")
    private long finishedAt;
    private String source;
    private String state;
    @JsonProperty("canceled-at")
    private long canceledAt;
    private String exchange;
    private String batch;

}
