package io.magicalne.smym.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class OrderPlaceRequest {

    @JsonProperty("account-id")
    private String accountId;
    private String amount;
    private String price;
    private String source;
    private String symbol;
    private String type;
}
