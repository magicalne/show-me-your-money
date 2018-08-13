package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class OrderPlaceResponse {
    private String status;
    private String data; //order id

    public boolean checkStatusOK() {
        return "ok".equals(this.status);
    }
}
