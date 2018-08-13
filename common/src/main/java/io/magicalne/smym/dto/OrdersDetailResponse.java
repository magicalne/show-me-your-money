package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class OrdersDetailResponse {

    /**
     * status : ok
     * data : {"id":59378,"symbol":"ethusdt","account-id":100009,"amount":"10.1000000000","price":"100.1000000000","created-at":1494901162595,"type":"buy-limit","field-amount":"10.1000000000","field-cash-amount":"1011.0100000000","field-fees":"0.0202000000","finished-at":1494901400468,"user-id":1000,"source":"api","state":"filled","canceled-at":0,"exchange":"huobi","batch":""}
     */

    private String status;
    public String errCode;
    public String errMsg;
    private OrderDetail data;

    public boolean checkStatusOK() {
        return "ok".equals(this.status);
    }
}
