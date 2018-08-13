package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class BalanceResponse {
    /**
     * status : ok
     * data : {"id":"100009","type":"spot","state":"working","list":[{"currency":"usdt","type":"trade","balance":"500009195917.4362872650"}],"user-id":"1000"}
     */
    private String status;
    public String errCode;
    public String errMsg;
    private Balance data;

}
