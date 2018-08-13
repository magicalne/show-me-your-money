package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class SymbolsResponse {


    /**
     * status : ok
     * ch : market.btcusdt.detail
     * ts : 1489473538996
     * tick : {"amount":4316.4346,"open":8090.54,"close":7962.62,"high":8119,"ts":1489464451000,"id":1489464451,"count":9595,"low":7875,"vol":3.449727690576E7}
     */

    private String status;
    private String ch;
    private long ts;
    private String errCode;
    private String errMsg;
    private Symbols tick;

}
