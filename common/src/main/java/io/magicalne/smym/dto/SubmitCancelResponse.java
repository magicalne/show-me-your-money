package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class SubmitCancelResponse {


    /**
     * status : ok
     * data : 59378
     */

    private String status;
    public String errCode;
    public String errMsg;
    private String data;

    public boolean checkStatusOK() {
        return "ok".equals(this.status);
    }
}
