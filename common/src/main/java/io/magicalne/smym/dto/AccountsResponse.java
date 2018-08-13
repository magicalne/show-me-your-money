package io.magicalne.smym.dto;


import io.magicalne.smym.exception.ApiException;
import lombok.Data;

import java.util.List;

@Data
public class AccountsResponse {

    /**
     * status : ok
     * data : [{"id":100009,"type":"spot","state":"working","user-id":1000}]
     */

    private String status;
    public String errCode;
    public String errMsg;
    private List<Accounts> data;

    public List<Accounts> checkAndReturn() {
        if ("ok".equals(status)) {
            return data;
        }
        throw new ApiException(errCode, errMsg);
    }
}
