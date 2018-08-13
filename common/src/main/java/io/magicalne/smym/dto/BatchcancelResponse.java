package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class BatchcancelResponse<T> {

    /**
     * status : ok
     * data : {"success":["1","3"],"failed":[{"err-msg":"记录无效","order-id":"2","err-code":"base-record-invalid"}]}
     */

    private String status;
    public String errCode;
    public String errMsg;
    private BatchCancel data;

}
