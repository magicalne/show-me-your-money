package io.magicalne.smym.dto;


import io.magicalne.smym.exception.ApiException;
import lombok.Data;

@Data
public class ApiResponse<T> {

    public String status;
    public String errCode;
    public String errMsg;
    public T data;

    public T checkAndReturn() {
        if ("ok".equals(status)) {
            return data;
        }
        throw new ApiException(errCode, errMsg);
    }
}
