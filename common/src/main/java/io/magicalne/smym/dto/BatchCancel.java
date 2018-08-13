package io.magicalne.smym.dto;

import lombok.Data;

import java.util.List;

@Data
public class BatchCancel {

    private List<String> success;
    private List<BatchCancelBean> failed;

}
