package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class BatchCancelBean {
	/** err-msg : 记录无效 order-id : 2 err-code : base-record-invalid */
	private String errMsg;
	private String orderId;
	private String errCode;

}