package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class SubKlineRes {
    private String id;
    private String ch;
    private long ts;
    private Tick tick;
}
