package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class Triangular {
    private String source;
    private String middle;
    private String last;

    public Triangular(String source, String middle, String last) {
        this.source = source;
        this.middle = middle;
        this.last = last;
    }
}