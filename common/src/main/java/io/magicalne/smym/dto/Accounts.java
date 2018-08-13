package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class Accounts {
    /**
     * id : 100009
     * type : spot
     * state : working
     * user-id : 1000
     */

    private int id;
    private String type;
    private String state;
    private int userid;
}
