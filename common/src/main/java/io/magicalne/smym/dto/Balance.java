package io.magicalne.smym.dto;

import lombok.Data;

import java.util.List;

@Data
public class Balance {
    /**
     * id : 100009
     * type : spot
     * state : working
     * list : [{"currency":"usdt","type":"trade","balance":"500009195917.4362872650"}]
     * user-id : 1000
     */

    private String id;
    private String type;
    private String state;
    private String userid;
    private List<BalanceBean> list;

}
