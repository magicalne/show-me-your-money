package io.magicalne.smym.exception;

public class BuyFailureException extends RuntimeException {
    private static final String ERR_MSG = "Buy %s at %s with %s failed. response: %s";
    private static final String ERR_MSG_1 = "Buy %s failed. order id: %d";

    public BuyFailureException(String symbol, String price, String qty, String response) {
        super(String.format(ERR_MSG, symbol, price, qty, response));
    }

    public BuyFailureException(String symbol, Long orderId) {
        super(String.format(ERR_MSG_1, symbol, orderId));
    }

}
