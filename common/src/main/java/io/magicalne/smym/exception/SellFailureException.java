package io.magicalne.smym.exception;

public class SellFailureException extends RuntimeException {
    private static final String ERR_MSG = "Sell %s at %s with %s failed. resoibse: %s";
    private static final String ERR_MSG_1 = "Sell symbol %s failed, order id: %d";

    public SellFailureException(String symbol, String price, String qty, String res) {
        super(String.format(ERR_MSG, symbol, price, qty, res));
    }

    public SellFailureException(String symbol, Long orderId) {
        super(String.format(ERR_MSG_1, symbol, orderId));
    }

    public SellFailureException(Throwable cause) {
        super(cause);
    }
}
