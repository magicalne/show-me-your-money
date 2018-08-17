package io.magicalne.smym.exception;

public class SellFailureException extends RuntimeException {
    private static final String ERR_MSG = "Sell %s at %s with %s failed. resoibse: %s";

    public SellFailureException(String symbol, String price, String qty, String res) {
        super(String.format(ERR_MSG, symbol, price, qty, res));
    }

    public SellFailureException(Throwable cause) {
        super(cause);
    }
}
