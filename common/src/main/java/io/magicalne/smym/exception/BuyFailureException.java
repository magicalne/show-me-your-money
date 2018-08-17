package io.magicalne.smym.exception;

public class BuyFailureException extends RuntimeException {
    private static final String ERR_MSG = "Buy %s at %s with %s failed. response: %s";

    public BuyFailureException(String symbol, String price, String qty, String response) {
        super(String.format(ERR_MSG, symbol, price, qty, response));
    }
}
