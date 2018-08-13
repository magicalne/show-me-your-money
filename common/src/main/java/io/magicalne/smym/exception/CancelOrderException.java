package io.magicalne.smym.exception;

public class CancelOrderException extends Exception {

    public CancelOrderException(String orderId) {
        super("Cannot cancel this order: " + orderId);
    }
}
