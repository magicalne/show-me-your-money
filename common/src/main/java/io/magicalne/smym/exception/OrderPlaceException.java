package io.magicalne.smym.exception;

public class OrderPlaceException extends RuntimeException {
    public OrderPlaceException(String req) {
        super("Cannot place this order! Request: " + req);
    }
}
