package io.magicalne.smym.exchanges;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BinanceEventHandler<T> {

    private final Map<String, T> subMap;
    public BinanceEventHandler(int size) {
        this.subMap = new ConcurrentHashMap<>(size / 3 * 4);
    }

    public void update(String symbol, T event) {
        log.info("update symbol: {}, {}", symbol, event);
        this.subMap.put(symbol, event);
    }

    public T getEventBySymbol(String symbol) {
        return this.subMap.get(symbol);
    }
}
