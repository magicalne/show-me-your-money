package io.magicalne.smym.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.magicalne.smym.dto.SubKlineRes;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SubEventHandler {

    private Map<String, SubKlineRes> sub;
    private final ObjectMapper objectMapper;
    public SubEventHandler(int size) {
        this.sub = new ConcurrentHashMap<>(size / 3 * 4);
        objectMapper = new ObjectMapper();
    }

    public void update(String res) throws IOException {
        SubKlineRes kline = objectMapper.readValue(res, SubKlineRes.class);
        String ch = kline.getCh();
        String symbol = extractSymbolFromCh(ch);
        sub.put(symbol, kline);
    }

    String extractSymbolFromCh(String ch) {
        char dot = '.';
        int first = ch.indexOf(dot);
        int second = ch.indexOf(dot, first+1);
        return ch.substring(first+1, second);
    }

    public Map<String, SubKlineRes> getSub() {
        return sub;
    }
}
