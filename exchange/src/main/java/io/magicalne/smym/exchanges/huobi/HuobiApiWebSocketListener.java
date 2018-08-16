package io.magicalne.smym.exchanges.huobi;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.magicalne.smym.Utils;
import io.magicalne.smym.exchanges.UniverseApiCallback;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

import java.io.IOException;

@Slf4j
public class HuobiApiWebSocketListener<T> extends WebSocketListener {
    private final UniverseApiCallback<T> callback;
    private final Class<T> eventClass;
    private boolean closing = false;
    private final ObjectMapper objectMapper = new ObjectMapper();


    public HuobiApiWebSocketListener(UniverseApiCallback<T> callback, Class<T> eventClass) {
        this.callback = callback;
        this.eventClass = eventClass;
    }

    @Override
    public void onMessage(WebSocket webSocket, ByteString bytes) {
        String res = null;
        try {
            byte[] unGzip = Utils.ungzip(bytes.toByteArray());
            res = new String(unGzip);
            String ping = "ping";
            if (res.contains(ping)) {
                webSocket.send(res.replace(ping, "pong"));
            } else if (res.contains("subbed")) {
                log.info(res);
            }else {
                T event = objectMapper.readValue(res, this.eventClass);
                this.callback.onResponse(event);
            }
        } catch (IOException e) {
            log.error("Read huobi pro api message with exception. res: {}, exception: {}", res, e);
        } catch (Exception e) {
            log.error("WTF???", e);
        }
    }

    @Override
    public void onClosing(WebSocket webSocket, int code, String reason) {
        this.closing = true;
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, Response response) {
        if (!this.closing) {
            this.callback.onFailure(t);
        }

    }
}
