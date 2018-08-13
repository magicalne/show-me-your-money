package io.magicalne.smym.exchanges;

import com.binance.api.client.BinanceApiCallback;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BinanceApiCallbackWrapper<T> implements BinanceApiCallback<T> {

    @Override
    public void onFailure(Throwable cause) {
        log.error("binance api exception:", cause);
    }
}
