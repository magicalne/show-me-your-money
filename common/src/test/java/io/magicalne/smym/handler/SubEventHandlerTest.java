package io.magicalne.smym.handler;

import org.junit.Assert;
import org.junit.Test;

public class SubEventHandlerTest {

    @Test
    public void extractSymbolFromChTest() {
        SubEventHandler handler = new SubEventHandler(16);
        String symbol = handler.extractSymbolFromCh("market.btcusdt.kline.1min");
        Assert.assertEquals(symbol, "btcusdt");
    }
}
