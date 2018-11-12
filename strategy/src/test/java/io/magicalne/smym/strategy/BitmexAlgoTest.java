package io.magicalne.smym.strategy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.magicalne.smym.exchanges.bitmex.BitmexDeltaClient;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import static java.lang.Double.NaN;

public class BitmexAlgoTest {

  private Queue<BitmexDeltaClient.OrderBookL2> queue;

  @Before
  public void setup() throws IOException {
    queue = new CircularFifoQueue<>(10);

    ClassLoader classLoader = getClass().getClassLoader();
    String filepath = Objects.requireNonNull(classLoader.getResource("orderbook_l2.json")).getFile();
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<Map<String, Map<String, Double>>> ref = new TypeReference<Map<String, Map<String, Double>>>() {};
    Map<String, Map<String, Double>> map = mapper.readValue(new File(filepath), ref);
    for (int i = 3499; i < 3510; i ++) {
      Map<String, Double> raw = map.get(i + "");
      List<BitmexDeltaClient.OrderBookEntry> bids = new LinkedList<>();
      List<BitmexDeltaClient.OrderBookEntry> asks = new LinkedList<>();
      for (int j = 0; j < 10; j ++) {
        BitmexDeltaClient.OrderBookEntry ask = new BitmexDeltaClient.OrderBookEntry();
        ask.setPrice(raw.get("ask_p_"+j));
        ask.setSize(new BigDecimal(raw.get("ask_vol_"+j)).longValue());
        asks.add(ask);

        BitmexDeltaClient.OrderBookEntry bid = new BitmexDeltaClient.OrderBookEntry();
        bid.setPrice(raw.get("bid_p_"+j));
        bid.setSize(new BigDecimal(raw.get("bid_vol_"+j)).longValue());
        bids.add(bid);
      }
      queue.add(new BitmexDeltaClient.OrderBookL2(asks, bids));
    }
  }

  @Test
  public void testExtractFeature() throws IOException {

    Map<String, Double> feature = BitmexAlgo.OrderFlowPrediction.extractFeature(queue);
    SortedSet<String> keys = new TreeSet<>(feature.keySet());

    Assert.assertEquals(feature.size(), 320);
    Assert.assertEquals(feature.get("bid_v_roll_4_kurt"), -5.9673713633452365, 0.001);
    Assert.assertEquals(feature.get("ask_p_roll_3_skew"), NaN, 0.001);
    Assert.assertEquals(feature.get("bid_v_roll_2_mean"), 3731272.0, 0.001);
    Assert.assertEquals(feature.get("spreed_vol_0"), -1746225.0, 0.001);
  }
}
