package io.magicalne.smym.exchanges.bitmex;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.threetenbp.ThreeTenModule;
import com.google.common.base.Preconditions;
import io.swagger.client.model.Order;
import lombok.Data;
import okhttp3.*;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexPublicTrade;
import org.knowm.xchange.bitmex.dto.trade.BitmexSide;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class BitmexDeltaClient {

  private static final String BUY = "Buy";
  private static final String SELL = "Sell";

  private final String baseUrl;
  private final OkHttpClient client;
  private final ObjectMapper objectMapper;

  public BitmexDeltaClient(String host, int port) {
    client = new OkHttpClient();
    baseUrl = "http://"+host+":"+port;
    objectMapper = new ObjectMapper();
    objectMapper
      .registerModule(new ThreeTenModule());
  }

  public Order getOrderById(String symbol, String orderId) throws BitmexQueryOrderException, IOException {
    Request req = new Request.Builder().url(baseUrl + "/order?symbol=" + symbol).get().build();
    Call call = client.newCall(req);
    try (Response res = call.execute()) {
      if (res.isSuccessful()) {
        ResponseBody body = res.body();
        Preconditions.checkNotNull(body);
        TypeReference<List<Order>> ref = new TypeReference<List<Order>>() {
        };
        List<Order> orders = objectMapper.readValue(body.string(), ref);
        for (Order order : orders) {
          if (order.getOrderID().equals(orderId)) {
            return order;
          }
        }
      }
    }
    throw new BitmexQueryOrderException("There is no such order in delta server. order id: " + orderId);
  }

  public OrderBookL2 getOrderBookL2(String symbol) throws IOException {
    Request req = new Request.Builder().url(baseUrl + "/orderBookL2_25?symbol=" + symbol).get().build();
    Call call = client.newCall(req);
    try (Response res = call.execute()) {
      if (res.isSuccessful()) {
        ResponseBody body = res.body();
        Preconditions.checkNotNull(body);
        TypeReference<List<OrderBookEntry>> ref = new TypeReference<List<OrderBookEntry>>() {
        };
        List<OrderBookEntry> entries = objectMapper.readValue(body.string(), ref);
        entries.sort(Comparator.comparingDouble(OrderBookEntry::getPrice));
        List<OrderBookEntry> asks = new ArrayList<>();
        List<OrderBookEntry> bids = new ArrayList<>();
        for (OrderBookEntry entry : entries) {
          if (BUY.equals(entry.getSide())) {
            bids.add(entry);
          } else {
            asks.add(entry);
          }
        }
        Collections.reverse(bids);
        return new OrderBookL2(asks, bids);
      }
    }

    throw new IOException("Cannot get order book!");
  }

  public Trades getTrade(String symbol) throws IOException {
    Request req = new Request.Builder().url(baseUrl + "/trade?symbol=" + symbol).get().build();
    Call call = client.newCall(req);
    try (Response res = call.execute()) {
      if (res.isSuccessful()) {
        ResponseBody body = res.body();
        Preconditions.checkNotNull(body);
        return objectMapper.readValue(body.string(), Trades.class);
      }
    }

    throw new IOException("Cannot get order book!");
  }

  @Data
  public static class Trades {
    private List<BitmexPublicTrade> trades;

    public Stats recentStats(long mills) {
      if (trades == null || trades.isEmpty()) {
        return null;
      }
      long t = System.currentTimeMillis() - mills;
      long buyVols = 0;
      long sellVols = 0;
      int buyOrders = 0;
      int sellOrders = 0;
      for (int i = trades.size() - 1; i > 0; i --) {
        BitmexPublicTrade trade = trades.get(i);
        if (trade.getTime().getTime() >= t) {
          long vol = trade.getSize().longValue();
          if (trade.getSide() == BitmexSide.BUY) {
            buyVols += vol;
            buyOrders ++;
          } else {
            sellVols += vol;
            sellOrders ++;
          }
        } else {
          break;
        }
      }
      if (buyVols == 0 && sellVols == 0) {
        return null;
      }
      double volImbalance = (buyVols - sellVols) * 1.0d / (buyVols + sellVols);
      double sideImbalance = (buyOrders - sellOrders) * 1.0d / (buyOrders + sellOrders);
      return new Stats(buyVols, sellVols, buyOrders, sellOrders, volImbalance, sideImbalance);
    }
  }

  @Data
  public static class Stats {
    private long buyVols;
    private long sellVols;
    private int buyOrders;
    private int sellOrders;
    private double volImbalance;
    private double sideImbalance;

    public Stats(long buyVols,
                 long sellVols,
                 int buyOrders,
                 int sellOrders,
                 double volImbalance,
                 double sideImbalance) {
      this.buyVols = buyVols;
      this.sellVols = sellVols;
      this.buyOrders = buyOrders;
      this.sellOrders = sellOrders;
      this.volImbalance = volImbalance;
      this.sideImbalance = sideImbalance;
    }
  }

  @Data
  public static class OrderBookL2 {
    private List<OrderBookEntry> bids; //desc
    private List<OrderBookEntry> asks; //asc

    public OrderBookL2(List<OrderBookEntry> asks, List<OrderBookEntry> bids) {
      this.asks = asks;
      this.bids = bids;
    }

    public double getBestBid() {
      return bids.get(0).getPrice();
    }

    public double getBestAsk() {
      return asks.get(0).getPrice();
    }

    public double imbalance() {
      long bidVol = bids.get(0).getSize();
      long askVol = asks.get(0).getSize();
      return (bidVol - askVol) *1.0d / (bidVol + askVol);
    }

    public double findFairBid() {
      long bidVol = bids.get(0).getSize();
      long askVol = asks.get(0).getSize();
      if (bidVol >= askVol) {
        return getBestBid();
      } else {
        double finalBid = getBestBid();
        for (OrderBookEntry e : bids) {
          askVol -= e.getSize();
          if (askVol < 0) {
            return e.getPrice();
          }
          finalBid = e.getPrice();
        }
        return finalBid;
      }
    }

    public double findFairAsk() {
      long bidVol = bids.get(0).getSize();
      long askVol = asks.get(0).getSize();
      if (bidVol <= askVol) {
        return getBestAsk();
      } else {
        double finalAsk = getBestAsk();
        for (OrderBookEntry e : asks) {
          bidVol -= e.getSize();
          if (bidVol < 0) {
            return e.getPrice();
          }
          finalAsk = e.getPrice();
        }
        return finalAsk;
      }
    }
  }

  @Data
  public static class OrderBookEntry {
    private String symbol;
    private long id ;
    private String side; //Sell or Buy
    private long size;
    private double price;
  }

}
