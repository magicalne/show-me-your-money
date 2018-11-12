package io.magicalne.smym.exchanges.bitmex;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.OffsetDateTimeSerializer;
import com.google.common.base.Preconditions;
import io.swagger.client.model.Order;
import lombok.Data;
import okhttp3.*;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

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
    JavaTimeModule module = new JavaTimeModule();
    module.addSerializer(OffsetDateTime.class, OffsetDateTimeSerializer.INSTANCE);
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
