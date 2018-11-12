package io.magicalne.smym.exchanges.bitmex;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.OrderApi;
import io.swagger.client.model.Order;

import java.math.BigDecimal;
import java.util.List;

public class BitmexExchange {
  private static final String URL = "https://testnet.bitmex.com/api/v1";
//  private static final String URL = "https://www.bitmex.com/api/v1";
  private static final String BUY = "Buy";
  private static final String SELL = "Sell";

  public static final String ORDER_STATUS_FILLED = "Filled";
  public static final String ORDER_STATUS_CANCELED = "Canceled";
  public static final String GOOD_TILL_CANCEL = "GoodTillCancel";

  private final OrderApi orderApi;

  public BitmexExchange(String accessId, String secretKey) {
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(URL);
    apiClient.setApiKey(secretKey);
    this.orderApi = new OrderApi(apiClient);
  }

  public Order placeLimitLongOrder(String symbol, double price, int contracts) throws ApiException {
    return this.orderApi.orderNew(symbol, BUY, null, new BigDecimal(contracts), price, null,
      null, null, null, null, null, "Limit",
      GOOD_TILL_CANCEL, null, null, null);
  }

  public Order placeLimitShortOrder(String symbol, double price, int contacts) throws ApiException {
    return this.orderApi.orderNew(symbol, SELL, null, new BigDecimal(contacts), price, null,
      null, null, null, null, null, "Limit",
      GOOD_TILL_CANCEL, null, null, null);
  }

  public Order placeMarketLongOrder(String symbol, int contracts) throws ApiException {
    return this.orderApi.orderNew(symbol, BUY, null, new BigDecimal(contracts), null, null,
      null, null, null, null, null, "Market",
      GOOD_TILL_CANCEL, null, null, null);
  }

  public Order placeMarketShortOrder(String symbol, int contracts) throws ApiException {
    return this.orderApi.orderNew(symbol, SELL, null, new BigDecimal(contracts), null, null,
      null, null, null, null, null, "Market",
      GOOD_TILL_CANCEL, null, null, null);
  }

  public Order amendOrderPrice(String orderId, double price) throws ApiException {
    return this.orderApi.orderAmend(orderId, null, null, null, null,
      null, null, price, null, null, null);
  }

  public Order cancel(String orderId) throws ApiException, BitmexCancelOrderException {
    List<Order> orders = this.orderApi.orderCancel(orderId, null, null);
    if (orders == null || orders.isEmpty() || orders.size() > 1) {
      throw new BitmexCancelOrderException("Should only cancel one order. But now: " + orders);
    }
    return orders.get(0);
  }
}
