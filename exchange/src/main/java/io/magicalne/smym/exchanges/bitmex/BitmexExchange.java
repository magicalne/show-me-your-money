package io.magicalne.smym.exchanges.bitmex;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexPrivateOrder;
import org.knowm.xchange.bitmex.dto.trade.BitmexReplaceOrderParameters;
import org.knowm.xchange.bitmex.service.BitmexTradeService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.MarketOrder;

import java.math.BigDecimal;

public class BitmexExchange {
  private static final String URL = "https://testnet.bitmex.com/api/v1";
//  private static final String URL = "https://www.bitmex.com/api/v1";

  public static final String ORDER_STATUS_FILLED = "Filled";
  public static final String ORDER_STATUS_CANCELED = "Canceled";
  public static final String GOOD_TILL_CANCEL = "GoodTillCancel";

  private final BitmexTradeService tradeService;

  public BitmexExchange(String accessId, String secretKey) {
    ExchangeSpecification exSpec = new org.knowm.xchange.bitmex.BitmexExchange().getDefaultExchangeSpecification();
    exSpec.setApiKey(accessId);
    exSpec.setSecretKey(secretKey);
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(exSpec);
    tradeService = ((BitmexTradeService) exchange.getTradeService());
  }

  public String placeLimitLongOrder(CurrencyPair pair, double price, int contracts) {
    LimitOrder limitOrder = new LimitOrder(Order.OrderType.BID, new BigDecimal(contracts), pair, null, null, new BigDecimal(price));
    return this.tradeService.placeLimitOrder(limitOrder);
  }

  public String placeLimitShortOrder(CurrencyPair pair, double price, int contracts) {
    LimitOrder limitOrder = new LimitOrder(Order.OrderType.ASK, new BigDecimal(contracts), pair, null, null, new BigDecimal(price));
    return this.tradeService.placeLimitOrder(limitOrder);
  }

  public String placeMarketLongOrder(CurrencyPair pair, int contracts) {
    MarketOrder marketOrder = new MarketOrder(Order.OrderType.BID, new BigDecimal(contracts), pair);
    return this.tradeService.placeMarketOrder(marketOrder);
  }

  public String placeMarketShortOrder(CurrencyPair pair, int contracts) {
    MarketOrder marketOrder = new MarketOrder(Order.OrderType.ASK, new BigDecimal(contracts), pair);
    return this.tradeService.placeMarketOrder(marketOrder);
  }

  public BitmexPrivateOrder amendOrderPrice(String orderId, int contracts, double price) {
    BitmexReplaceOrderParameters para = new BitmexReplaceOrderParameters(orderId, null, null,
      null, null, new BigDecimal(contracts), null, new BigDecimal(price),
      null, null, null);
    return this.tradeService.replaceOrder(para);
  }

  public boolean cancel(String orderId) {
    return this.tradeService.cancelOrder(orderId);

  }
}
