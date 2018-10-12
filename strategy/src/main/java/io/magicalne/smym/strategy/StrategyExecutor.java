package io.magicalne.smym.strategy;

public class StrategyExecutor {

  public static void main(String[] args) throws InterruptedException {
    String accessKey = System.getenv("BINANCE_ACCESS_KEY");
    String secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
    MarketMakingV1 mm = new MarketMakingV1(accessKey, secretKey, "BNBBTC", "1", "1.003", 3, "0.0015276");
    mm.execute();
  }
}
