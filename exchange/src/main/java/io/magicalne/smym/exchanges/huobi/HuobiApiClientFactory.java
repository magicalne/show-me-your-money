package io.magicalne.smym.exchanges.huobi;

public class HuobiApiClientFactory {

    private String accessKey;
    private String secretKey;

    private HuobiApiClientFactory() {
    }

    private HuobiApiClientFactory(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public static HuobiApiClientFactory newInstance(String accessKey, String scretKey) {
        return new HuobiApiClientFactory(accessKey, scretKey);
    }

    public HuobiProRest createRestClient() {
        return new HuobiProRest(accessKey, secretKey);
    }

    public HuobiProWebSocketClient createWebSocketClient() {
        return new HuobiProWebSocketClient();
    }
}
