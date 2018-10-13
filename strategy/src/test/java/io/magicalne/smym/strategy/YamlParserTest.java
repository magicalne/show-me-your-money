package io.magicalne.smym.strategy;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.magicalne.smym.dto.MarketMakingConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class YamlParserTest {

  @Test
  public void test() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    String filepath = Objects.requireNonNull(classLoader.getResource("mm.yaml")).getFile();
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    MarketMakingConfig config = mapper.readValue(new File(filepath), MarketMakingConfig.class);
    System.out.println(config);
  }
}
