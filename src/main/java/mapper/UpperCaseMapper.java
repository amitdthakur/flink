package mapper;

import org.apache.flink.api.common.functions.MapFunction;

public class UpperCaseMapper implements MapFunction<String, String> {


  @Override
  public String map(String o) {
    return o.toUpperCase();
  }
}
