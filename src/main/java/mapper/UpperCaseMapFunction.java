package mapper;

import org.apache.flink.api.common.functions.MapFunction;

public class UpperCaseMapFunction implements MapFunction<String, String> {
  
  @Override
  public String map(String o) {
    return o.toUpperCase();
  }
}
