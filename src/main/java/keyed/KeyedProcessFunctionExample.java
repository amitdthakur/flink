package keyed;

import model.User;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.util.Collector;

public class KeyedProcessFunctionExample extends
    KeyedProcessFunction<String, User, String> {


  @Override
  public void processElement(User value, KeyedProcessFunction<String, User, String>.Context ctx,
      Collector<String> out) throws Exception {
    out.collect(value.getName().toUpperCase());
  }
}
