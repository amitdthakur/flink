package process;

import java.util.Locale;
import model.User;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.Collector;

public class SimpleProcessFunction extends ProcessFunction<User, String> {


  @Override
  public void processElement(User value, ProcessFunction<User, String>.Context ctx,
      Collector<String> out) throws Exception {
    out.collect(value.getName().toUpperCase());
  }
}
