package window;

import model.User;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessWindowFunctionExample extends ProcessWindowFunction<User, User, Integer, TimeWindow> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessWindowFunctionExample.class);

  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param aDouble  The key for which this window is evaluated.
   * @param context  The context in which the window is being evaluated.
   * @param elements The elements in the window being evaluated.
   * @param out      A collector for emitting elements.
   */
  @Override
  public void process(Integer aDouble,
      ProcessWindowFunction<User, User, Integer, TimeWindow>.Context context,
      Iterable<User> elements, Collector<User> out) {
    double sumWithDrew = 0;
    String name = "";
    int accountNumber = 0;
    for (User user : elements) {
      sumWithDrew = sumWithDrew + user.getAmountToWithDraw();
      name = user.getName();
      accountNumber = user.getAccountNumber();
      LOGGER.info("User:{} AccountNumber:{} Sum:{} ", user.getName(), user.getAccountNumber(),
          sumWithDrew);
    }
    out.collect(User.builder().name(name).accountNumber(accountNumber).amountToWithDraw(sumWithDrew)
        .build());
  }
}