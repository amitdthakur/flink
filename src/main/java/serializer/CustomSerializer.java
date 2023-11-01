package serializer;

import model.User;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class CustomSerializer implements SerializationSchema<User> {

  private ObjectMapper objectMapper;

  /**
   * @param context
   * @throws Exception
   */
  @Override
  public void open(InitializationContext context) throws Exception {
    objectMapper = new ObjectMapper();
  }

  /**
   * @param user
   * @return
   */
  @Override
  public byte[] serialize(User user) {
    try {
      return objectMapper.writeValueAsBytes(user);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}