package model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class User {

  private String name;
  private int accountNumber;
  private double amountToWithDraw;
  private String atmLocation;
}
