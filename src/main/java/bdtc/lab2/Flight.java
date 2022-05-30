package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Flight {
    private String hour;
    private String countryin;
    private String countryout;
}
