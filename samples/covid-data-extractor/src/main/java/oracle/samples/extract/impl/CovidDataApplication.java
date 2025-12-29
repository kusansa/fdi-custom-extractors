package oracle.samples.extract.impl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
public class CovidDataApplication {
    public static void main(String[] args) {
        SpringApplication.run(CovidDataApplication.class, args);
    }
}

