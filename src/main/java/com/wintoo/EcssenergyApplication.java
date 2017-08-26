package com.wintoo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EcssenergyApplication {

	public static void main(String[] args) {
		SpringApplication.run(EcssenergyApplication.class, args);
	}
}
