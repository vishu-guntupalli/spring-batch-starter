package com.vishu.batch.writer;

import java.net.URI;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vishu.batch.model.DiscountProduct;

public class RestApiWriter {

	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private ObjectMapper objectMapper;
	
	public void sendToErp(DiscountProduct discountProduct) throws Exception {
			restTemplate.postForEntity(new URI("http://localhost:8000/oracle/erp"), objectMapper.writeValueAsString(discountProduct), String.class);
	}

}
