package com.vishu.batch.writer;

import java.net.URI;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vishu.batch.model.DiscountProduct;

public class RestApiWriter {

	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private ObjectMapper objectMapper;
	
	@Value("${erpConnectString}")
	private String erpConnectString;
	
	public void sendToErp(DiscountProduct discountProduct) throws Exception {
			restTemplate.postForEntity(new URI(erpConnectString), objectMapper.writeValueAsString(discountProduct), String.class);
	}

}
