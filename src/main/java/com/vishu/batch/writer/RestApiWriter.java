package com.vishu.batch.writer;

import java.net.URI;
import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vishu.batch.model.DiscountProduct;

public class RestApiWriter implements ItemWriter<DiscountProduct>{

	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private ObjectMapper objectMapper;
	
	@Override
	public void write(List<? extends DiscountProduct> items) throws Exception {
		for(DiscountProduct discountProduct: items) {
			restTemplate.postForEntity(new URI("http://localhost:8000/oracle/erp"), objectMapper.writeValueAsString(discountProduct), String.class);
		}
	}

}
