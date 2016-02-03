package com.vishu.test.batch.processor;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vishu.batch.config.BatchConfiguration;
import com.vishu.batch.model.DiscountProduct;
import com.vishu.batch.model.Product;
import com.vishu.test.batch.config.TestConfiguration;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes={TestConfiguration.class, BatchConfiguration.class})
public class TestItemProcessor {
   
	@Autowired
	private ItemProcessor<Product, DiscountProduct> productProcessor;
	
	@Test
	public void testProcessProducts() throws Exception {
		Product product = new Product();
		product.setProductId(1);
		product.setProductName("Apple");
		product.setPrice(2.50);
		
		DiscountProduct discountProduct = productProcessor.process(product);
		
		Assert.assertNotNull(discountProduct);
		Assert.assertFalse(discountProduct.isDiscountAvailable());
		Assert.assertEquals(product.getProductId(), discountProduct.getProductId());
		Assert.assertEquals(product.getPrice(), discountProduct.getPrice(), 0.0);
		Assert.assertEquals(product.getProductName(), discountProduct.getProductName());
	}
}
