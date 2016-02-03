package com.vishu.test.batch.writer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vishu.batch.config.BatchConfiguration;
import com.vishu.batch.model.DiscountProduct;
import com.vishu.test.batch.config.TestConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes={TestConfiguration.class, BatchConfiguration.class})
public class TestProductWriter {
	
	@Autowired
	private FlatFileItemWriter<DiscountProduct> productWriter;
	
	@Before
	public void setUp() {
		
	}
	
	@After
	public void tearDown() {
		productWriter.close();
	}
	
	@Test
	public void testProductWriter() throws Exception {
		ArrayList<DiscountProduct> discountProductList = new ArrayList<>();
		ArrayList<String> expectedTokenList = new ArrayList<>();
		ArrayList<String> actualTokenList = new ArrayList<>();
		
		productWriter.setResource(new ClassPathResource("TestFruitWithDiscount.csv"));
		productWriter.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		
		DiscountProduct discountProduct1 = new DiscountProduct();
		discountProduct1.setProductId(1);
		discountProduct1.setProductName("Product 1");
		discountProduct1.setPrice(2.50);
		discountProduct1.setDiscountAvailable(false);
		
		discountProductList.add(discountProduct1);
		productWriter.write(discountProductList);
		
		discountProductList.forEach((discountProduct) -> expectedTokenList.add(discountProduct.getProductId()+","+discountProduct.getProductName()+","+discountProduct.getPrice()+","+discountProduct.isDiscountAvailable()));
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/guntupv/Documents/spring-batch-starter/target/test-classes/TestFruitWithDiscount.csv"));
		String line = "";
		while((line=bufferedReader.readLine()) != null) {
			String[] tokens = line.split(",");
			actualTokenList.add(tokens[0]+","+tokens[1]+","+tokens[2]+","+tokens[3]); 
		}
		
		Assert.assertTrue(expectedTokenList.size() > 0);
		Assert.assertEquals(actualTokenList, expectedTokenList);
	}

}
