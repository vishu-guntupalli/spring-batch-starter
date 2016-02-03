package com.vishu.test.batch.reader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vishu.batch.config.BatchConfiguration;
import com.vishu.batch.model.Product;
import com.vishu.test.batch.config.TestConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes={BatchConfiguration.class, TestConfiguration.class})
public class TestItemReader {

	@Autowired
	@Qualifier("productsFileReader")
	private FlatFileItemReader<Product> productsFileReader;
	
	@Before
	public void setUp() {
		
	}
	
	@After
	public void tearDown() {
		productsFileReader.close();
	}
	
	@Test
	public void testProductFileReader_testCount() throws UnexpectedInputException, ParseException, Exception {
		productsFileReader.setResource(new ClassPathResource("TestFruit.csv"));
		productsFileReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());

		int count = 0;
		Product product;
		while((product = productsFileReader.read())!=null){
			Assert.assertNotNull(product);
			count++;
		}
		Assert.assertEquals(5, count);
	}
	
}
