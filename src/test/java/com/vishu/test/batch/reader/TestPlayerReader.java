package com.vishu.test.batch.reader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vishu.batch.config.BatchConfiguration;
import com.vishu.batch.model.BaseballPlayer;
import com.vishu.test.batch.config.TestConfiguration;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes={TestConfiguration.class, BatchConfiguration.class})
public class TestPlayerReader {
	
	@Autowired
	@Qualifier("playerReader")
	private FlatFileItemReader<BaseballPlayer> playerReader;
	
	@Before
	public void setUp() {
		
	}
	
	@After
	public void tearDown(){
		playerReader.close();
	}
	
	@Test
	public void testPlayerReader_testCount() throws UnexpectedInputException, ParseException, Exception {
		playerReader.setResource(new ClassPathResource("TestAwardsPlayers.csv"));
		playerReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		
		BaseballPlayer baseballPlayer;
		int count = 0;
		while((baseballPlayer=playerReader.read()) != null) {
			count++;
		}
		Assert.assertEquals(3, count);
	}
	
	@Test(expected=FlatFileParseException.class)
	public void testPlayerReader_FlatFileParseException() throws UnexpectedInputException, ParseException, Exception {
		playerReader.setResource(new ClassPathResource("TestAwardsPlayers_bad.csv"));
		playerReader.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		
		playerReader.read();
	}

}
