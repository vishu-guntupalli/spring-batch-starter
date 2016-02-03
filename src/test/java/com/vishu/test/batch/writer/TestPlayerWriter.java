package com.vishu.test.batch.writer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vishu.batch.config.BatchConfiguration;
import com.vishu.batch.model.BaseballPlayer;
import com.vishu.test.batch.config.TestConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {TestConfiguration.class, BatchConfiguration.class})
public class TestPlayerWriter {
	
	@Autowired
	private FlatFileItemWriter<BaseballPlayer> playerWriter;
	
	@Test
	public void testPlayerWriter() throws Exception{
		ArrayList<BaseballPlayer> playersList = new ArrayList<>();
		ArrayList<String> expectedTokenList = new ArrayList<>();
		ArrayList<String> actualTokenList = new ArrayList<>();
		
		playerWriter.setResource(new ClassPathResource("TestAwardsPlayers_output.csv"));
		playerWriter.open(MetaDataInstanceFactory.createStepExecution().getExecutionContext());
		
		BaseballPlayer baseballPlayer = new BaseballPlayer();
		baseballPlayer.setPlayerId("vishu16");
		baseballPlayer.setNotes("Good Player");
		baseballPlayer.setLeagueType("Big League");
		baseballPlayer.setAwardType("Good Programmer");
		baseballPlayer.setYear(2016);
		baseballPlayer.setIsTie("N");
		
		playersList.add(baseballPlayer);
		
		playerWriter.write(playersList);
		playersList.forEach((player) -> expectedTokenList.add(player.getPlayerId()+","+player.getAwardType()+","+player.getYear()+","+player.getLeagueType()+","+player.getIsTie()+","+player.getNotes()));
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/guntupv/Documents/spring-batch-starter/target/test-classes/TestAwardsPlayers_output.csv"));
		String line = "";
		while((line=bufferedReader.readLine()) != null) {
			String[] tokens = line.split(",");
			actualTokenList.add(tokens[0]+","+tokens[1]+","+tokens[2]+","+tokens[3]+","+tokens[4]+","+tokens[5]); 
		}
		
		Assert.assertTrue(expectedTokenList.size() > 0);
		Assert.assertEquals(actualTokenList, expectedTokenList);
	}
}
