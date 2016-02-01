package com.vishu.batch.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vishu.batch.config.BatchConfiguration;
import com.vishu.batch.config.TestConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={BatchConfiguration.class, TestConfiguration.class})
public class BatchAPITest {

	@InjectMocks
	private BatchAPI batchAPI;
	
	@Autowired
	private Job importProductsJob;
	
	@Autowired
	private Job processPlayersJob;
	
	@Mock
	private JobLauncher mockJoblauncher;
	
	@Captor
	private ArgumentCaptor<Job> jobCaptor;
	
	@Captor 
	private ArgumentCaptor<JobParameters> parametersCaptor;
	
	@Before
	public void setUp() {
	    MockitoAnnotations.initMocks(this);
	    batchAPI.setImportProductsJob(importProductsJob);
	    batchAPI.setProcessPlayersJob(processPlayersJob);
	}
	
	@Test
	public void testStartImportProductsJob() throws Exception{
		JobExecution jobExecution = new JobExecution((long) 123);
		jobExecution.setExitStatus(ExitStatus.COMPLETED);
		
		Map<String, String> paramMap = new LinkedHashMap<String, String>();
		paramMap.put("foo", "bar");
	   
		Mockito.when(mockJoblauncher.run(jobCaptor.capture(), parametersCaptor.capture())).thenReturn(jobExecution);
		
		HashMap<String, String> returnMap= (HashMap<String, String>) batchAPI.startImportProductsJob(paramMap);
		
		assertNotNull(jobCaptor.getValue());
        assertEquals("importProductsJob", jobCaptor.getValue().getName());
        assertNotNull(parametersCaptor.getValue());
		assertEquals(paramMap.size(), parametersCaptor.getValue().getParameters().size());
		assertEquals(paramMap.toString(), parametersCaptor.getValue().getParameters().toString());
		Mockito.verify(mockJoblauncher).run(jobCaptor.getValue(), parametersCaptor.getValue());
		assertNotNull(returnMap);
		assertEquals(ExitStatus.COMPLETED.getExitCode().toString(), returnMap.get("ExitStatus"));
	}
	
	@Test
	public void testProcessPlayersJob() throws Exception {
		JobExecution jobExecution = new JobExecution((long) 123);
		jobExecution.setExitStatus(ExitStatus.COMPLETED);
		
		Map<String, String> paramMap = new LinkedHashMap<String, String>();
		paramMap.put("foo", "bar");
		
		Mockito.when(mockJoblauncher.run(jobCaptor.capture(), parametersCaptor.capture())).thenReturn(jobExecution);
		
		HashMap<String, String> returnMap= (HashMap<String, String>) batchAPI.startProcessPlayersJob(paramMap);
		
		assertNotNull(jobCaptor.getValue());
		assertEquals("processPlayersJob", jobCaptor.getValue().getName());
		assertNotNull(parametersCaptor.getValue());
		assertEquals(paramMap.toString(), parametersCaptor.getValue().getParameters().toString());
		assertEquals(paramMap.size(), parametersCaptor.getValue().getParameters().size());
		Mockito.verify(mockJoblauncher).run(jobCaptor.getValue(), parametersCaptor.getValue());
		assertEquals(ExitStatus.COMPLETED.getExitCode().toString(), returnMap.get("ExitStatus"));
	}
	
}
