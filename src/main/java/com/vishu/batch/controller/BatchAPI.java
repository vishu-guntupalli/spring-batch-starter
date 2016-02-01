package com.vishu.batch.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchAPI {
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private Job importProductsJob;
	
	@Autowired
	private Job processPlayersJob;

	@RequestMapping(method=RequestMethod.POST, value="/batch/api/startImportProductsJob", consumes=MediaType.APPLICATION_JSON_VALUE, produces=MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody Map<String, String> startImportProductsJob(@RequestBody Map<String, String> paramMap) throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		
		JobParametersBuilder parametersBuilder = new JobParametersBuilder();
		paramMap.forEach((key, value) -> parametersBuilder.addString(key, value));
		
		JobExecution jobExecution = jobLauncher.run(importProductsJob, parametersBuilder.toJobParameters());
		ExitStatus exitStatus = jobExecution.getExitStatus();
		HashMap<String, String> returnMap = new HashMap<String, String>();
		returnMap.put("ExitStatus", exitStatus.getExitCode().toString());
		return returnMap;
	}
	
	@RequestMapping(method=RequestMethod.POST, value="/batch/api/startProcessPlayersJob", consumes=MediaType.APPLICATION_JSON_VALUE, produces=MediaType.APPLICATION_JSON_VALUE)
	public @ResponseBody Map<String, String> startProcessPlayersJob(@RequestBody Map<String, String> paramMap) throws Exception{
		
		JobParametersBuilder parametersBuilder = new JobParametersBuilder();
		paramMap.forEach((key, value) -> parametersBuilder.addString(key, value));
		
		JobExecution jobExecution = jobLauncher.run(processPlayersJob, parametersBuilder.toJobParameters());
		ExitStatus exitStatus = jobExecution.getExitStatus();
		HashMap<String, String> returnMap = new HashMap<String, String>();
		returnMap.put("ExitStatus", exitStatus.getExitCode().toString());
		return returnMap;
	}

	public JobLauncher getJobLauncher() {
		return jobLauncher;
	}

	public void setJobLauncher(JobLauncher jobLauncher) {
		this.jobLauncher = jobLauncher;
	}

	public Job getImportProductsJob() {
		return importProductsJob;
	}

	public void setImportProductsJob(Job importProductsJob) {
		this.importProductsJob = importProductsJob;
	}

	public Job getProcessPlayersJob() {
		return processPlayersJob;
	}

	public void setProcessPlayersJob(Job processPlayersJob) {
		this.processPlayersJob = processPlayersJob;
	}

}
