package com.vishu.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Application {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(Application.class, args);
        
        JobLauncher jobLauncher = (JobLauncher) applicationContext.getBean("jobLauncher");
        jobLauncher.run((Job) applicationContext.getBean("processPlayersJob"), new JobParameters());
    }
}
