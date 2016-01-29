package com.vishu.batch.config;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.vishu.batch.model.BaseballPlayer;
import com.vishu.batch.model.DiscountProduct;
import com.vishu.batch.model.Product;
import com.vishu.batch.processor.BaseballPlayerProcessor;
import com.vishu.batch.processor.ProductItemProcessor;


@Configuration
@EnableBatchProcessing
@PropertySource(value = { "classpath:/com/vishu/batch/batch.properties" })
public class BatchConfiguration {
	
	private static final String DATABASE_TYPE = "MySQL";

	private static final String ISOLATION_LEVEL = "ISOLATION_READ_UNCOMMITTED";

	//File Read and write paths
	@Value("${productFilePath}")
	private String productFilePath;
	
	@Value("${playerFilePath}")
	private String playerFilePath;
	
	@Value("${writeFilePath}")
	private String writeFilePath;
	
	@Value("${playerFileWritePath}")
	private String playerFileWritePath;
	
    // The following four variables are used for DB connection	
	@Value("${spring.datasource.url}")
	private String url;
	
	@Value("${spring.datasource.username}")
	private String username;
	
	@Value("${spring.datasource.password}")
	private String password;
	
	@Value("${spring.datasource.driverClassName}")
	private String driverClassName;
	
	@Bean
    public ItemReader<Product> productsFileReader() {
        FlatFileItemReader<Product> reader = new FlatFileItemReader<Product>();
        reader.setResource(new ClassPathResource(productFilePath));
        reader.setLineMapper(new DefaultLineMapper<Product>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "productId", "productName", "price" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {{
                setTargetType(Product.class);
            }});
        }});
        return reader;
    }
	
	@Bean
	public ItemReader<BaseballPlayer> playerReader() {
		FlatFileItemReader<BaseballPlayer> playerReader = new FlatFileItemReader<BaseballPlayer>();
		playerReader.setResource(new ClassPathResource(playerFilePath));
		playerReader.setLinesToSkip(1);
		playerReader.setLineMapper(new DefaultLineMapper<BaseballPlayer>(){{
			setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "playerId", "awardType", "year", "leagueType", "isTie", "notes" });
            }});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<BaseballPlayer>() {{
                setTargetType(BaseballPlayer.class);
            }});	
		}});
		
		return playerReader;
	}
	
	@Bean 
	public ItemProcessor<Product, DiscountProduct> productProcessor() {
		return new ProductItemProcessor();
	}
	
	@Bean
	public ItemProcessor<BaseballPlayer, BaseballPlayer> playerProcessor() {
		return new BaseballPlayerProcessor();
	}

    @Bean
    public ItemWriter<DiscountProduct> productWriter() {
    	FlatFileItemWriter<DiscountProduct> discountProductWriter = new FlatFileItemWriter<DiscountProduct>();
    	discountProductWriter.setResource(new ClassPathResource(writeFilePath));
    	discountProductWriter.setForceSync(true);
    	
    	DelimitedLineAggregator<DiscountProduct> lineAggregator = new DelimitedLineAggregator<DiscountProduct>();
    	lineAggregator.setDelimiter(",");
    	
    	BeanWrapperFieldExtractor<DiscountProduct> fieldExtractor = new BeanWrapperFieldExtractor<DiscountProduct>();
    	fieldExtractor.setNames(new String[] {"productId", "productName", "price", "discountAvailable"});
    	
    	lineAggregator.setFieldExtractor(fieldExtractor);
    	discountProductWriter.setLineAggregator(lineAggregator);
    	
    	return discountProductWriter;
    }
    
    @Bean
    public ItemWriter<BaseballPlayer> playerWriter() {
    	FlatFileItemWriter<BaseballPlayer> playerWriter = new FlatFileItemWriter<BaseballPlayer>();
    	playerWriter.setResource(new ClassPathResource(playerFileWritePath));
    	playerWriter.setForceSync(true);
    	
    	DelimitedLineAggregator<BaseballPlayer> lineAggregator = new DelimitedLineAggregator<BaseballPlayer>();
    	lineAggregator.setDelimiter(",");
    	
    	BeanWrapperFieldExtractor<BaseballPlayer> fieldExtractor = new BeanWrapperFieldExtractor<BaseballPlayer>();
    	fieldExtractor.setNames(new String[] { "playerId", "awardType", "year", "leagueType", "isTie", "notes" });
    	
    	lineAggregator.setFieldExtractor(fieldExtractor);
    	playerWriter.setLineAggregator(lineAggregator);
    	
    	return playerWriter;
    	
    }
    
    @Bean 
    public BasicDataSource dataSource() {
    	BasicDataSource datasource = new BasicDataSource();
    	datasource.setUrl(url);
        datasource.setUsername(username);
        datasource.setPassword(password);
        datasource.setDriverClassName(driverClassName);
        
        return datasource;
    }
    
    @Bean
    public DataSourceTransactionManager transactionManager() {
    	DataSourceTransactionManager transactionManager = new DataSourceTransactionManager();
    	transactionManager.setDataSource(dataSource());
    	
    	return transactionManager;
    }
    
	@Bean 
    public JobRepository jobRepository() throws Exception {
    	JobRepositoryFactoryBean jobRepository = new JobRepositoryFactoryBean();
    	jobRepository.setDataSource(dataSource());
    	jobRepository.setTransactionManager(transactionManager());
    	jobRepository.setIsolationLevelForCreate(ISOLATION_LEVEL);
    	jobRepository.setDatabaseType(DATABASE_TYPE);
    	
    	return jobRepository.getJobRepository();
    }
	
	@Bean
	public JobLauncher jobLauncher(JobRepository jobRepository) {
		SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
		simpleJobLauncher.setJobRepository(jobRepository);
		
		return simpleJobLauncher;
	}
    
    @Bean(name="importProductJob")
    public Job importProductJob(JobBuilderFactory jobs, Step processProducts) throws Exception {
        return jobs.get("importProductJob")
                .incrementer(new RunIdIncrementer())
                .repository(jobRepository())
                .flow(processProducts)
                .end()
                .build();
    }
    
    @Bean
    public Job processPlayersJob(JobBuilderFactory jobs, Step processPlayers) throws Exception {
    	return jobs.get("processPlayersJob")
                .incrementer(new RunIdIncrementer())
                .repository(jobRepository())
                .flow(processPlayers)
                .end()
                .build();
    }

    @Bean
    public Step processProducts(StepBuilderFactory stepBuilderFactory, ItemReader<Product> reader,
            ItemWriter<DiscountProduct> writer, ItemProcessor<Product, DiscountProduct> processor) {
        return stepBuilderFactory.get("processProducts")
                .<Product, DiscountProduct> chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
    
    @Bean
    public Step processPlayers(StepBuilderFactory stepBuilderFactory, ItemReader<BaseballPlayer> reader,
    		ItemWriter<BaseballPlayer> writer, ItemProcessor<BaseballPlayer, BaseballPlayer> itemProcessor) {
    	return stepBuilderFactory.get("processPlayers")
    			.<BaseballPlayer, BaseballPlayer> chunk(10)
    			.reader(reader)
    			.processor(itemProcessor)
    			.writer(writer)
    			.build();
    }
	
}
