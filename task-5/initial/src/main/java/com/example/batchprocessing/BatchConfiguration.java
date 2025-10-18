package com.example.batchprocessing;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.rmi.ConnectException;

@Configuration
public class BatchConfiguration {

	@Bean
	public FlatFileItemReader<Product> reader() {
        var resource = new ClassPathResource("product-data.csv");
		return new FlatFileItemReaderBuilder<Product>()
            .name("productReader")
                .resource(resource)
                .delimited()
                .delimiter(",")
                .names("productId", "productSku","productName", "productAmount", "productData")
			.targetType(Product.class)
			.build();
	}

    private final String Cmd =
                    """
                    INSERT INTO products (productId, productSku, productName, productAmount, productData)
                    VALUES (:productId, :productSku, :productName, :productAmount, :productData)
                    on conflict(productId)
                    do update 
                        set productSku = EXCLUDED.productSku,
                        productName = EXCLUDED.productName,
                        productAmount = EXCLUDED.productAmount,
                        productData = EXCLUDED.productData;
                    """;
	@Bean
	public JdbcBatchItemWriter<Product> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Product>()
                .dataSource(dataSource)
                .sql(Cmd)
                .beanMapped()
                .build();
	}

	@Bean
	public Job importProductJob(JobRepository jobRepository, Step step1, JobCompletionNotificationListener listener) {
		return new JobBuilder("import_product_job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
	}

	@Bean
	public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
					  FlatFileItemReader<Product> reader, ProductItemProcessor processor, JdbcBatchItemWriter<Product> writer) {
		return new StepBuilder("import", jobRepository)
                .<Product, Product>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .retry(ConnectException.class)
                .retryLimit(3)
                .build();
	}

}
