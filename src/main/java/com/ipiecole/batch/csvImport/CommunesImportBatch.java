package com.ipiecole.batch.csvImport;


import com.ipiecole.batch.dto.CommuneCSV;
import com.ipiecole.batch.exception.CommuneCSVException;
import com.ipiecole.batch.exception.NetworkException;
import com.ipiecole.batch.model.Commune;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.IncorrectTokenCountException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.backoff.FixedBackOffPolicy;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class CommunesImportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Value("${importFile.chunkSize}")
    private Integer chunkSize;

    @Bean
    public Step stepGetMissingCoordinates(){
        FixedBackOffPolicy policy = new FixedBackOffPolicy();
        policy.setBackOffPeriod(2000);
        return stepBuilderFactory.get("getMissingCoordinates")
                .<Commune, Commune> chunk(10)
                .reader(communesMissingCoordinatesJpaItemReader())
                .processor(communeMissingCoordinatesItemProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .retryLimit(5)
                .retry(NetworkException.class)
                .backOffPolicy(policy)
                .build();
    }

    @Bean
    public CommuneMissingCoordinatesItemProcessor communeMissingCoordinatesItemProcessor(){
        return new CommuneMissingCoordinatesItemProcessor();
    }

    @Bean
    public JpaPagingItemReader<Commune> communesMissingCoordinatesJpaItemReader(){
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("communesMissingCoordinatesJpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c where c.latitude is null or c.longitude is null")
                .build();
    }

    @Bean
    public JpaItemWriter<Commune> writerJPA(){
        return new JpaItemWriterBuilder<Commune>().entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Commune> writerJDBC(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Commune>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO COMMUNE(code_insee, nom, code_postal, latitude, longitude) " +
                        "VALUES (:codeinsee, :nom, :codepostal, :, :longitude)" +
                         "DUPLICATE KEY UPDATE (code_postal = code_postal, latitude = latitude, longitude = longitude)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public CommuneCSVItemProcessor communeCSVToCommuneProcessor() {
        return new CommuneCSVItemProcessor();
    }

    @Bean
    public StepExecutionListener communeCSVImportStepListener(){
        return new CommuneCSVImportStepListener();
    }

    @Bean
    public ChunkListener communeCSVImportChunkListener(){
        return new CommuneCSVImportChunkListener();
    }

    @Bean
    public ItemReadListener<CommuneCSV> communeCSVItemReadListener(){
        return new CommuneCSVItemListener();
    }
    @Bean
    public ItemWriteListener<Commune> communeCSVItemWriteListener(){
        return new CommuneCSVItemListener();
    }

    @Bean
    public CommunesCSVImportSkipListener communesCSVImportSkipListener(){
        return new CommunesCSVImportSkipListener();
    }

    @Bean
    public Step stepImportCSV(){
        return stepBuilderFactory.get("importFile")
                .<CommuneCSV, Commune> chunk(chunkSize)
                .reader(communesCSVItemReader())
                .processor(communeCSVToCommuneProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .skip(CommuneCSVException.class)
                .skip(FlatFileParseException.class)
                .listener(communesCSVImportSkipListener())
//                .listener(communeCSVImportStepListener())
//                .listener(communeCSVImportChunkListener())
//                .listener(communeCSVItemReadListener())
                .listener(communeCSVItemWriteListener())
                .listener(communeCSVToCommuneProcessor())
                .build();
    }

    @Bean
    public FlatFileItemReader<CommuneCSV> communesCSVItemReader(){
        return new FlatFileItemReaderBuilder<CommuneCSV>()
                .name("communesCSVItemReader")
                .linesToSkip(1)
                .resource(new ClassPathResource("laposte_hexasmal.csv"))
                .delimited()
                .delimiter(";")
                .names("codeInsee", "nom", "codePostal", "ligne5", "libelleAcheminement", "coordonneesGPS")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>(){{
                    setTargetType(CommuneCSV.class);
                }})
                .build();
    }

    @Bean
    public Step stepHelloWorld(){
        return stepBuilderFactory.get("stepHelloWorld")
                .tasklet(helloWorldTasklet())
                .listener(helloWorldTasklet())
                .build();
    }

    @Bean
    public Tasklet helloWorldTasklet(){
        return new HelloWorldTasklet();
    }


    @Bean
    public Job importCsvJob(Step stepHelloWorld, Step stepImportCSV, Step stepGetMissingCoordinates){
        return jobBuilderFactory.get("importCsvJob")
                .incrementer(new RunIdIncrementer())
                .flow(stepHelloWorld)
                .next(stepImportCSV)
                .on("COMPLETED_WITH_MISSING_COORDINATES").to(stepGetMissingCoordinates)
                .end().build();
    }
}