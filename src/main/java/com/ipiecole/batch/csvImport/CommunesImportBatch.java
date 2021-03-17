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

    // Correspond au pas de commit
    @Value("${importFile.chunkSize}")
    private Integer chunkSize;


    /////////////////////////////////////////////
    ////////////// ITEM READER //////////////////
    /////////////////////////////////////////////

    // Les ItemReader sont les éléments permettant la lecture par lot de données issues de sources diverses (fichiers plats, XML, bases de données...)
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

    // Cet ItemReader lis les données à partir de la base de données => ici on récupères les infos n'ayant pas de coordonnées GPS
    @Bean
    public JpaPagingItemReader<Commune> communesMissingCoordinatesJpaItemReader(){
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("communesMissingCoordinatesJpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c where c.latitude is null or c.longitude is null")
                .build();
    }


    ////////////////////////////////////////////////
    ////////////// ITEM PROCESSOR //////////////////
    ////////////////////////////////////////////////

    // Chaque élément lu par l'ItemReader est envoyé à l'ItemProcessor configuré ensuite au niveau de la step
    // Le type de sortie de l'ItemReader doit être compatible avec le type d'entrée de l'ItemProcessor (ou de l'ItemWriter)
    // Les Item Processor étant des traitement unitaire, attention à ne pas faire d'opérations coûteuses dans la méthode process (comme des requêtes BDD par exemple)
    // Préférer dans ce cas des opérations ensemblistes (cf. Tasklet)
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

    // l'Item Processor transforme les données afin qu'elles correspondent au format des données à écrire
    @Bean
    public CommuneMissingCoordinatesItemProcessor communeMissingCoordinatesItemProcessor(){
        return new CommuneMissingCoordinatesItemProcessor();
    }

    // Cet Item Processor formate les données du fichiers d'entrée
    @Bean
    public CommuneCSVItemProcessor communeCSVToCommuneProcessor() {
        return new CommuneCSVItemProcessor();
    }


    /////////////////////////////////////////////
    ////////////// ITEM WRITER //////////////////
    /////////////////////////////////////////////

    // l'ItemWriter permet d'écrire par lot les données envoyées depuis l'ItemReader (éventuellement via l'ItemProcessor)
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


    //////////////////////////////////////////
    ////////////// TASKLETS //////////////////
    //////////////////////////////////////////

    //  Les Tasklets permettent de réaliser tout type de traitements unitaires (exécution de requêtes SQL par exemple)
    @Bean
    public Tasklet helloWorldTasklet(){
        return new HelloWorldTasklet();
    }


    //////////////////////////////////////////
    ////////////// LISTENER //////////////////
    //////////////////////////////////////////

    // Les Listeners permettent d'exécuter des instructions à des moments clés du batch

    // StepExecution Listener : Intervenir avant ou après l'exécution d'une Step
    // Le StepExecutionListener définit une méthode beforeStep et afterStep
    // Permet de journaliser le compte-rendu d'exécution de la Step
    @Bean
    public StepExecutionListener communeCSVImportStepListener(){
        return new CommuneCSVImportStepListener();
    }

    // ChunkListener : Intervenir avant ou après chaque Chunk
    // Le ChunkListener définit une méthode beforeChunk et afterChunk
    // Permet de journaliser l'avancement du batch
    @Bean
    public ChunkListener communeCSVImportChunkListener(){
        return new CommuneCSVImportChunkListener();
    }

    // ItemReadListener : Intervenir avant ou après une lecture, ou lors d'une erreur de lecture
    // L'Item Read Listener définit les méthodes before*, after* et on*Error avec * = Read, Write, ou Process
    @Bean
    public ItemReadListener<CommuneCSV> communeCSVItemReadListener(){
        return new CommuneCSVItemListener();
    }

    // ItemProcessListener : Intervenir avant ou après un traitement, ou lors d'une erreur de traitement

    // ItemWriteListener : Intervenir avant ou après une écriture, ou lors d'une erreur d'écriture
    // L'Item Write Listener définit les méthodes before*, after* et on*Error avec * = Read, Write, ou Process
    @Bean
    public ItemWriteListener<Commune> communeCSVItemWriteListener(){
        return new CommuneCSVItemListener();
    }

    // SkipListener : Intervenir lorsqu'un élément a été ignoré
    // Le SkipListener définit les méthodes onSkipInRead, onSkipInProcess, onSkipInWrite
    @Bean
    public CommunesCSVImportSkipListener communesCSVImportSkipListener(){
        return new CommunesCSVImportSkipListener();
    }


    /////////////////////////////////////////////////////////
    ////////////// STEP (FLOT D'EXECUTION) //////////////////
    /////////////////////////////////////////////////////////

    // Le plus simple moyen d'ordonnancer les Steps consiste à les enchaîner de manière séquentielle
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
    public Step stepHelloWorld(){
        return stepBuilderFactory.get("stepHelloWorld")
                .tasklet(helloWorldTasklet())
                .listener(helloWorldTasklet())
                .build();
    }

    //////////////////////////////////////
    ////////////// JOB  //////////////////
    //////////////////////////////////////

    // Ordonnancement des Step

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