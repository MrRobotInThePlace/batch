package com.ipiecole.batch.dbexport;

import com.ipiecole.batch.model.Commune;
import com.ipiecole.batch.repository.CommuneReaderRepository;
import com.ipiecole.batch.repository.CommuneRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import javax.persistence.EntityManagerFactory;
import java.sql.SQLException;
import java.util.*;


@Configuration
@EnableBatchProcessing
public class CommunesDBExportBatch<AggregateItem> {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    public CommuneRepository communeRepository;

    @Autowired
    public CommuneReaderRepository communeReaderRepository;

    // Correspond au pas de commit
    @Value("${importFile.chunkSize}")
    private Integer chunkSize;


    //////////////////////////////////////////
    ////////////// TASKLETS //////////////////
    //////////////////////////////////////////

    //  Contrôle de l'exécution de l'export
    @Bean
    public Tasklet communesExportTasklet(){ return new CommunesExportTasklet();
    }


    /////////////////////////////////////////////
    ////////////// ITEM READER //////////////////
    /////////////////////////////////////////////

    // Lecture des données de la BDD et ordonnancement des données en fonction du code postal puis du code Insee
    @Bean
    @Qualifier("repositoryItemReaderWithParams")
    public RepositoryItemReader<Commune> repositoryItemReaderWithParams() {

        RepositoryItemReader<Commune> repositoryItemReader = new RepositoryItemReader<Commune>();
        repositoryItemReader.setRepository(communeReaderRepository);
        repositoryItemReader.setMethodName("findAll");
        List<Commune> list = new ArrayList<Commune>();
        repositoryItemReader.setArguments(list);
        repositoryItemReader.setPageSize(40);
        HashMap<String, Sort.Direction> sorts = new HashMap<>();
        sorts.put("codePostal", Sort.Direction.ASC);
        sorts.put("codeInsee", Sort.Direction.ASC);
        repositoryItemReader.setSort(sorts);
        return repositoryItemReader;
    }

    /* 2ème méthode possible avec le JpaPagingItemReader
    @Bean
    public JpaPagingItemReader<Commune> repositoryItemReaderWithParams2() {
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("repositoryItemReaderWithParams")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c order by code_postal, code_insee")
                .build();
    } */


    /////////////////////////////////////////////
    ////////////// ITEM WRITER //////////////////
    /////////////////////////////////////////////

    // Ecriture du fichier txt en sortie
    @Bean
    @StepScope
    public FlatFileItemWriter<Commune> flatFileItemWriter() {
        BeanWrapperFieldExtractor<Commune> bwfe = new BeanWrapperFieldExtractor<Commune>();
        bwfe.setNames(new String[]{"codePostal", "codeInsee", "nom", "latitude", "longitude"});

        // Formatage des données (ajout de tirets entre chaque donnée sauf entre la commune et ses coordonnées GPS
        // Formatage des coordonnées GPS limité à 5 chiffres après la virgule
        FormatterLineAggregator<Commune> agg = new FormatterLineAggregator<Commune>();
        agg.setFormat("%5s - %5s - %s : %.5f %.5f");
        agg.setFieldExtractor(bwfe);

        FlatFileItemWriter<Commune> flatFileItemWriter =
                new FlatFileItemWriter<>();
        flatFileItemWriter.setName("txtWriter");

        flatFileItemWriter.setResource(
                new FileSystemResource("target/test.txt"));
        flatFileItemWriter.setLineAggregator(agg);

        // Ecriture de l'header et du footer
        flatFileItemWriter.setHeaderCallback(new CustomHeaderCallback(communeRepository));
        flatFileItemWriter.setFooterCallback(new CustomFooterCallback(communeRepository));

        return flatFileItemWriter;
    }


    //////////////////////////////////////////
    ////////////// LISTENER //////////////////
    //////////////////////////////////////////

    // SkipListener : Intervenir lorsqu'un élément a été ignoré
    // Le SkipListener définit les méthodes onSkipInRead, onSkipInProcess, onSkipInWrite
    @Bean
    public CommunesDBExportSkipListener communesDBExportSkipListener() {
        return new CommunesDBExportSkipListener();
    }


    /////////////////////////////////////////////////////////
    ////////////// STEP (FLOT D'EXECUTION) //////////////////
    /////////////////////////////////////////////////////////

    @Bean
    public Step stepExportTasklet(){
        return stepBuilderFactory.get("stepExportTasklet")
                .tasklet(communesExportTasklet())
                .listener(communesExportTasklet())
                .build();
    }

    @Bean
    public Step stepExport() {
        return stepBuilderFactory.get("exportFile")
                .<Commune, Commune>chunk(chunkSize)
                .reader(repositoryItemReaderWithParams())
                .writer(flatFileItemWriter())
                .listener(communesDBExportSkipListener())

                // gestion erreur => relance du traitement en cas d'indisponibilité limitée à 3 fois ici
                .faultTolerant()
                .retryLimit(3)
                .retry(SQLException.class)

                // gestion erreur => ignorer les élément levant une exception limité ici à 10. Une fois ce seuil atteint la step tombe en erreur
                .skipLimit(10)
                .skip(FlatFileParseException.class)

                .build();
    }


    //////////////////////////////////////
    ////////////// JOB  //////////////////
    //////////////////////////////////////

    // Ordonnancement des steps
    @Bean
    @Qualifier("exportCommunes")
    public Job exportCommunes(Step stepExport){
        return jobBuilderFactory.get("exportCommunes")
                .incrementer(new RunIdIncrementer())
                .flow(stepExportTasklet())
                .next(stepExport())
                .end().build();
    }
}
