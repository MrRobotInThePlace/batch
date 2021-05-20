package com.ipiecole.batch;

import com.ipiecole.batch.model.Commune;
import com.ipiecole.batch.repository.CommuneRepository;
import com.ipiecole.batch.utils.BatchTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.test.AssertFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import java.io.File;
import java.util.Date;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class CommunesDBExportBatchIntegrationTest extends BatchTest {

    @Autowired
    @Qualifier("exportCommunes")
    private Job exportCommunes;

    @Autowired
    private CommuneRepository communeRepository;

    @BeforeEach
    @After
    public void setupAndTeardown(){
        communeRepository.deleteAll();
        this.initializeJobLauncherTestUtils(exportCommunes);
    }

    @Test
    public void testSimpleJobOk() throws Exception {
        //Given
        JobParametersBuilder paramsBuilder = new JobParametersBuilder();
        paramsBuilder.addDate("date", new Date());
        paramsBuilder.addString("filePath", "target/test.txt");
        JobParameters jobParameters = paramsBuilder.toJobParameters();
        communeRepository.save(new Commune("01006", "Saint Ambleon", "01300", 45.7494989044, 5.59432017366));
        communeRepository.save(new Commune("01454", "Virignin", "01300", 45.7267387762, 5.71282330936));
        communeRepository.save(new Commune("07024", "Banne", "07460", 44.3607782702, 4.15113804507));
        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        JobInstance actualJobInstance = jobExecution.getJobInstance();
        ExitStatus actualJobExitStatus = jobExecution.getExitStatus();
        // then
        Assert.assertEquals("exportCommunes", actualJobInstance.getJobName());
        Assert.assertEquals(ExitStatus.COMPLETED, actualJobExitStatus);
        AssertFile.assertFileEquals(new File("src/test/ressources/laposte_out_test.txt"), new File("target/test.txt"));
    }
}