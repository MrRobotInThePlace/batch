package com.ipiecole.batch.utils;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class BatchTest {
    @Autowired
    protected JobLauncher jobLauncher;

    @Autowired
    protected JobRepository jobRepository;

    protected JobLauncherTestUtils jobLauncherTestUtils;

    protected void initializeJobLauncherTestUtils(Job job) {
        this.jobLauncherTestUtils = new JobLauncherTestUtils();
        this.jobLauncherTestUtils.setJobLauncher(jobLauncher);
        this.jobLauncherTestUtils.setJobRepository(jobRepository);
        this.jobLauncherTestUtils.setJob(job);
    }
}
