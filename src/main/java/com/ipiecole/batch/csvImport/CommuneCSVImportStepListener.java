package com.ipiecole.batch.csvImport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class CommuneCSVImportStepListener implements StepExecutionListener {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before Step CSV Import");
    }
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("After Step CSV Import");
        logger.info(stepExecution.getSummary());
        return ExitStatus.COMPLETED;
    }
}