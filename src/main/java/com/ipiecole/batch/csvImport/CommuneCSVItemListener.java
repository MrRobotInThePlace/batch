package com.ipiecole.batch.csvImport;

import com.ipiecole.batch.dto.CommuneCSV;
import com.ipiecole.batch.model.Commune;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;

import java.util.List;

public class CommuneCSVItemListener implements ItemReadListener<CommuneCSV>, ItemWriteListener<Commune> {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    public void beforeRead() {
        logger.info("Before Read CSV Import");
    }
    @Override
    public void afterRead(CommuneCSV item) {
        logger.info("After Read CSV Import => " + item.toString());
    }
    @Override
    public void onReadError(Exception ex) {
        logger.error("On Read Error CSV Import => " + ex.getMessage());
    }
    @Override
    public void beforeWrite(List<? extends Commune> items) {
        logger.info("Before Write CSV Import => " + items.toString());
    }
    @Override
    public void afterWrite(List<? extends Commune> items) {
        logger.info("After Write CSV Import => " + items.toString());
    }
    @Override
    public void onWriteError(Exception exception, List<? extends Commune> items) {
        logger.error("On Write Error CSV Import => " + exception.getMessage() + " " + items.toString());
    }
}