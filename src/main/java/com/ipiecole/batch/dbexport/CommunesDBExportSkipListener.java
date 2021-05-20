package com.ipiecole.batch.dbexport;

import com.ipiecole.batch.dto.CommuneCSV;
import com.ipiecole.batch.model.Commune;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

public class CommunesDBExportSkipListener implements SkipListener<CommuneCSV, Commune> {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    // fournis l'implementation de cette méthode pour écouter les évènements sautés pendant l'export
    @Override
    public void onSkipInRead(Throwable t) {
        logger.warn("Skip in Read => " + t.getMessage());
    }

    // fournis l'implementation de cette méthode pour écouter les évènements sautés pendant l'écriture d'un enregistrement
    @Override
    public void onSkipInWrite(Commune item, Throwable t) {
        logger.warn("Skip in Write => " + item.toString() + ", " + t.getMessage());
    }

    // permet de lister les évènements sautés pendant le traitement d'export
    @Override
    public void onSkipInProcess(CommuneCSV item, Throwable t) {
        logger.warn("Skip in Process => " + item.toString() + ", " + t.getMessage());
    }
}

