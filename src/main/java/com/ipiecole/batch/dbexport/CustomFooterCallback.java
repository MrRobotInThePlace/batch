package com.ipiecole.batch.dbexport;

import com.ipiecole.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import java.io.IOException;
import java.io.Writer;


public class CustomFooterCallback implements FlatFileFooterCallback {

    private final CommuneRepository communeRepository;

    public CustomFooterCallback(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }

    // récupération de la query définit dans "CommuneRepository"
    @Override
    public void writeFooter(Writer writer) throws IOException {
      writer.write("Total communes : " + communeRepository.countDistinctNom());
   }

}