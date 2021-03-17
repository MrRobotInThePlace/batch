package com.ipiecole.batch.dbexport;

import com.ipiecole.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import java.io.IOException;
import java.io.Writer;

public class CustomHeaderCallback implements FlatFileHeaderCallback {

    private final CommuneRepository communeRepository;

    public CustomHeaderCallback(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }

    // récupération de la query définit dans "CommuneRepository"
    @Override
    public void writeHeader(Writer writer) throws IOException {
        writer.write("Total codes postaux : " + communeRepository.countDistinctCodePostal());
    }

}