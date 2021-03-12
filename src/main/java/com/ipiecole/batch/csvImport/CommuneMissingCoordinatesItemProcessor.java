package com.ipiecole.batch.csvImport;

import com.ipiecole.batch.model.Commune;
import com.ipiecole.batch.utils.OpenStreetMapUtils;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class CommuneMissingCoordinatesItemProcessor implements ItemProcessor<Commune, Commune> {
    @Override
    public Commune process(Commune item) throws Exception {

        Map<String, Double> coordinatesOSM = OpenStreetMapUtils.getInstance().getCoordinates(
                item.getNom() + " " + item.getCodePostal());

        if(coordinatesOSM != null && coordinatesOSM.size() == 2) {
            item.setLongitude(coordinatesOSM.get("lon"));
            item.setLatitude(coordinatesOSM.get("lat"));
            return item;
        }
        return null;
    }
}
