package com.ipiecole.batch.csvImport;

import com.ipiecole.batch.dto.CommuneCSV;
import com.ipiecole.batch.exception.CommuneCSVException;
import com.ipiecole.batch.model.Commune;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterProcess;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeProcess;
import org.springframework.batch.core.annotation.OnProcessError;
import org.springframework.batch.item.ItemProcessor;


//  C'est une interface prenant deux types en paramètres,
//  le premier correspond à la classe d'entrée (la même que l'ItemReader), => ici correspondant au format "Commune CSV"
//  le second à la classe de sortie (la même que l'ItemWriter) => ici correspondant au format "Commune"
public class CommuneCSVItemProcessor implements ItemProcessor<CommuneCSV, Commune> {

    private Integer nbCommunesWithoutCoordinates = 0;

    @Override
    public Commune process(CommuneCSV item) throws Exception {
        Commune commune = new Commune();
        validateCommuneCSV(item);
        commune.setCodeInsee(item.getCodeInsee());
        commune.setCodePostal(item.getCodePostal());
        //Majuscule première lettre de chaque terme
        String nomCommune = WordUtils.capitalizeFully(item.getNom());
        //Proprification du nom
        nomCommune = nomCommune.replaceAll("^L ", "L'");
        nomCommune = nomCommune.replaceAll(" L ", " L'");
        nomCommune = nomCommune.replaceAll("^D ", "D'");
        nomCommune = nomCommune.replaceAll(" D ", " D'");
        nomCommune = nomCommune.replaceAll("^St ", "Saint ");
        nomCommune = nomCommune.replaceAll(" St ", " Saint ");
        nomCommune = nomCommune.replaceAll("^Ste ", "Sainte ");
        nomCommune = nomCommune.replaceAll(" Sainte ", " Sainte ");
        commune.setNom(nomCommune);
        //Latitude/Longitude
        String[] coordonnees = item.getCoordonneesGps().split(",");
        if(coordonnees.length == 2){
            commune.setLatitude(Double.valueOf(coordonnees[0]));
            commune.setLongitude(Double.valueOf(coordonnees[1]));
        }
        return commune;
    }

    private void validateCommuneCSV(CommuneCSV item) throws CommuneCSVException {
        //Contrôler Code INSEE 5 chiffres
        if(item.getCodeInsee() != null && !item.getCodeInsee().matches("^[0-9AB]{5}$")){
            throw new CommuneCSVException("Le code Insee ne contient pas 5 chiffres");
        }
        //Contrôler Code postal 5 chiffres
        if(item.getCodePostal() != null && !item.getCodePostal().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code Postal ne contient pas 5 chiffres");
        }
        //Contrôler nom de la communes lettres en majuscules, espaces, tirets, et apostrophes
        if(item.getNom() != null && !item.getNom().matches("^[A-Z-' ]+$")){
            throw new CommuneCSVException("Le nom de la commune n'est pas composé uniquement de lettres, espaces et tirets");
        }
        //Contrôler les coordonnées GPS
        if(item.getCoordonneesGps() != null && !item.getCoordonneesGps().matches("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$")){
            //+1 sur le nombre de communes où il manque des coordonnées
            nbCommunesWithoutCoordinates++;
            throw new CommuneCSVException("Les coordonnées GPS sont incorrectes ! " + item.getCoordonneesGps());
        }
    }

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("After Step CSV Import");
        logger.info(stepExecution.getSummary());
        if(nbCommunesWithoutCoordinates > 0){
            return new ExitStatus("COMPLETED_WITH_MISSING_COORDINATES");
        }
        return ExitStatus.COMPLETED;
    }
    @BeforeProcess
    public void beforeProcess(CommuneCSV input){
        logger.info("Before Process => " + input.toString());
    }
    @AfterProcess
    public void afterProcess(CommuneCSV input, Commune output){
        logger.info("After Process => " + input.toString() + " => " + output.toString());
    }
    @OnProcessError
    public void onProcessError(CommuneCSV input, Exception ex){
        logger.error("Error Process => " + input.toString() + " => " + ex.getMessage());
    }
}