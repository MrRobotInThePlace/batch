package com.ipiecole.batch.dto;

public class CommuneCSV {
    private String codeInsee;
    private String nom;
    private String codePostal;
    private String ligne5;
    private String libelleAcheminement;
    private String coordonneesGps;
    public String getCodeInsee() {
        return codeInsee;
    }
    public void setCodeInsee(String codeInsee) {
        this.codeInsee = codeInsee;
    }
    public String getNom() {
        return nom;
    }
    public void setNom(String nom) {
        this.nom = nom;
    }
    public String getCodePostal() {
        return codePostal;
    }
    public void setCodePostal(String codePostal) {
        this.codePostal = codePostal;
    }
    public String getLigne5() {
        return ligne5;
    }
    public void setLigne5(String ligne5) {
        this.ligne5 = ligne5;
    }
    public String getLibelleAcheminement() {
        return libelleAcheminement;
    }
    public void setLibelleAcheminement(String libelleAcheminement) {
        this.libelleAcheminement = libelleAcheminement;
    }
    public String getCoordonneesGps() {
        return coordonneesGps;
    }
    public void setCoordonneesGps(String coordonneesGps) {
        this.coordonneesGps = coordonneesGps;
    }

    @Override
    public String toString() {
        return "CommuneCSV{" +
                "codeInsee='" + codeInsee + '\'' +
                ", nom='" + nom + '\'' +
                ", codePostal='" + codePostal + '\'' +
                ", ligne5='" + ligne5 + '\'' +
                ", libelleAcheminement='" + libelleAcheminement + '\'' +
                ", coordonneesGps='" + coordonneesGps + '\'' +
                '}';
    }
}