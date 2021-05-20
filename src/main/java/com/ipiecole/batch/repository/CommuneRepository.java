package com.ipiecole.batch.repository;

import com.ipiecole.batch.model.Commune;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface CommuneRepository extends JpaRepository<Commune, Long> {

    @Query("select count(distinct c.codePostal) from Commune c")
    Long countDistinctCodePostal();

    @Query("select count(distinct c.nom) from Commune c")
    Long countDistinctNom();

}
