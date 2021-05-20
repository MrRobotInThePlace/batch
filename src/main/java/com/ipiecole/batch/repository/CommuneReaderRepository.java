package com.ipiecole.batch.repository;

import com.ipiecole.batch.model.Commune;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface CommuneReaderRepository extends PagingAndSortingRepository<Commune, Long> {

    @Override
    Page<Commune> findAll(Pageable pageable);

}
