package com.btb.rdf.esco.batch;

import com.btb.rdf.esco.config.ApplicationConfig;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ProcessOccupationalSkillRelation {

    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationConfig config;
    private final PlatformTransactionManager tm;

    @Bean("ProcessOccupationalSkillRelation.step")
    public Step step() {
        return this.stepBuilderFactory.get("Occupational Skill relations")
                .transactionManager(tm)
                .<OccupationalSkillRelation, OccupationalSkillRelation>chunk(config.getChunkSize())
                .reader(occupationalSkillRelationFlatFileItemReader())
                .writer(occupationalSkillRelationItemWriter())
                .listener(new StepChunkListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<OccupationalSkillRelation> occupationalSkillRelationFlatFileItemReader() {
        final String[] fields = new String[]{"occupationUri", "relationType", "skillType", "skillUri"};

        return new FlatFileItemReaderBuilder<OccupationalSkillRelation>()
                .name("ProcessOccupationalSkillRelation Reader")
                .resource(new ClassPathResource("occupationSkillRelations.csv"))
                .linesToSkip(1) // skip header
                .recordSeparatorPolicy(new SeparatorPolicy(fields.length))
                .delimited()
                .names(fields)
                .targetType(OccupationalSkillRelation.class)
                .maxItemCount(config.getMaxCount())
                .build();
    }

    @Data
    public static class OccupationalSkillRelation {
        private String occupationUri;
        private String relationType;
        private String skillType;
        private String skillUri;
    }

    @Bean
    public ItemWriter<OccupationalSkillRelation> occupationalSkillRelationItemWriter() {
        // TODO
        return items -> {
        };
    }
}

