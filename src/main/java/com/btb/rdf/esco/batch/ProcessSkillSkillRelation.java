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
public class ProcessSkillSkillRelation {

    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationConfig config;
    private final PlatformTransactionManager tm;

    @Bean("ProcessSkillSkillRelation.step")
    public Step step() {
        return this.stepBuilderFactory.get("Skill Skill relations")
                .transactionManager(tm)
                .<SkillSkillRelation, SkillSkillRelation>chunk(config.getChunkSize())
                .reader(skillSkillRelationFlatFileItemReader())
                .writer(skillSkillRelationItemWriter())
                .listener(new StepChunkListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<SkillSkillRelation> skillSkillRelationFlatFileItemReader() {
        final String[] fields = new String[]{"originalSkillUri", "originalSkillType", "relationType", "relatedSkillType", "relatedSkillUri"};

        return new FlatFileItemReaderBuilder<SkillSkillRelation>()
                .name("ProcessSkillSkillRelation Reader")
                .resource(new ClassPathResource("skillSkillRelations.csv"))
                .linesToSkip(1) // skip header
                .recordSeparatorPolicy(new SeparatorPolicy(fields.length))
                .delimited()
                .names(fields)
                .targetType(SkillSkillRelation.class)
                .maxItemCount(config.getMaxCount())
                .build();
    }

    @Bean
    public ItemWriter<SkillSkillRelation> skillSkillRelationItemWriter() {
        // TODO
        return items -> {
        };
    }

    @Data
    public static class SkillSkillRelation {
        private String originalSkillUri;
        private String originalSkillType;
        private String relationType;
        private String relatedSkillType;
        private String relatedSkillUri;
    }
}
