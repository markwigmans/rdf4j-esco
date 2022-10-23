package com.btb.rdf.esco.batch;

import com.btb.rdf.esco.config.ApplicationConfig;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
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
public class ProcessSkills {

    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationConfig config;
    private final PlatformTransactionManager tm;
    private final Repository repository;

    @Bean("ProcessSkills.step")
    public Step step() {
        return this.stepBuilderFactory.get("Skills")
                .transactionManager(tm)
                .<Skill, Skill>chunk(config.getChunkSize())
                .reader(skillFlatFileItemReader())
                .writer(skillItemWriter())
                .listener(new StepChunkListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<Skill> skillFlatFileItemReader() {
        final String[] fields = new String[]{"conceptType", "conceptUri", "skillType", "reuseLevel", "preferredLabel", "altLabels", "hiddenLabels", "status", "modifiedDate", "scopeNote", "definition", "inScheme", "description"};

        return new FlatFileItemReaderBuilder<Skill>()
                .name("ProcessSkills Reader")
                .resource(new ClassPathResource("csv/skills_nl.csv"))
                .linesToSkip(1) // skip header
                .recordSeparatorPolicy(new SeparatorPolicy(fields.length))
                .delimited()
                .names(fields)
                .targetType(Skill.class)
                .build();
    }

    @Bean
    public ItemWriter<Skill> skillItemWriter() {
        RepositoryConnection con = repository.getConnection();
        return items -> {
            items.forEach(item -> {
                ModelBuilder builder = new ModelBuilder();
                builder.setNamespace("esco", "http://data.europa.eu/esco/model#");
                builder.subject(item.getConceptUri());
                builder.add("esco:skillType", item.getSkillType());
                builder.add("esco:skillReuseLevel", item.getReuseLevel());
                builder.add("esco:NodeLiteral", item.getDescription());
                Model model = builder.build();
                Rio.write(model, System.out, RDFFormat.TURTLE);
                con.add(model);
            });
        };
    }

    @Data
    public static class Skill {

        private String altLabels;
        private String conceptType;
        private String conceptUri;
        private String definition;
        private String description;
        private String hiddenLabels;
        private String inScheme;
        private String modifiedDate;
        private String preferredLabel;
        private String reuseLevel;
        private String scopeNote;
        private String skillType;
        private String status;
    }
}
