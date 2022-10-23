package com.btb.rdf.esco.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;

@Slf4j
public class StepChunkListener implements ChunkListener {

    @Override
    public void beforeChunk(ChunkContext context) {
        final StepExecution stepExecution = context.getStepContext().getStepExecution();
        log.info("Called beforeChunk({}) : {}", stepExecution.getStepName(), stepExecution);
    }

    @Override
    public void afterChunk(ChunkContext context) {
        // log.info("Called afterChunk({})", context.getStepContext().getStepExecution().getStepName());
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        log.info("Called afterChunkError() : {}", context);
    }
}
