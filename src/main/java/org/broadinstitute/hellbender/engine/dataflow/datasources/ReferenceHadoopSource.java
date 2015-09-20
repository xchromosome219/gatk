package org.broadinstitute.hellbender.engine.dataflow.datasources;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.HadoopReferenceSequenceFileFactory;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import org.apache.hadoop.fs.Path;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class to load a reference sequence from a fasta file on HDFS.
 */
public class ReferenceHadoopSource implements ReferenceSource, Serializable {
    private static final long serialVersionUID = 1L;

    private final String referencePath;

    /**
     * @param referencePath the path to the reference file on HDFS
     */
    public ReferenceHadoopSource(final String referencePath) {
        this.referencePath = referencePath;
    }

    @Override
    public ReferenceBases getReferenceBases(final PipelineOptions pipelineOptions, final SimpleInterval interval) throws IOException {
        ReferenceSequenceFile referenceSequenceFile = HadoopReferenceSequenceFileFactory.getReferenceSequenceFile(new Path(referencePath));
        ReferenceSequence sequence = referenceSequenceFile.getSubsequenceAt(interval.getContig(), interval.getStart(), interval.getEnd());
        return new ReferenceBases(sequence.getBases(), interval);
    }

    @Override
    public Map<String, ReferenceBases> getAllReferenceBases() throws IOException {
        ReferenceSequenceFile referenceSequenceFile = HadoopReferenceSequenceFileFactory.getReferenceSequenceFile(new Path(referencePath));
        Map<String, ReferenceBases> bases = new LinkedHashMap<>();
        ReferenceSequence seq;
        while ((seq = referenceSequenceFile.nextSequence()) != null) {
            String name = seq.getName();
            bases.put(name, new ReferenceBases(seq.getBases(), new SimpleInterval(name, 1, seq.length())));
        }
        return bases;
    }

    @Override
    public SAMSequenceDictionary getReferenceSequenceDictionary(final SAMSequenceDictionary optReadSequenceDictionaryToMatch) throws IOException {
        ReferenceSequenceFile referenceSequenceFile = HadoopReferenceSequenceFileFactory.getReferenceSequenceFile(new Path(referencePath));
        return referenceSequenceFile.getSequenceDictionary();
    }

}
