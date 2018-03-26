package org.broadinstitute.hellbender.tools.spark.sv.discovery;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.EvidenceTargetLink;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.*;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.List;

import static org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection;


public final class SvDiscoveryInputData {

    public static final class InputMetaData {

        public final String sampleId;
        public final String nonCanonicalChromosomeNamesFile;
        public final DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection discoverStageArgs;

        public final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast;
        public final List<SVInterval> assembledIntervals;
        public final PairedStrandedIntervalTree<EvidenceTargetLink> evidenceTargetLinks;
        public final ReadMetadata readMetadata;

        public final Broadcast<SAMFileHeader> headerBroadcast;
        public final Broadcast<ReferenceMultiSource> referenceBroadcast;
        public final Broadcast<SAMSequenceDictionary> referenceSequenceDictionaryBroadcast;

        public final Logger toolLogger;

        InputMetaData(final String sampleId, final String nonCanonicalChromosomeNamesFile,
                      final DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection discoverStageArgs,
                      final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast,
                      final List<SVInterval> assembledIntervals,
                      final PairedStrandedIntervalTree<EvidenceTargetLink> evidenceTargetLinks,
                      final ReadMetadata readMetadata,
                      final Broadcast<SAMFileHeader> headerBroadcast,
                      final Broadcast<ReferenceMultiSource> referenceBroadcast,
                      final Broadcast<SAMSequenceDictionary> referenceSequenceDictionaryBroadcast,
                      final Logger toolLogger) {

            Utils.validate(! (evidenceTargetLinks != null && readMetadata == null),
                    "Must supply read metadata when incorporating evidence target links");

            this.sampleId = sampleId;
            this.nonCanonicalChromosomeNamesFile = nonCanonicalChromosomeNamesFile;
            this.discoverStageArgs = discoverStageArgs;
            this.cnvCallsBroadcast = cnvCallsBroadcast;
            this.assembledIntervals = assembledIntervals;
            this.evidenceTargetLinks = evidenceTargetLinks;
            this.readMetadata = readMetadata;
            this.headerBroadcast = headerBroadcast;
            this.referenceBroadcast = referenceBroadcast;
            this.referenceSequenceDictionaryBroadcast = referenceSequenceDictionaryBroadcast;
            this.toolLogger = toolLogger;
        }
    }

    public final InputMetaData inputMetaData;

    public final JavaRDD<GATKRead> assemblyRawAlignments;

    public String outputPath;

    public SvDiscoveryInputData(final JavaSparkContext ctx,
                                final DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection discoverStageArgs,
                                final String nonCanonicalChromosomeNamesFile,
                                final String outputPath,
                                final ReadMetadata readMetadata,
                                final List<SVInterval> assembledIntervals,
                                final PairedStrandedIntervalTree<EvidenceTargetLink> evidenceTargetLinks,
                                final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast,
                                final JavaRDD<GATKRead> reads,
                                final SAMFileHeader headerForReads,
                                final ReferenceMultiSource reference,
                                final Logger toolLogger) {

        this(SVUtils.getSampleId(headerForReads), discoverStageArgs, nonCanonicalChromosomeNamesFile, outputPath, readMetadata,
                assembledIntervals, evidenceTargetLinks, reads, toolLogger,
                ctx.broadcast(reference), ctx.broadcast(headerForReads.getSequenceDictionary()), ctx.broadcast(headerForReads),
                cnvCallsBroadcast);
    }

    public SvDiscoveryInputData(final String sampleId,
                                final DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection discoverStageArgs,
                                final String nonCanonicalChromosomeNamesFile,
                                final String outputPath,
                                final ReadMetadata readMetadata,
                                final List<SVInterval> assembledIntervals,
                                final PairedStrandedIntervalTree<EvidenceTargetLink> evidenceTargetLinks,
                                final JavaRDD<GATKRead> reads,
                                final Logger toolLogger,
                                final Broadcast<ReferenceMultiSource> referenceBroadcast,
                                final Broadcast<SAMSequenceDictionary> referenceSequenceDictionaryBroadcast,
                                final Broadcast<SAMFileHeader> headerBroadcast,
                                final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast) {

        this(new InputMetaData(sampleId, nonCanonicalChromosomeNamesFile, discoverStageArgs,
                cnvCallsBroadcast, assembledIntervals, evidenceTargetLinks, readMetadata,
                headerBroadcast, referenceBroadcast, referenceSequenceDictionaryBroadcast, toolLogger),
                reads, outputPath);
    }

    public SvDiscoveryInputData(final InputMetaData inputMetaData, final JavaRDD<GATKRead> assemblyRawAlignments, final String outputPath) {

        this.inputMetaData = inputMetaData;

        this.assemblyRawAlignments = assemblyRawAlignments;

        this.outputPath = outputPath;
    }

    public void updateOutputPath(final String newOutputPath) {
        outputPath = newOutputPath;
    }

}
