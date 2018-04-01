package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.*;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.AnnotatedVariantProducer;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvDiscoverFromLocalAssemblyContigAlignmentsSpark;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvDiscoveryInputData;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignedContig;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignedContigGenerator;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AssemblyContigWithFineTunedAlignments;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.AssemblyContigAlignmentSignatureClassifier;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.ImpreciseVariantDetector;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.AlignedAssemblyOrExcuse;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.EvidenceTargetLink;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.FindBreakpointEvidenceSpark;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.*;
import org.broadinstitute.hellbender.utils.SequenceDictionaryUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection;
import static org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection.FindBreakpointEvidenceSparkArgumentCollection;

/**
 * Runs the structural variation discovery workflow on a single sample
 *
 * <p>This tool packages the algorithms described in {@link FindBreakpointEvidenceSpark} and
 * {@link org.broadinstitute.hellbender.tools.spark.sv.discovery.DiscoverVariantsFromContigAlignmentsSAMSpark}
 * as an integrated workflow.  Please consult the
 * descriptions of those tools for more details about the algorithms employed.  In brief, input reads are examined
 * for evidence of structural variation in a genomic region, regions so identified are locally assembled, and
 * the local assemblies are called for structural variation.</p>
 *
 * <h3>Inputs</h3>
 * <ul>
 *     <li>An input file of aligned reads.</li>
 *     <li>The reference to which the reads have been aligned.</li>
 *     <li>A BWA index image for the reference.
 *         You can use BwaMemIndexImageCreator to create the index image file.</li>
 *     <li>A list of ubiquitous kmers to ignore.
 *         You can use FindBadGenomicGenomicKmersSpark to create the list of kmers to ignore.</li>
 * </ul>
 *
 * <h3>Output</h3>
 * <ul>
 *     <li>A vcf file describing the discovered structural variants.</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 * <pre>
 *   gatk StructuralVariationDiscoveryPipelineSpark \
 *     -I input_reads.bam \
 *     -R reference.2bit \
 *     --aligner-index-image reference.img \
 *     --kmers-to-ignore ignored_kmers.txt \
 *     -O structural_variants.vcf
 * </pre>
 * <p>This tool can be run without explicitly specifying Spark options. That is to say, the given example command
 * without Spark options will run locally. See
 * <a href ="https://software.broadinstitute.org/gatk/documentation/article?id=10060">Tutorial#10060</a>
 * for an example of how to set up and run a Spark tool on a cloud Spark cluster.</p>
 *
 * <h3>Caveats</h3>
 * <p>Expected input is a paired-end, coordinate-sorted BAM with around 30x coverage.
 * Coverage much lower than that probably won't work well.</p>
 * <p>The reference is broadcast by Spark, and must therefore be a .2bit file due to current restrictions.</p>
 */
@DocumentedFeature
@BetaFeature
@CommandLineProgramProperties(
        oneLineSummary = "Runs the structural variation discovery workflow on a single sample",
        summary =
        "This tool packages the algorithms described in FindBreakpointEvidenceSpark and" +
        " DiscoverVariantsFromContigAlignmentsSAMSpark as an integrated workflow.  Please consult the" +
        " descriptions of those tools for more details about the algorithms employed.  In brief, input reads are examined" +
        " for evidence of structural variation in a genomic region, regions so identified are locally assembled, and" +
        " the local assemblies are called for structural variation.",
        programGroup = StructuralVariantDiscoveryProgramGroup.class)
public class StructuralVariationDiscoveryPipelineSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private final Logger localLogger = LogManager.getLogger(StructuralVariationDiscoveryPipelineSpark.class);
    @ArgumentCollection
    private final FindBreakpointEvidenceSparkArgumentCollection evidenceAndAssemblyArgs
            = new FindBreakpointEvidenceSparkArgumentCollection();
    @ArgumentCollection
    private final DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection discoverStageArgs
            = new DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection();

    @Argument(doc = "sam file for aligned contigs", fullName = "contig-sam-file")
    private String outputAssemblyAlignments;

    @Argument(doc = "directory for VCF output, including those from experimental interpretation tool if so requested, " +
            "will be created if not present; sample name will be appended after the provided argument",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME)
    private String variantsOutDir;

    @Advanced
    @Argument(doc = "flag to signal that user wants to run experimental interpretation tool as well",
            fullName = "exp-interpret", optional = true)
    private Boolean expInterpret = false;

    @Override
    public boolean requiresReads()
    {
        return true;
    }

    @Override
    public boolean requiresReference() {return true;}

    @Override
    protected void runTool( final JavaSparkContext ctx ) {

        Utils.validate(evidenceAndAssemblyArgs.externalEvidenceFile == null || discoverStageArgs.cnvCallsFile == null,
                "Please only specify one of externalEvidenceFile or cnvCallsFile");

        if (discoverStageArgs.cnvCallsFile != null) {
            evidenceAndAssemblyArgs.externalEvidenceFile = discoverStageArgs.cnvCallsFile;
        }

        JavaRDD<GATKRead> unfilteredReads = getUnfilteredReads();
        final SAMFileHeader headerForReads = getHeaderForReads();

        // gather evidence, run assembly, and align
        final FindBreakpointEvidenceSpark.AssembledEvidenceResults assembledEvidenceResults =
                FindBreakpointEvidenceSpark
                        .gatherEvidenceAndWriteContigSamFile(ctx,
                                evidenceAndAssemblyArgs,
                                headerForReads,
                                unfilteredReads,
                                outputAssemblyAlignments,
                                localLogger);

        // todo: when we call imprecise variants don't return here
        if (assembledEvidenceResults.getAlignedAssemblyOrExcuseList().isEmpty()) return;

        // parse the contig alignments and extract necessary information
        final JavaRDD<AlignedContig> parsedAlignments =
                new AlignedContigGenerator.InMemoryAlignmentParser(ctx, assembledEvidenceResults.getAlignedAssemblyOrExcuseList(), headerForReads)
                        .getAlignedContigs();

        // todo: when we call imprecise variants don't return here
        if(parsedAlignments.isEmpty()) return;

        final SvDiscoveryInputData svDiscoveryInputData = getSvDiscoveryInputData(ctx, headerForReads, assembledEvidenceResults);

        // TODO: 1/14/18 this is to be phased-out: old way of calling precise variants
        // assembled breakpoints
        @SuppressWarnings("deprecation")
        List<VariantContext> assemblyBasedVariants =
                org.broadinstitute.hellbender.tools.spark.sv.discovery.DiscoverVariantsFromContigAlignmentsSAMSpark
                        .discoverVariantsFromChimeras(svDiscoveryInputData, parsedAlignments);

        final List<VariantContext> annotatedVariants = processEvidenceTargetLinks(assemblyBasedVariants, svDiscoveryInputData);

        final String outputPath = svDiscoveryInputData.outputPath;
        final SAMSequenceDictionary refSeqDictionary = svDiscoveryInputData.referenceSequenceDictionaryBroadcast.getValue();
        final Logger toolLogger = svDiscoveryInputData.toolLogger;
        SVVCFWriter.writeVCF(annotatedVariants, outputPath + "inv_del_ins.vcf", refSeqDictionary, toolLogger);

        if ( expInterpret != null ) {
            experimentalInterpretation(ctx, assembledEvidenceResults, svDiscoveryInputData, evidenceAndAssemblyArgs.crossContigsToIgnoreFile);
        }

        {
            final JavaRDD<GATKRead> readsReloadedFromBam = svDiscoveryInputData.assemblyRawAlignments;

            final SvDiscoveryInputData svDiscoveryInputData_reloaded =
                    new SvDiscoveryInputData(svDiscoveryInputData.sampleId, svDiscoveryInputData.discoverStageArgs,
                            svDiscoveryInputData.outputPath + "experimentalInterpretation_",
                            svDiscoveryInputData.metadata, svDiscoveryInputData.assembledIntervals,
                            svDiscoveryInputData.evidenceTargetLinks,

                            readsReloadedFromBam,

                            svDiscoveryInputData.toolLogger, svDiscoveryInputData.referenceBroadcast,
                            svDiscoveryInputData.referenceSequenceDictionaryBroadcast, svDiscoveryInputData.headerBroadcast,
                            svDiscoveryInputData.cnvCallsBroadcast);

            final JavaRDD<AlignedContig> alignedContigs_reloaded = new AlignedContigGenerator
                    .SAMFormattedContigAlignmentParser(readsReloadedFromBam, headerForReads, true)
                    .getAlignedContigs();

            @SuppressWarnings("deprecation")
            List<VariantContext> master_again_variants =
                    org.broadinstitute.hellbender.tools.spark.sv.discovery.DiscoverVariantsFromContigAlignmentsSAMSpark
                            .discoverVariantsFromChimeras(svDiscoveryInputData, alignedContigs_reloaded);

            SvDiscoverFromLocalAssemblyContigAlignmentsSpark
                    .dispatchJobs(
                            SvDiscoverFromLocalAssemblyContigAlignmentsSpark
                                    .preprocess(svDiscoveryInputData_reloaded, evidenceAndAssemblyArgs.crossContigsToIgnoreFile, true),
                            svDiscoveryInputData_reloaded
                    );
        }
    }

    private SvDiscoveryInputData getSvDiscoveryInputData(final JavaSparkContext ctx,
                                                         final SAMFileHeader headerForReads,
                                                         final FindBreakpointEvidenceSpark.AssembledEvidenceResults assembledEvidenceResults) {
        final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast =
                broadcastCNVCalls(ctx, headerForReads, discoverStageArgs.cnvCallsFile);
        try {
            if ( !java.nio.file.Files.exists(Paths.get(variantsOutDir)) ) {
                IOUtils.createDirectory(variantsOutDir);
            }
        } catch (final IOException ioex) {
            throw new GATKException("Failed to create output directory " + variantsOutDir + " though it does not yet exist", ioex);
        }

        final String outputPrefixWithSampleName = variantsOutDir + (variantsOutDir.endsWith("/") ? "" : "/")
                                                    + SVUtils.getSampleId(headerForReads) + "_";

        final JavaRDD<GATKRead> reloadedContigAlignments = new ReadsSparkSource(ctx, readArguments.getReadValidationStringency())
                .getParallelReads(outputAssemblyAlignments, referenceArguments.getReferenceFileName(), null, bamPartitionSplitSize);

        return new SvDiscoveryInputData(ctx, discoverStageArgs, outputPrefixWithSampleName,
                assembledEvidenceResults.getReadMetadata(), assembledEvidenceResults.getAssembledIntervals(),
                makeEvidenceLinkTree(assembledEvidenceResults.getEvidenceTargetLinks()),
                cnvCallsBroadcast,
                reloadedContigAlignments,
                getHeaderForReads(), getReference(), localLogger);
    }

    /**
     * Uses the input EvidenceTargetLinks to
     *  <ul>
     *      <li>
     *          either annotate the variants called from assembly discovery with split read and read pair evidence, or
     *      </li>
     *      <li>
     *          to call new imprecise variants if the number of pieces of evidence exceeds a given threshold.
     *      </li>
     *  </ul>
     *
     */
    private static List<VariantContext> processEvidenceTargetLinks(List<VariantContext> assemblyBasedVariants,
                                                                   final SvDiscoveryInputData svDiscoveryInputData) {

        final List<VariantContext> annotatedVariants;
        if (svDiscoveryInputData.evidenceTargetLinks != null) {
            final PairedStrandedIntervalTree<EvidenceTargetLink> evidenceTargetLinks = svDiscoveryInputData.evidenceTargetLinks;
            final ReadMetadata metadata = svDiscoveryInputData.metadata;
            final ReferenceMultiSource reference = svDiscoveryInputData.referenceBroadcast.getValue();
            final DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection discoverStageArgs = svDiscoveryInputData.discoverStageArgs;
            final Logger toolLogger = svDiscoveryInputData.toolLogger;

            // annotate with evidence links
            annotatedVariants = AnnotatedVariantProducer.
                    annotateBreakpointBasedCallsWithImpreciseEvidenceLinks(assemblyBasedVariants,
                            evidenceTargetLinks, metadata, reference, discoverStageArgs, toolLogger);

            // then also imprecise deletion
            final List<VariantContext> impreciseVariants = ImpreciseVariantDetector.
                    callImpreciseDeletionFromEvidenceLinks(evidenceTargetLinks, metadata, reference,
                            discoverStageArgs.impreciseVariantEvidenceThreshold,
                            discoverStageArgs.maxCallableImpreciseVariantDeletionSize,
                            toolLogger);

            annotatedVariants.addAll(impreciseVariants);
        } else {
            annotatedVariants = assemblyBasedVariants;
        }

        return annotatedVariants;
    }

    // hook up prototyping breakpoint and type inference tool
    private void experimentalInterpretation(final JavaSparkContext ctx,
                                            final FindBreakpointEvidenceSpark.AssembledEvidenceResults assembledEvidenceResults,
                                            final SvDiscoveryInputData svDiscoveryInputData,
                                            final String nonCanonicalChromosomeNamesFile) {

        if ( ! expInterpret )
            return;

        final Broadcast<SAMSequenceDictionary> referenceSequenceDictionaryBroadcast = svDiscoveryInputData.referenceSequenceDictionaryBroadcast;
        final Broadcast<SAMFileHeader> headerBroadcast = svDiscoveryInputData.headerBroadcast;
        final SAMFileHeader headerForReads = headerBroadcast.getValue();
        final SAMReadGroupRecord contigAlignmentsReadGroup = new SAMReadGroupRecord(SVUtils.GATKSV_CONTIG_ALIGNMENTS_READ_GROUP_ID);
        final List<String> refNames = SequenceDictionaryUtils.getContigNamesList(referenceSequenceDictionaryBroadcast.getValue());

        List<GATKRead> readsList =
                assembledEvidenceResults
                        .getAlignedAssemblyOrExcuseList().stream()
                        .filter(AlignedAssemblyOrExcuse::isNotFailure)
                        .flatMap(aa -> aa.toSAMStreamForAlignmentsOfThisAssembly(headerForReads, refNames, contigAlignmentsReadGroup))
                        .map(SAMRecordToGATKReadAdapter::new)
                        .collect(Collectors.toList());
        JavaRDD<GATKRead> reads = ctx.parallelize(readsList);

        final String sampleId = svDiscoveryInputData.sampleId;
        final Broadcast<ReferenceMultiSource> referenceBroadcast = svDiscoveryInputData.referenceBroadcast;
        final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast = svDiscoveryInputData.cnvCallsBroadcast;

        final SvDiscoveryInputData updatedSvDiscoveryInputData =
                new SvDiscoveryInputData(sampleId, svDiscoveryInputData.discoverStageArgs,
                        svDiscoveryInputData.outputPath + "experimentalInterpretation_",
                        svDiscoveryInputData.metadata, svDiscoveryInputData.assembledIntervals,
                        svDiscoveryInputData.evidenceTargetLinks, reads, svDiscoveryInputData.toolLogger,
                        referenceBroadcast, referenceSequenceDictionaryBroadcast, headerBroadcast, cnvCallsBroadcast);

        EnumMap<AssemblyContigAlignmentSignatureClassifier.RawTypes, JavaRDD<AssemblyContigWithFineTunedAlignments>>
                contigsByPossibleRawTypes =
                SvDiscoverFromLocalAssemblyContigAlignmentsSpark.preprocess(updatedSvDiscoveryInputData, nonCanonicalChromosomeNamesFile,true);

        SvDiscoverFromLocalAssemblyContigAlignmentsSpark.dispatchJobs(contigsByPossibleRawTypes, updatedSvDiscoveryInputData);
    }

    /**
     * Makes a PairedStrandedIntervalTree from a list of EvidenceTargetLinks. The value of each entry in the resulting tree
     * will be the original EvidenceTargetLink. If the input list is null, returns a null tree.
     */
    private PairedStrandedIntervalTree<EvidenceTargetLink> makeEvidenceLinkTree(final List<EvidenceTargetLink> evidenceTargetLinks) {
        final PairedStrandedIntervalTree<EvidenceTargetLink> evidenceLinkTree;

        if (evidenceTargetLinks != null) {
            evidenceLinkTree = new PairedStrandedIntervalTree<>();
            evidenceTargetLinks.forEach(l -> evidenceLinkTree.put(l.getPairedStrandedIntervals(), l));
        } else {
            evidenceLinkTree = null;
        }
        return evidenceLinkTree;
    }

    public static Broadcast<SVIntervalTree<VariantContext>> broadcastCNVCalls(final JavaSparkContext ctx,
                                                                              final SAMFileHeader header,
                                                                              final String cnvCallsFile) {
        final SVIntervalTree<VariantContext> cnvCalls;
        if (cnvCallsFile != null) {
            cnvCalls = CNVInputReader.loadCNVCalls(cnvCallsFile, header);
        } else {
            cnvCalls = null;
        }

        final Broadcast<SVIntervalTree<VariantContext>> broadcastCNVCalls;
        if (cnvCalls != null) {
            broadcastCNVCalls = ctx.broadcast(cnvCalls);
        } else {
            broadcastCNVCalls = null;
        }
        return broadcastCNVCalls;
    }

}
