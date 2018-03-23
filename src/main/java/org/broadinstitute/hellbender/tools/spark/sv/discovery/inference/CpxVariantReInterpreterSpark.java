package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryPipelineSpark;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SimpleSVType;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvDiscoverFromLocalAssemblyContigAlignmentsSpark;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvDiscoveryInputData;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignedContig;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AssemblyContigWithFineTunedAlignments;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVVCFWriter;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.AssemblyContigAlignmentSignatureClassifier.RawTypes.Cpx;

/**
 * (Internal) Tries to extract simple variants from a provided GATK-SV CPX.vcf
 */
@DocumentedFeature
@BetaFeature
@CommandLineProgramProperties(
        oneLineSummary = "(Internal) Tries to extract simple variants from a provided GATK-SV CPX.vcf",
        summary =
                "This tool is used in development and should not be of interest to most researchers." +
                " It is a prototype of complex structural variant re-interpretation." +
                " In particular, it tries to extract basic SVTYPE's from a user-provided GATK-SV CPX.vcf," +
                " and outputs two VCF files containing bare bone information on the simple variants.",
        programGroup = StructuralVariantDiscoveryProgramGroup.class)
public class CpxVariantReInterpreterSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private final Logger localLogger = LogManager.getLogger(CpxVariantReInterpreterSpark.class);

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    public boolean requiresReads() {
        return true;
    }

    @ArgumentCollection
    private StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection
            discoverStageArgs
            = new StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection();

    @Argument(doc = "file containing non-canonical chromosome names (e.g chrUn_KI270588v1) in the reference, human reference (hg19 or hg38) assumed when omitted",
            shortName = "alt-tigs",
            fullName = "non-canonical-contig-names-file", optional = true)
    public String nonCanonicalChromosomeNamesFile;

    @Argument(doc = "file containing complex variants as output by GATK-SV",
            fullName = "cpx-vcf")
    private String complexVCF;

    @Argument(doc = "prefix for two files containing derived simple variants for complex variants having one/multiple entry in SEGMENT annotation",
            fullName = "prefix-out-vcf")
    private String derivedSimpleVCFPrefix;

    public static String EVENT_KEY = "CPX_EVENT"; // for use in output VCF to link to original CPX variant

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        final List<VariantContext> complexVariants = new VariantsSparkSource(ctx)
                .getParallelVariantContexts(complexVCF, null).collect();

        localLogger.info("Dealing with " + complexVariants.size() + " complex variants");

        final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast =
                StructuralVariationDiscoveryPipelineSpark.broadcastCNVCalls(ctx, getHeaderForReads(),
                        discoverStageArgs.cnvCallsFile);
        final SvDiscoveryInputData svDiscoveryInputData =
                new SvDiscoveryInputData(ctx, discoverStageArgs, nonCanonicalChromosomeNamesFile, derivedSimpleVCFPrefix,
                        null, null, null,
                        cnvCallsBroadcast,
                        getUnfilteredReads(), getHeaderForReads(), getReference(), localLogger);

        apiOpenToPipeline( complexVariants, svDiscoveryInputData);
    }

    // exist for use by SV pipeline
    public static void apiOpenToPipeline(final List<VariantContext> complexVariants,
                                         final SvDiscoveryInputData svDiscoveryInputData) {

        final ReferenceMultiSource reference = svDiscoveryInputData.inputMetaData.referenceBroadcast.getValue();
        final SAMSequenceDictionary refSeqDict = svDiscoveryInputData.inputMetaData.referenceSequenceDictionaryBroadcast.getValue();

        final Stream<VariantContext> zeroSegmentCalls = complexVariants.stream()
                .filter(vc -> makeSureAttributeIsList(vc, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS).isEmpty());

        final Stream<VariantContext> oneSegmentCalls = complexVariants.stream()
                .filter(vc -> makeSureAttributeIsList(vc, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS).size() == 1);

        final Stream<VariantContext> multiSegmentCalls = complexVariants.stream()
                .filter(vc -> makeSureAttributeIsList(vc, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS).size() > 1);

        final List<VariantContext> reInterpretOneSegmentCalls = reInterpretZeroOrOneSegmentCalls(zeroSegmentCalls, oneSegmentCalls, reference);

        final List<VariantContext> reInterpretMultiSegmentsCalls = reInterpretMultiSegmentsCalls(multiSegmentCalls, svDiscoveryInputData);

        final String prefix = svDiscoveryInputData.outputPath;
        CpxVariantReInterpreterSpark.writeResults(reInterpretOneSegmentCalls, reInterpretMultiSegmentsCalls, refSeqDict,
                prefix + "_1_seg.vcf", prefix + "_multi_seg.vcf",
                svDiscoveryInputData.inputMetaData.toolLogger);
    }

    //==================================================================================================================

    /**
     * Re-interpret CPX vcf records whose {@link GATKSVVCFConstants#CPX_SV_REF_SEGMENTS} has only one entry,
     * aka "one-segment" calls, and those without such annotations, i.e. those with pure inserted sequence.
     *
     * @return the {@link SimpleSVType}-d variants extracted from the input {@code complexVariants}
     */
    private static List<VariantContext> reInterpretZeroOrOneSegmentCalls(final Stream<VariantContext> zeroSegmentCalls,
                                                                         final Stream<VariantContext> oneSegmentCalls,
                                                                         final ReferenceMultiSource reference) {

        final Stream<VariantContext> reInterpreted = zeroSegmentCalls
                .map(vc -> {
                    final Allele anchorBaseRefAllele = Allele.create(vc.getAlleles().get(0).getBases()[0], true);
                    final Allele altSymbAlleleIns = Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_INS));
                    final int altSeqLength = vc.getAttributeAsString(GATKSVVCFConstants.SEQ_ALT_HAPLOTYPE, "").length() - 1;
                    final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                            .chr(vc.getContig()).start(vc.getStart()).stop(vc.getStart())
                            .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleIns))
                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                            .attribute(GATKSVVCFConstants.SVLEN, altSeqLength)
                            .attribute(EVENT_KEY, vc.getID())
                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, makeSureAttributeIsList(vc, GATKSVVCFConstants.CONTIG_NAMES));
                    return vcBuilderForIns.make();
                });

        return Stream.concat(
                reInterpreted,
                oneSegmentCalls.flatMap(vc -> CpxVariantReInterpreterSpark.workerForOneSegmentCalls(vc, reference))
        ).collect(Collectors.toList());
    }

    /**
     * Depending on if the ref segment is present in alt arrangement, logic as follows:
     * <ul>
     *     <li>
     *         ref segment is absent
     *         <ul>
     *             <li> present inverted -> INV (if size > 49) and INS (if new material > 49) </li>
     *             <li> not at all       -> DEL (if size > 49) and INS (if new material > 49) </li>
     *         </ul>
     *     </li>
     *
     *     <li>
     *         ref segment is present
     *         <ul>
     *             <li> there's >=1 inverted presence -> INV (if size > 49) and INS (if new material subtracting the INV > 49) </li>
     *             <li> there's no  inverted presence -> INS (if new material > 49) </li>
     *         </ul>
     *     </li>
     * </ul>
     */
    private static Stream<VariantContext> workerForOneSegmentCalls(final VariantContext vc, final ReferenceMultiSource reference) {

        final List<String> evidenceContigs = makeSureAttributeIsList(vc, GATKSVVCFConstants.CONTIG_NAMES);
        final SimpleInterval refSegment =
                new SimpleInterval(vc.getAttributeAsString(GATKSVVCFConstants.CPX_SV_REF_SEGMENTS, ""));
        final int segmentSize = refSegment.size();

        final int altSeqLength = vc.getAttributeAsString(GATKSVVCFConstants.SEQ_ALT_HAPLOTYPE, "").length() - 1;

        final List<String> altArrangement = makeSureAttributeIsList(vc, GATKSVVCFConstants.CPX_EVENT_ALT_ARRANGEMENTS);

        final List<VariantContext> result = new ArrayList<>();
        final Allele anchorBaseRefAllele = Allele.create(vc.getAlleles().get(0).getBases()[0], true);
        final Allele fatInsertionRefAllele;
        try {
            fatInsertionRefAllele = Allele.create(reference.getReferenceBases(refSegment).getBases(), true);
        } catch (final IOException ioex) {
            throw new GATKException("", ioex);
        }
        final Allele altSymbAlleleDel = Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_DEL));
        final Allele altSymbAlleleIns = Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_INS));
        final Allele altSymbAlleleInv = Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_INV));
        final long invertedAppearance = altArrangement.stream().filter(s -> s.equals("-1")).count();
        final long asIsAppearance = altArrangement.stream().filter(s -> s.equals("1")).count();

        if (asIsAppearance == 0) {
            if (invertedAppearance == 0) {
                if ( segmentSize > 49 ) {
                    final VariantContextBuilder vcBuilderForDel = new VariantContextBuilder()
                            .chr(refSegment.getContig()).start(refSegment.getStart()).stop(refSegment.getEnd())
                            .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleDel))
                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.DEL.name())
                            .attribute(GATKSVVCFConstants.SVLEN, -segmentSize)
                            .attribute(VCFConstants.END_KEY, refSegment.getEnd())
                            .attribute(EVENT_KEY, refSegment.getEnd())
                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                    final VariantContext del = vcBuilderForDel.make();
                    result.add(del);
                }
                if ( altSeqLength > 49 ) { // depending on if the segment is > 49, the REF allele is either anchor or fat
                    if (segmentSize > 49) {
                        final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                                .chr(vc.getContig()).start(vc.getStart()).stop(vc.getStart())
                                .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleIns))
                                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                                .attribute(GATKSVVCFConstants.SVLEN, altSeqLength)
                                .attribute(EVENT_KEY, vc.getID())
                                .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                        final VariantContext ins = vcBuilderForIns.make();
                        result.add(ins);
                    } else { // fat insertion
                        final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                                .chr(refSegment.getContig()).start(refSegment.getStart()).stop(refSegment.getEnd())
                                .alleles(Arrays.asList(fatInsertionRefAllele, altSymbAlleleIns))
                                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                                .attribute(GATKSVVCFConstants.SVLEN, altSeqLength)
                                .attribute(EVENT_KEY, vc.getID())
                                .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                        final VariantContext ins = vcBuilderForIns.make();
                        result.add(ins);
                    }
                }
            } else {
                if (segmentSize > 49) {
                    final VariantContextBuilder vcBuilderForInv = new VariantContextBuilder()
                            .chr(refSegment.getContig()).start(refSegment.getStart()).stop(refSegment.getEnd())
                            .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleInv))
                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INV.name())
                            .attribute(GATKSVVCFConstants.SVLEN, 0) // per VCF spec, INV should have 0 length
                            .attribute(VCFConstants.END_KEY, refSegment.getEnd())
                            .attribute(EVENT_KEY, vc.getID())
                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                    final VariantContext inv = vcBuilderForInv.make();
                    result.add(inv);
                    if ( altSeqLength - segmentSize > 49 ) {
                        final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                                .chr(vc.getContig()).start(vc.getStart()).stop(vc.getStart())
                                .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleIns))
                                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                                .attribute(GATKSVVCFConstants.SVLEN, altSeqLength - segmentSize)
                                .attribute(EVENT_KEY, vc.getID())
                                .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                        final VariantContext ins = vcBuilderForIns.make();
                        result.add(ins);
                    }
                } else if ( altSeqLength > 49 ){ // ref segment replace with fat INS (because the ref segment is not long enough to merit a deletion)
                    final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                            .chr(refSegment.getContig()).start(refSegment.getStart()).stop(refSegment.getEnd())
                            .alleles(Arrays.asList(fatInsertionRefAllele, altSymbAlleleIns))
                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                            .attribute(GATKSVVCFConstants.SVLEN, altSeqLength)
                            .attribute(EVENT_KEY, vc.getID())
                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                    final VariantContext ins = vcBuilderForIns.make();
                    result.add(ins);
                }
            }
        } else { // at least one "1", so no deletion can be emitted
            if (invertedAppearance == 0) {
                if ( altSeqLength - segmentSize > 49 ) { // long enough net gain
                    // distinguish between cases {"1", ....} and {.....,"1"} to know where to put the insertion
                    if (altArrangement.get(altArrangement.size() - 1).equals("1")) {
                        final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                                .chr(vc.getContig()).start(vc.getStart()).stop(vc.getStart())
                                .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleIns))
                                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                                .attribute(GATKSVVCFConstants.SVLEN, altSeqLength - segmentSize)
                                .attribute(EVENT_KEY, vc.getID())
                                .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                        final VariantContext ins = vcBuilderForIns.make();
                        result.add(ins);
                    } else if (altArrangement.get(0).equals("1")) {
                        final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                                .chr(vc.getContig()).start(vc.getStart()).stop(vc.getStart())
                                .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleIns))
                                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                                .attribute(GATKSVVCFConstants.SVLEN, altSeqLength - segmentSize)
                                .attribute(EVENT_KEY, vc.getID())
                                .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                        final VariantContext ins = vcBuilderForIns.make();
                        result.add(ins);
                    }
                }
            } else {
                if (segmentSize > 49) { // segment inverted long enough to warrant an inversion
                    final VariantContextBuilder vcBuilderForInv = new VariantContextBuilder()
                            .chr(refSegment.getContig()).start(refSegment.getStart()).stop(refSegment.getEnd())
                            .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleInv))
                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INV.name())
                            .attribute(GATKSVVCFConstants.SVLEN, 0) // per VCF spec, INV should have 0 length
                            .attribute(VCFConstants.END_KEY, refSegment.getEnd())
                            .attribute(EVENT_KEY, vc.getID())
                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                    final VariantContext inv = vcBuilderForInv.make();
                    result.add(inv);
                    if ( altSeqLength - segmentSize > 49 ) {
                        final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                                .chr(vc.getContig()).start(vc.getStart()).stop(vc.getStart())
                                .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleIns))
                                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                                .attribute(GATKSVVCFConstants.SVLEN, altSeqLength - segmentSize)
                                .attribute(EVENT_KEY, vc.getID())
                                .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                        final VariantContext ins = vcBuilderForIns.make();
                        result.add(ins);
                    }
                } else if ( altSeqLength - segmentSize > 49 ) { // inverted segment not long enough for an inversion call
                    final VariantContextBuilder vcBuilderForIns = new VariantContextBuilder()
                            .chr(vc.getContig()).start(vc.getStart()).stop(vc.getStart())
                            .alleles(Arrays.asList(anchorBaseRefAllele, altSymbAlleleIns))
                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name())
                            .attribute(GATKSVVCFConstants.SVLEN, altSeqLength - segmentSize)
                            .attribute(EVENT_KEY, vc.getID())
                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs);
                    final VariantContext ins = vcBuilderForIns.make();
                    result.add(ins);
                }
            }
        }
        return result.stream();
    }

    //==================================================================================================================

    /**
     * Re-interpret CPX vcf records whose {@link GATKSVVCFConstants#CPX_SV_REF_SEGMENTS} has more than one entries,
     * aka "multi-segment" calls.
     *
     * @return the {@link SimpleSVType}-d variants extracted from the input {@code complexVariants}
     */
    private static List<VariantContext> reInterpretMultiSegmentsCalls(final Stream<VariantContext> multiSegmentCalls,
                                                                      final SvDiscoveryInputData svDiscoveryInputData) {

        final Tuple2<Stream<VariantContext>, Set<VariantContext>> acceptedSimpleVariantsAndRejectedComplex =
                pairIterationWayOfReInterpretation(multiSegmentCalls, svDiscoveryInputData);
        final ReferenceMultiSource reference = svDiscoveryInputData.inputMetaData.referenceBroadcast.getValue();
        return Stream.concat(
                acceptedSimpleVariantsAndRejectedComplex._1,
                acceptedSimpleVariantsAndRejectedComplex._2.stream().flatMap(vc -> workerForMultiSegmentsCalls(vc, reference)))
                .collect(Collectors.toList());
    }

    /**
     * Get contigs who triggered multi-segment calls,
     * create a map from their read name to the corresponding CPX variant,
     * then collect their alignments for sending them through the pair-iteration-ed way of re-interpretation.
     */
    private static Tuple2<Stream<VariantContext>, Set<VariantContext>> pairIterationWayOfReInterpretation(final Stream<VariantContext> multiSegmentCalls,
                                                                                                          final SvDiscoveryInputData svDiscoveryInputData) {

        final Map<String, VariantContext> contigNameToCpxVariant = multiSegmentCalls
                .flatMap(vc -> makeSureAttributeIsList(vc, GATKSVVCFConstants.CONTIG_NAMES).stream().map(s -> new Tuple2<>(s, vc)))
                .collect(Collectors.toMap(pair -> pair._1, pair -> pair._2));

        final JavaRDD<GATKRead> relevantAlignments =
                svDiscoveryInputData.assemblyRawAlignments.filter(read -> contigNameToCpxVariant.containsKey(read.getName()));
        final SvDiscoveryInputData filteredData =
                new SvDiscoveryInputData(svDiscoveryInputData.inputMetaData, relevantAlignments, svDiscoveryInputData.outputPath);

        // resend the contigs through the pair-iteration-ed path
        final JavaRDD<AlignedContig> analysisReadyContigs =
                SvDiscoverFromLocalAssemblyContigAlignmentsSpark.preprocess(filteredData, false)
                        .get(Cpx)
                        .map(AssemblyContigWithFineTunedAlignments::getSourceContig);

        @SuppressWarnings("deprecation")
        final List<VariantContext> reInterpreted =
                org.broadinstitute.hellbender.tools.spark.sv.discovery
                        .DiscoverVariantsFromContigAlignmentsSAMSpark
                        .discoverVariantsFromChimeras(filteredData.inputMetaData, analysisReadyContigs);

        // collect those CPX variants whose re-interpretation from above would be rejected, then re-interpret with workerForMultiSegmentsCalls()
        final Stream<VariantContext> result = reInterpreted
                .stream()
                .map(simpleVC -> {
                    final List<String> consistentComplexVariantIDs =
                            makeSureAttributeIsList(simpleVC, GATKSVVCFConstants.CONTIG_NAMES).stream()
                                    .map(contigNameToCpxVariant::get)
                                    .filter(cpx -> isConsistentWithCPX(simpleVC, cpx))
                                    .map(VariantContext::getID)
                                    .collect(Collectors.toList());
                    if (consistentComplexVariantIDs.isEmpty()) {
                        return null;
                    } else { // add new annotation for signalling that they were extracted from CPX variants
                        return new VariantContextBuilder(simpleVC)
                                .attribute(EVENT_KEY,
                                        String.join(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR, consistentComplexVariantIDs))
                                .make();
                    }
                })
                .filter(Objects::nonNull);

        final Set<VariantContext> complexVariantsWithRejectedSimpleInterpretations =
                reInterpreted.stream()
                        .flatMap(simpleVC ->
                                makeSureAttributeIsList(simpleVC, GATKSVVCFConstants.CONTIG_NAMES).stream()
                                        .map(contigNameToCpxVariant::get)
                                        .filter( cpx -> !isConsistentWithCPX(simpleVC, cpx))
                        )
                        .collect(Collectors.toSet());
        return new Tuple2<>(result, complexVariantsWithRejectedSimpleInterpretations);
    }

    /**
     * // TODO: 3/26/18 check consistency with CPX call, here we check consistency only for DEL and INV calls
     * @param simple   simple variant derived from pair-iteration logic that is to be checked
     * @param complex  CPX variant induced by the same contig that induced the simple variant
     */
    private static boolean isConsistentWithCPX(final VariantContext simple, final VariantContext complex) {

        final List<SimpleInterval> refSegments = makeSureAttributeIsList(complex, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS)
                .stream().map(SimpleInterval::new).collect(Collectors.toList());

        final Tuple2<Set<Integer>, List<Integer>> presentAndInvertedSegments = getPresentAndInvertedSegments(complex);

        final Set<Integer> presentSegments = presentAndInvertedSegments._1;
        final Set<Integer> invertedSegments = new HashSet<>( presentAndInvertedSegments._2 );

        final String typeString = (String) simple.getAttribute(GATKSVVCFConstants.SVTYPE, "");
        if (typeString.equals(SimpleSVType.TYPES.DEL.name())) {

            final List<SimpleInterval> missingSegments = IntStream.rangeClosed(1, refSegments.size()).boxed()
                    .filter(i -> !presentSegments.contains(i) && !invertedSegments.contains(i))
                    .map(i -> refSegments.get(i-1))
                    .collect(Collectors.toList());
            if (missingSegments.isEmpty())
                return false;

            final SimpleInterval deletedRange = new SimpleInterval(simple.getContig(), simple.getStart(), simple.getEnd());
            // dummy number for chr to be used in constructing SVInterval, since 2 input AI's both map to the same chr by this point
            final int dummyChr = -1;
            final SVInterval intervalOne = new SVInterval(dummyChr, deletedRange.getStart() - 1, deletedRange.getEnd());

            for (final SimpleInterval missing : missingSegments) {
                final SVInterval intervalTwo = new SVInterval(dummyChr, missing.getStart() - 1, missing.getEnd());
                // allow 1-base fuzziness from either end
                if ( Math.abs(missing.size() - deletedRange.size()) > 2 )
                    return false;
                if( 2 >= Math.abs( Math.min(missing.size(), deletedRange.size()) - intervalTwo.overlapLen(intervalOne) ) ){
                    return true;
                }
            }
            return false;
        } else if (typeString.equals(SimpleSVType.TYPES.INV.name())) {

            if (invertedSegments.isEmpty())
                return false;

            final SimpleInterval invertedRange = new SimpleInterval(simple.getContig(), simple.getStart(), simple.getEnd());
            // dummy number for chr to be used in constructing SVInterval, since 2 input AI's both map to the same chr by this point
            final int dummyChr = -1;
            final SVInterval intervalOne = new SVInterval(dummyChr, invertedRange.getStart() - 1, invertedRange.getEnd());
            for (int i = 0; i < refSegments.size(); ++i) {
                final SimpleInterval segment = refSegments.get(i);
                final SVInterval intervalTwo = new SVInterval(dummyChr, segment.getStart() - 1, segment.getEnd());
                // allow 1-base fuzziness from either end
                if( 2 >= Math.abs( Math.min(segment.size(), invertedRange.size()) - intervalTwo.overlapLen(intervalOne) ) ){ // this also gets rid of a case where a large deletion is interpreted but is actually a dispersed duplication (the stringent kills it)
                    return (invertedSegments.contains(i)) && (!presentSegments.contains(i)); // inverted range appears in inverted segments, but absent in alt description without the "-" sign (if present, treat as insertion)
                }
            }
            return false;
        } else
            return true; // TODO: 3/26/18 check consistency with CPX call, here we check consistency only for DEL and INV calls
    }

    /**
     * Experimental way of re-interpreting the multi-segment calls,
     * doesn't seem to perform well yet for INS calls based on manual-inspection,
     * so we don't output insertion calls from this way of re-interpretation
     */
    private static Stream<? extends VariantContext> workerForMultiSegmentsCalls(final VariantContext complexVC,
                                                                                final ReferenceMultiSource reference) {

        final Tuple2<Set<Integer>, List<Integer>> presentAndInvertedSegments = getPresentAndInvertedSegments(complexVC);
        final Set<Integer> presentSegments = presentAndInvertedSegments._1;
        final List<Integer> invertedSegments = presentAndInvertedSegments._2;
        final List<SimpleInterval> segments = makeSureAttributeIsList(complexVC, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS).stream()
                .map(SimpleInterval::new)
                .collect(Collectors.toList());

        final List<Integer> missingSegments = IntStream.rangeClosed(1, segments.size()).boxed()
                .filter(i -> !presentSegments.contains(i) && !invertedSegments.contains(i))
                .collect(Collectors.toList());

        final List<String> evidenceContigs = makeSureAttributeIsList(complexVC, GATKSVVCFConstants.CONTIG_NAMES);

        final List<VariantContext> result = new ArrayList<>();

        // inversions
        if ( !invertedSegments.isEmpty() ) {
            result.addAll(
                    invertedSegments.stream()
                            .filter(i -> segments.get(i - 1).size() > 49 && (!presentSegments.contains(i)) ) // large enough; in addition, if both as-is and inverted versions exist, treat as insertions?
                            .map(i -> {
                                try {
                                    final SimpleInterval segment = segments.get(i - 1);
                                    final byte[] ref = reference.getReferenceBases(new SimpleInterval(segment.getContig(), segment.getStart(), segment.getStart())).getBases();
                                    return new VariantContextBuilder()
                                            .chr(segment.getContig()).start(segment.getStart()).stop(segment.getEnd())
                                            .alleles(Arrays.asList(
                                                    Allele.create(ref, true),
                                                    Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_INV)))
                                            )
                                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INV.name())
                                            .attribute(EVENT_KEY, complexVC.getID())
                                            .attribute(VCFConstants.END_KEY, segment.getEnd())
                                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs)
                                            .make();
                                } catch (final IOException ioex) {
                                    throw new GATKException("", ioex);
                                }
                            })
                            .collect(Collectors.toList())
            );
        }

        // deletions
        if ( !missingSegments.isEmpty() ){
            result.addAll(
                    missingSegments.stream()
                            .filter(i -> segments.get(i - 1).size() > 49) // large enough
                            .map(i -> {
                                try {
                                    final SimpleInterval segment = segments.get(i - 1);
                                    final byte[] ref = reference.getReferenceBases(new SimpleInterval(segment.getContig(), segment.getStart(), segment.getStart())).getBases();
                                    return new VariantContextBuilder()
                                            .chr(segment.getContig()).start(segment.getStart()).stop(segment.getEnd())
                                            .alleles(Arrays.asList(
                                                    Allele.create(ref, true),
                                                    Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_DEL)))
                                            )
                                            .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.DEL.name())
                                            .attribute(GATKSVVCFConstants.SVLEN, -segment.size())
                                            .attribute(EVENT_KEY, complexVC.getID())
                                            .attribute(VCFConstants.END_KEY, segment.getEnd())
                                            .attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs)
                                            .make();
                                } catch (final IOException ioex) {
                                    throw new GATKException("", ioex);
                                }
                            })
                            .collect(Collectors.toList())
            );
        }

        return result.stream();
    }

    /**
     * Retrieves from the provide {@code complexVC}, reference segments described in
     * {@link GATKSVVCFConstants#CPX_SV_REF_SEGMENTS}, that are
     *   a) present as is, i.e. not inverted
     *   b) inverted
     */
    private static Tuple2<Set<Integer>, List<Integer>> getPresentAndInvertedSegments(final VariantContext complexVC) {
        final List<Integer> invertedSegments = new ArrayList<>();
        final Set<Integer> presentSegments = new TreeSet<>();
        makeSureAttributeIsList(complexVC, GATKSVVCFConstants.CPX_EVENT_ALT_ARRANGEMENTS)
                .forEach(s -> {
                    if ( s.startsWith("-") && ( !s.contains(":") )) { // some segment inverted
                        invertedSegments.add( Integer.valueOf(s.substring(1)) );
                    }
                    if ( !s.contains(":") && !s.startsWith(CpxVariantCanonicalRepresentation.UNMAPPED_INSERTION) && !s.startsWith("-") ) { // a ref segment, but not inverted
                        presentSegments.add(Integer.valueOf(s));
                    }
                });
        return new Tuple2<>(presentSegments, invertedSegments);
    }

    //==================================================================================================================
    private static void writeResults(final List<VariantContext> reInterpretOneSegmentCalls,
                                     final List<VariantContext> reInterpretMultiSegmentsCalls,
                                     final SAMSequenceDictionary sequenceDictionary,
                                     final String derivedOneSegmentSimpleVCF,
                                     final String derivedMultiSegmentSimpleVCF,
                                     final Logger logger) {

        SVVCFWriter.writeVCF(reInterpretOneSegmentCalls, derivedOneSegmentSimpleVCF, sequenceDictionary, logger);

        SVVCFWriter.writeVCF(reInterpretMultiSegmentsCalls, derivedMultiSegmentSimpleVCF, sequenceDictionary, logger);
    }

    // this exist because for whatever reason, VC.getAttributeAsStringList() sometimes returns a giant single string
    // while using VC.getAttributeAsString() gives back an array.....
    private static List<String> makeSureAttributeIsList(final VariantContext vc, final String attributeKey) {
        return vc.getAttributeAsStringList(attributeKey, "").stream()
                .flatMap(s -> {
                    if ( s.contains(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR) ) {
                        final String[] split = s.split(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR);
                        return Arrays.stream(split);
                    } else {
                        return Stream.of(s);
                    }
                })
                .collect(Collectors.toList());
    }
}
