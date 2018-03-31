package org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.*;
import htsjdk.samtools.util.SequenceUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.AlignedAssemblyOrExcuse;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVUtils;
import org.broadinstitute.hellbender.utils.SequenceDictionaryUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignmentUtils;
import org.broadinstitute.hellbender.utils.fermi.FermiLiteAssembly;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Serializable;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection.GAPPED_ALIGNMENT_BREAK_DEFAULT_SENSITIVITY;

/**
 * Loads various upstream assembly and alignment formats and turn into custom {@link AlignedContig} format in the discovery stage.
 * Implementations are expected to apply filter to the provided raw format contig alignments and
 * break the gapped alignments by calling into {@link ContigAlignmentsModifier#splitGappedAlignment(AlignmentInterval, int, int)}.
 */
public abstract class AlignedContigGenerator {

    public abstract JavaRDD<AlignedContig> getAlignedContigs();

    // =================================================================================================================

    public static final class InMemoryAlignmentParser extends AlignedContigGenerator implements Serializable {
        private static final long serialVersionUID = 1L;

        private final JavaSparkContext ctx;
        private final List<AlignedAssemblyOrExcuse> alignedAssemblyOrExcuseList;
        private final SAMFileHeader header;


        public InMemoryAlignmentParser(final JavaSparkContext ctx,
                                       final List<AlignedAssemblyOrExcuse> alignedAssemblyOrExcuseList,
                                       final SAMFileHeader header) {
            this.ctx = ctx;
            this.alignedAssemblyOrExcuseList = alignedAssemblyOrExcuseList;
            this.header = header;
        }

        @Override
        public JavaRDD<AlignedContig> getAlignedContigs() {

            // here we have two options, one is going through the route "BwaMemAlignment -> SAM -> GATKRead -> SAM -> AlignmentInterval"
            //                           which is the route if the discovery pipeline is run by "FindBreakpointEvidenceSpark -> write sam file -> load sam file -> DiscoverVariantsFromContigAlignmentsSAMSpark"
            //                         , the other is to go directly "BwaMemAlignment -> AlignmentInterval" by calling into {@code filterAndConvertToAlignedContigDirect()}, which is faster but not used here.
            //                         ; the two routes are tested to be generating the same output via {@code AlignedContigGeneratorUnitTest#testConvertAlignedAssemblyOrExcuseToAlignedContigsDirectAndConcordanceWithSAMRoute()}
            return filterAndConvertToAlignedContigViaSAM(alignedAssemblyOrExcuseList, header, ctx);
        }

        // =============================================================================================================
        /**
         * Filters out "failed" assemblies, unmapped and secondary (i.e. "XA") alignments, and
         * turn the alignments of contigs into custom {@link AlignmentInterval} format.
         * Should be generating the same output as {@link #filterAndConvertToAlignedContigDirect(Iterable, List, SAMFileHeader)};
         * and currently this assertion is tested in {@see AlignedContigGeneratorUnitTest#testConvertAlignedAssemblyOrExcuseToAlignedContigsDirectAndConcordanceWithSAMRoute()}
         */
        @VisibleForTesting
        public static JavaRDD<AlignedContig> filterAndConvertToAlignedContigViaSAM(final List<AlignedAssemblyOrExcuse> alignedAssemblyOrExcuseList,
                                                                                   final SAMFileHeader header,
                                                                                   final JavaSparkContext ctx) {

            final SAMFileHeader cleanHeader = new SAMFileHeader(header.getSequenceDictionary());
            final List<String> refNames = SequenceDictionaryUtils.getContigNamesList(header.getSequenceDictionary());

            return ctx.parallelize(alignedAssemblyOrExcuseList)
                    .filter(AlignedAssemblyOrExcuse::isNotFailure)
                    .flatMap(alignedAssemblyNoExcuse -> {
                                final FermiLiteAssembly assembly = alignedAssemblyNoExcuse.getAssembly();
                                final int assemblyId = alignedAssemblyNoExcuse.getAssemblyId();
                                final List<List<BwaMemAlignment>> allAlignmentsOfThisAssembly = alignedAssemblyNoExcuse.getContigAlignments();
                                final int nContigs = assembly.getNContigs();
                                return IntStream.range(0, nContigs)
                                        .mapToObj(contigIdx ->
                                                BwaMemAlignmentUtils.toSAMStreamForRead(
                                                        AlignedAssemblyOrExcuse.formatContigName(assemblyId, contigIdx),
                                                        assembly.getContig(contigIdx).getSequence(),
                                                        allAlignmentsOfThisAssembly.get(contigIdx),
                                                        cleanHeader, refNames,
                                                        new SAMReadGroupRecord(SVUtils.GATKSV_CONTIG_ALIGNMENTS_READ_GROUP_ID)
                                                )
                                        ).iterator();
                            }
                    )
                    .map(forOneContig ->
                            forOneContig.filter(sam -> !sam.getReadUnmappedFlag() && !sam.isSecondaryAlignment())
                                    .collect(Collectors.toList()))
                    .filter(list -> !list.isEmpty())
                    .map(forOneContig ->
                            SAMFormattedContigAlignmentParser.
                                    parseReadsAndOptionallySplitGappedAlignments(forOneContig,
                                            StructuralVariationDiscoveryArgumentCollection
                                                    .DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection
                                                    .GAPPED_ALIGNMENT_BREAK_DEFAULT_SENSITIVITY,
                                            true));
        }

        // =============================================================================================================
        /**
         * Filter out "failed" assemblies, unmapped and secondary (i.e. "XA") alignments, and
         * turn the alignments of contigs into custom {@link AlignmentInterval} format.
         * Should be generating the same output as {@link #filterAndConvertToAlignedContigViaSAM(List, SAMFileHeader, JavaSparkContext)};
         * and currently this assertion is tested in {@see AlignedContigGeneratorUnitTest#testConvertAlignedAssemblyOrExcuseToAlignedContigsDirectAndConcordanceWithSAMRoute()}
         */
        @VisibleForTesting
        public static List<AlignedContig> filterAndConvertToAlignedContigDirect(final Iterable<AlignedAssemblyOrExcuse> alignedAssemblyOrExcuseIterable,
                                                                                final List<String> refNames, final SAMFileHeader header) {

            return Utils.stream(alignedAssemblyOrExcuseIterable)
                    .filter(AlignedAssemblyOrExcuse::isNotFailure)
                    .map(alignedAssembly -> getAlignedContigsInOneAssembly(alignedAssembly, refNames, header))
                    .flatMap(Utils::stream)                                     // size == total # of contigs' from all successful assemblies
                    .filter(contig -> !contig.getAlignments().isEmpty())     // filter out unmapped and contigs without primary alignments
                    .collect(Collectors.toList());
        }

        /**
         * Work on "successful" assembly and turn its contigs' alignments to custom {@link AlignmentInterval} format.
         */
        @VisibleForTesting
        public static Iterable<AlignedContig> getAlignedContigsInOneAssembly(final AlignedAssemblyOrExcuse alignedAssembly,
                                                                             final List<String> refNames,
                                                                             final SAMFileHeader header) {

            final FermiLiteAssembly assembly = alignedAssembly.getAssembly();

            final List<List<BwaMemAlignment>> allAlignments = alignedAssembly.getContigAlignments();

            return IntStream.range(0, assembly.getNContigs())
                    .mapToObj( contigIdx -> {
                        final byte[] contigSequence = assembly.getContig(contigIdx).getSequence();
                        final String contigName = AlignedAssemblyOrExcuse.formatContigName(alignedAssembly.getAssemblyId(), contigIdx);
                        final List<AlignmentInterval> alignmentsForOneContig
                                = getAlignmentsForOneContig(contigName, contigSequence, allAlignments.get(contigIdx), refNames, header);
                        return new AlignedContig(contigName, contigSequence, alignmentsForOneContig);
                    } ).collect(Collectors.toList());
        }

        /**
         * Converts alignment records of the contig pointed to by {@code contigIdx} in a {@link FermiLiteAssembly} to custom {@link AlignmentInterval} format.
         * Filters out unmapped and ambiguous mappings (i.e. XA).
         */
        private static List<AlignmentInterval> getAlignmentsForOneContig(final String contigName,
                                                                         final byte[] contigSequence,
                                                                         final List<BwaMemAlignment> contigAlignments,
                                                                         final List<String> refNames,
                                                                         final SAMFileHeader header) {

            return contigAlignments.stream()
                    .filter( bwaMemAlignment ->  bwaMemAlignment.getRefId() >= 0
                            && SAMFlag.SECONDARY_ALIGNMENT.isUnset(bwaMemAlignment.getSamFlag())) // mapped and not XA (i.e. not secondary)
                    .map(bwaMemAlignment -> BwaMemAlignmentUtils.applyAlignment(contigName, contigSequence, null,
                            null, bwaMemAlignment, refNames, header, false, false))
                    .map(AlignmentInterval::new)
                    .map(ar -> ContigAlignmentsModifier.splitGappedAlignment(ar, StructuralVariationDiscoveryArgumentCollection
                            .DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection
                            .GAPPED_ALIGNMENT_BREAK_DEFAULT_SENSITIVITY, contigSequence.length))
                    .flatMap(Utils::stream).collect(Collectors.toList());
        }
    }

    public static final class SAMFormattedContigAlignmentParser extends AlignedContigGenerator implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final JavaRDD<GATKRead> unfilteredContigAlignments;
        private final SAMFileHeader header;
        private final boolean splitGapped;

        public SAMFormattedContigAlignmentParser(final JavaRDD<GATKRead> unfilteredContigAlignments,
                                                 final SAMFileHeader header, final boolean splitGapped) {
            this.unfilteredContigAlignments = unfilteredContigAlignments;
            this.header = header;
            this.splitGapped = splitGapped;
        }

        @Override
        public JavaRDD<AlignedContig> getAlignedContigs() {
            return unfilteredContigAlignments
                    .filter(r -> !r.isSecondaryAlignment())
                    .groupBy(GATKRead::getName)
                    .map(Tuple2::_2)
                    .map(gatkReads ->
                            parseReadsAndOptionallySplitGappedAlignments(
                                    Utils.stream(gatkReads).map(r->r.convertToSAMRecord(header)).collect(Collectors.toList()),
                                    GAPPED_ALIGNMENT_BREAK_DEFAULT_SENSITIVITY, splitGapped));
        }

        /**
         * Iterates through the input {@code noSecondaryAlignments}, which are assumed to contain no secondary alignment (i.e. records with "XA" tag),
         * converts to custom {@link AlignmentInterval} format and
         * split the records when the gap in the alignment reaches the specified {@code sensitivity}.
         * The size of the returned iterable of {@link AlignmentInterval}'s is guaranteed to be no lower than that of the input iterable.
         */
        @VisibleForTesting
        public static AlignedContig parseReadsAndOptionallySplitGappedAlignments(final Iterable<SAMRecord> noSecondaryAlignments,
                                                                                 final int gapSplitSensitivity,
                                                                                 final boolean splitGapped) {

            Utils.validateArg(noSecondaryAlignments.iterator().hasNext(), "input collection of GATK reads is empty");

            final SAMRecord primaryAlignment
                    = Utils.stream(noSecondaryAlignments).filter(sam -> !sam.getSupplementaryAlignmentFlag())
                    .findFirst()
                    .orElseThrow(() -> new GATKException("no primary alignment for read " + noSecondaryAlignments.iterator().next().getReadName()));

            Utils.validate(!primaryAlignment.getCigar().containsOperator(CigarOperator.H),
                    "assumption that primary alignment does not contain hard clipping is invalid for read: " + primaryAlignment.toString());

            final byte[] contigSequence = primaryAlignment.getReadBases().clone();
            final List<AlignmentInterval> parsedAlignments;
            if ( primaryAlignment.getReadUnmappedFlag() ) { // the Cigar
                parsedAlignments = Collections.emptyList();
            } else {
                if (primaryAlignment.getReadNegativeStrandFlag()) {
                    SequenceUtil.reverseComplement(contigSequence);
                }

                final Stream<AlignmentInterval> unSplitAIList = Utils.stream(noSecondaryAlignments).map(AlignmentInterval::new);
                if (splitGapped) {
                    final int unClippedContigLength = primaryAlignment.getReadLength();
                    parsedAlignments = unSplitAIList.map(ar ->
                            ContigAlignmentsModifier.splitGappedAlignment(ar, gapSplitSensitivity, unClippedContigLength))
                            .flatMap(Utils::stream).collect(Collectors.toList());
                } else {
                    parsedAlignments = unSplitAIList.collect(Collectors.toList());
                }
            }
            return new AlignedContig(primaryAlignment.getReadName(), contigSequence, parsedAlignments);
        }
    }
}
