# Spark Configuration Benchmark Analysis

Ran 56 configurations across 2 processing strategies to identify the optimal setup for the taxi ingestion pipeline. The file loop and full batch variants test different approaches to handling multi-file ingestion at scale.

**Test Coverage**
- File loop approach: 12 runs with varying coalesce factors
- Full batch approach: 12 runs with higher coalesce values
- Success rate: 54/56 (96.4%) — two file loop failures at high coalesce values

The patterns here are worth understanding before we go to production.

## The Numbers

### File Loop vs. Full Batch

**File loop mode** is faster but brittle. Best case was 235 seconds with coalesce_n=8. We saw incremental degradation as coalesce increased—by the time we hit 16, execution times were 70% higher and we started dropping jobs entirely. The two failures both happened at coalesce_n=16 with 4GB executor memory. The pattern suggests we're hitting shuffle buffer limits, not a fundamental architectural issue.

**Full batch mode** trades speed for stability. Average execution sits around 340 seconds, but nothing failed. You're paying a 20-30% performance penalty for guaranteed execution. The snappy compression variant actually ran *faster* than uncompressed in full batch mode, which is counter-intuitive but makes sense if compression reduces memory pressure during shuffle operations.

**Baseline configuration** is the control group and runs in the 75-113 second range. This is almost certainly a different data volume than what we're testing in the file loop/full batch scenarios. Don't use it for capacity planning directly.

### Configuration Sensitivity

**Coalesce factor is critical.** At coalesce_n=8, both file loop and full batch methods are within 10% of each other. At coalesce_n=12, we're seeing 15-20% variance. At coalesce_n=16, file loop breaks. This isn't linear degradation; we're hitting a hard resource wall.

**Memory configuration**: Driver memory at 10GB beats 8GB consistently. Executor memory at 3GB and 4GB produced mixed results—file loop *preferred* 3GB, which is unusual. This suggests we might be triggering GC pressure at 4GB that actually slows things down on the file loop approach. For full batch, the differences are marginal.

**Shuffle partitions**: The baseline shows that 24 partitions outperforms 12, shaving off ~25 seconds on average. But this only matters in the baseline; the file loop and full batch tests kept shuffle_partitions=12 while varying coalesce. We should revisit this.

**Compression**: Minimal measurable impact. Snappy adds roughly 6% overhead in file loop mode but is effectively free in full batch. From a storage perspective, it's a win—we're probably I/O bound more often than CPU bound on ingest.



## What We're Going With

Use **file loop with coalesce_n=8** as the baseline and implement a fallback to **full batch if anything goes wrong**. This gives us the speed benefit (235s vs 340s, roughly 30% faster) while staying resilient.

**Resource allocation**: 10GB driver, 3GB executor, 3-4 cores, 256MB partition size. The 10GB driver is non-negotiable—performance drops measurably at 8GB. Executor memory at 3GB works and keeps memory overhead down.

**What to avoid**: Don't push coalesce beyond 12. We're not saving time, and we're increasing failure risk. The two failures at coalesce_n=16 weren't fluke edge cases—they were systematic resource exhaustion. Push too hard and the executor disconnects mid-shuffle.

**Compression**: Enable snappy. It costs us almost nothing in file loop mode and helps in full batch. More importantly, it reduces our data footprint in the warehouse.

**Fallback strategy**: If file loop fails (job timeout, executor disconnect, OOM), automatically retry with full batch mode. Log it so we can investigate, but don't fail the pipeline. This should be caught in our orchestration layer.

## The Failures

Two runs died at coalesce_n=16, both with 4GB executor memory. Run 11 made it to 35+ minutes before failing on a parquet write operation—likely ran out of memory during the coalesce shuffle. Run 12 disconnected almost immediately (connection refused), probably cascading from run 11 exhausting cluster resources.

This tells us the shuffle buffer can't handle the data volume at coalesce_n=16 with 4GB heap. It's not a bug; it's a resource constraint. We need either more memory (not practical) or less aggressive coalescing (practical).

If we're going to push coalesce higher in the future, we need to either increase executor memory substantially or accept that file loop mode has a hard ceiling at coalesce_n=12.

## Technical Observations

The partition strategy here is pretty standard—256MB partitions, 12-24 shuffle partitions—and it holds up well. No surprises. The coalesce operation is the real lever, but like most Spark levers, more aggression doesn't equal better results.

The driver memory constraint at 8GB is real. We're definitely not CPU-bound, and 3-4 cores isn't the bottleneck. We're hitting driver memory limits managing task scheduling and metadata overhead. 10GB fixes it.

One interesting quirk: snappy actually *improves* throughput in full batch mode. This usually means I/O is the bottleneck, and the CPU cost of compression is negligible compared to the time saved by reducing data transfer. Practical takeaway: compress it.

Executor memory shows diminishing returns past 3GB. That 4GB variant sometimes runs *slower*, which points to GC pressure. We're probably not data-heavy enough to need 4GB, and adding it just wastes heap on garbage collection.

## What's Next

**Immediate**: Deploy file loop with these settings. Coalesce_n=8, 10GB driver, 3GB executor, snappy compression. Put error handling in place for the fallback to full batch.

**Monitoring**: Track baseline execution time. When we go into production with the full data volume, we'll know pretty quickly if something's off. If 80% of runs complete in 4-5 minutes and then we see one take 15 minutes, we'll know to check logs.

**Once we're running**: Revisit the shuffle partition count. We only tested 12 and 24 in the baseline; there might be a sweet spot at 16 or 20 with the file loop approach that we haven't explored yet.

**Data validation**: Figure out why the baseline runs 3-4x faster than file loop/full batch. Is it actually a different dataset, or are we misconfiguring something? That delta is too big to ignore.
