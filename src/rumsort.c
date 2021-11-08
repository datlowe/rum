/*-------------------------------------------------------------------------
 *
 * rumsort.c
 *	  Generalized tuple sorting routines.
 *
 * This module handles sorting of RumSortItem or RumScanItem structures.
 * It contains copy of static functions from
 * src/backend/utils/sort/tuplesort.c.
 *
 *
 * Portions Copyright (c) 2015-2019, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/tuplesort.h"
#include "rumsort.h"
#include "tuplesort.c"
#include "rum.h"

#if PG_VERSION_NUM >= 150000
#	define TAPE(tapeset, result_tape) result_tape
#   define TAPE2(state) state->result_tape
#else
#	define TAPE(tapeset, result_tape) tapeset, result_tape
#   define TAPE2(state) state, state->result_tape
#endif

static RumTuplesortstate *rum_tuplesort_begin_common(int workMem, bool randomAccess);

static int comparetup_rum_true(const SortTuple *a, const SortTuple *b,
			   RumTuplesortstate *state);
static int comparetup_rum_false(const SortTuple *a, const SortTuple *b,
			   RumTuplesortstate *state);
static int comparetup_rum(const SortTuple *a, const SortTuple *b,
			   RumTuplesortstate *state, bool compareItemPointer);


static void copytup_rum(RumTuplesortstate *state, SortTuple *stup, void *tup);

#if PG_VERSION_NUM >= 150000
static void writetup_rum(RumTuplesortstate *state, LogicalTape *unused,
			 SortTuple *stup);
static void readtup_rum(RumTuplesortstate *state, SortTuple *stup,
			LogicalTape *unused, unsigned int len);
static void writetup_rumitem(RumTuplesortstate *state, LogicalTape *unused,
			 SortTuple *stup);
static void readtup_rumitem(RumTuplesortstate *state, SortTuple *stup,
			LogicalTape *unused, unsigned int len);
#else
static void writetup_rum(RumTuplesortstate *state, int tapenum,
			 SortTuple *stup);
static void readtup_rum(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len);
static void writetup_rumitem(RumTuplesortstate *state, int tapenum,
			 SortTuple *stup);
static void readtup_rumitem(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len);

#endif

static bool rum_tuplesort_gettuple_common(RumTuplesortstate *state, bool forward,
							  SortTuple *stup, bool *should_free);

static void reversedirection_rum(RumTuplesortstate *state);

static void copytup_rumitem(RumTuplesortstate *state, SortTuple *stup, void *tup);


static int
comparetup_rum_true(const SortTuple *a, const SortTuple *b, RumTuplesortstate *state)
{
	return comparetup_rum(a, b, state, true);
}

static int
comparetup_rum_false(const SortTuple *a, const SortTuple *b, RumTuplesortstate *state)
{
	return comparetup_rum(a, b, state, false);
}


/*
 *		rum_tuplesort_begin_xxx
 *
 * Initialize for a tuple sort operation.
 *
 * After calling rum_tuplesort_begin, the caller should call rum_tuplesort_putXXX
 * zero or more times, then call rum_tuplesort_performsort when all the tuples
 * have been supplied.  After performsort, retrieve the tuples in sorted
 * order by calling rum_tuplesort_getXXX until it returns false/NULL.  (If random
 * access was requested, rescan, markpos, and restorepos can also be called.)
 * Call rum_tuplesort_end to terminate the operation and release memory/disk space.
 *
 * Each variant of rum_tuplesort_begin has a workMem parameter specifying the
 * maximum number of kilobytes of RAM to use before spilling data to disk.
 * (The normal value of this parameter is work_mem, but some callers use
 * other values.)  Each variant also has a randomAccess parameter specifying
 * whether the caller needs non-sequential access to the sort result.
 */

static RumTuplesortstate *
rum_tuplesort_begin_common(int workMem, bool randomAccess)
{
	RumTuplesortstate *state;
	MemoryContext sortcontext;
	MemoryContext oldcontext;

	/*
	 * Create a working memory context for this sort operation. All data
	 * needed by the sort will live inside this context.
	 */
	sortcontext = RumContextCreate(CurrentMemoryContext, "TupleSort");

	/*
	 * Make the Tuplesortstate within the per-sort context.  This way, we
	 * don't need a separate pfree() operation for it at shutdown.
	 */
	oldcontext = MemoryContextSwitchTo(sortcontext);

	state = (RumTuplesortstate *) palloc0(sizeof(RumTuplesortstate));

#ifdef TRACE_SORT
	if (trace_sort)
		pg_rusage_init(&state->ru_start);
#endif

	state->status = TSS_INITIAL;
	state->randomAccess = randomAccess;
	state->bounded = false;
	state->boundUsed = false;
	state->allowedMem = workMem * 1024L;
	state->availMem = state->allowedMem;
	state->sortcontext = sortcontext;
	state->tapeset = NULL;
	state->memtupcount = 0;

	/*
	 * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
	 * see comments in grow_memtuples().
	 */
	state->memtupsize = Max(1024,
						ALLOCSET_SEPARATE_THRESHOLD / sizeof(SortTuple) + 1);

	state->growmemtuples = true;
	state->memtuples = (SortTuple *) palloc(state->memtupsize * sizeof(SortTuple));

	USEMEM(state, GetMemoryChunkSpace(state->memtuples));

	/* workMem must be large enough for the minimal memtuples array */
	if (LACKMEM(state))
		elog(ERROR, "insufficient memory allowed for sort");

	state->currentRun = 0;

	/*
	 * maxTapes, tapeRange, and Algorithm D variables will be initialized by
	 * inittapes(), if needed
	 */

	state->result_tape = NULL;	/* flag that result tape has not been formed */

	MemoryContextSwitchTo(oldcontext);

	return state;
}

static int
comparetup_rum(const SortTuple *a, const SortTuple *b, RumTuplesortstate *state, bool compareItemPointer)
{
	RumSortItem *i1,
			   *i2;
	float8		v1 = DatumGetFloat8(a->datum1);
	float8		v2 = DatumGetFloat8(b->datum1);
	int			i;

	if (v1 < v2)
		return -1;
	else if (v1 > v2)
		return 1;

	i1 = (RumSortItem *) a->tuple;
	i2 = (RumSortItem *) b->tuple;
	for (i = 1; i < state->nKeys; i++)
	{
		if (i1->data[i] < i2->data[i])
			return -1;
		else if (i1->data[i] > i2->data[i])
			return 1;
	}

	if (compareItemPointer)
		return 0;

	/*
	 * If key values are equal, we sort on ItemPointer.
	 */
	if (i1->iptr.ip_blkid.bi_hi < i2->iptr.ip_blkid.bi_hi)
		return -1;
	else if (i1->iptr.ip_blkid.bi_hi > i2->iptr.ip_blkid.bi_hi)
		return 1;

	if (i1->iptr.ip_blkid.bi_lo < i2->iptr.ip_blkid.bi_lo)
		return -1;
	else if (i1->iptr.ip_blkid.bi_lo > i2->iptr.ip_blkid.bi_lo)
		return 1;

	if (i1->iptr.ip_posid < i2->iptr.ip_posid)
		return -1;
	else if (i1->iptr.ip_posid > i2->iptr.ip_posid)
		return 1;

	return 0;
}

static void
copytup_rum(RumTuplesortstate *state, SortTuple *stup, void *tup)
{
	RumSortItem *item = (RumSortItem *) tup;

	stup->datum1 = Float8GetDatum(state->nKeys > 0 ? item->data[0] : 0);
	stup->isnull1 = false;
	stup->tuple = tup;
	USEMEM(state, GetMemoryChunkSpace(tup));
}

static void
#if PG_VERSION_NUM >= 150000
writetup_rum(RumTuplesortstate *state, LogicalTape *unused, SortTuple *stup)
#else
writetup_rum(RumTuplesortstate *state, int tapenum, SortTuple *stup)
#endif
{
	RumSortItem *item = (RumSortItem *) stup->tuple;
	unsigned int writtenlen = RumSortItemSize(state->nKeys) + sizeof(unsigned int);

//	Assert(tapenum == state->tapenum);
	LogicalTapeWrite(TAPE(state->tapeset, state->result_tape),
					 (void *) &writtenlen, sizeof(writtenlen));
	LogicalTapeWrite(TAPE(state->tapeset, state->result_tape),
					 (void *) item, RumSortItemSize(state->nKeys));
	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeWrite(TAPE(state->tapeset, state->result_tape),
						 (void *) &writtenlen, sizeof(writtenlen));

	FREEMEM(state, GetMemoryChunkSpace(item));
	pfree(item);
}

static void
#if PG_VERSION_NUM >= 150000
readtup_rum(RumTuplesortstate *state, SortTuple *stup,
			LogicalTape *unused, unsigned int len)
#else
readtup_rum(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len)
#endif
{
	unsigned int tuplen = len - sizeof(unsigned int);
	RumSortItem *item = (RumSortItem *) palloc(RumSortItemSize(state->nKeys));

	Assert(tuplen == RumSortItemSize(state->nKeys));

	USEMEM(state, GetMemoryChunkSpace(item));
	LogicalTapeReadExact(TAPE(state->tapeset, state->result_tape),
						 (void *) item, RumSortItemSize(state->nKeys));
	stup->datum1 = Float8GetDatum(state->nKeys > 0 ? item->data[0] : 0);
	stup->isnull1 = false;
	stup->tuple = item;

	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeReadExact(TAPE(state->tapeset, state->result_tape),
							 &tuplen, sizeof(tuplen));
}

static void
reversedirection_rum(RumTuplesortstate *state)
{
//	state->reverse = !state->reverse;
	reversedirection(state);
}

static void
copytup_rumitem(RumTuplesortstate *state, SortTuple *stup, void *tup)
{
	stup->isnull1 = true;
	stup->tuple = palloc(sizeof(RumScanItem));
	memcpy(stup->tuple, tup, sizeof(RumScanItem));
	USEMEM(state, GetMemoryChunkSpace(stup->tuple));
}

static void
#if PG_VERSION_NUM >= 150000
writetup_rumitem(RumTuplesortstate *state, LogicalTape *unused, SortTuple *stup)
#else
writetup_rumitem(RumTuplesortstate *state, int tapenum, SortTuple *stup)
#endif
{
	RumScanItem *item = (RumScanItem *) stup->tuple;
	unsigned int writtenlen = sizeof(*item) + sizeof(unsigned int);

	LogicalTapeWrite(TAPE(state->tapeset, state->result_tape),
					 (void *) &writtenlen, sizeof(writtenlen));
	LogicalTapeWrite(TAPE(state->tapeset, state->result_tape),
					 (void *) item, sizeof(*item));
	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeWrite(TAPE(state->tapeset, state->result_tape),
						 (void *) &writtenlen, sizeof(writtenlen));

	FREEMEM(state, GetMemoryChunkSpace(item));
	pfree(item);
}

static void
#if PG_VERSION_NUM >= 150000
readtup_rumitem(RumTuplesortstate *state, SortTuple *stup,
			LogicalTape *unused, unsigned int len)
#else
readtup_rumitem(RumTuplesortstate *state, SortTuple *stup,
			int tapenum, unsigned int len)
#endif
{
	unsigned int tuplen = len - sizeof(unsigned int);
	RumScanItem *item = (RumScanItem *) palloc(sizeof(RumScanItem));

	Assert(tuplen == sizeof(RumScanItem));

	USEMEM(state, GetMemoryChunkSpace(item));
	LogicalTapeReadExact(TAPE(state->tapeset, state->result_tape),
						 (void *) item, tuplen);
	stup->isnull1 = true;
	stup->tuple = item;

	if (state->randomAccess)	/* need trailing length word? */
		LogicalTapeReadExact(TAPE(state->tapeset, state->result_tape),
							 &tuplen, sizeof(tuplen));
}


/* ------------------------------------------------------------------- */
/* Public functions */
/*
 * Get sort state memory context.  Currently it is used only to allocate
 * RumSortItem.
 */
MemoryContext
rum_tuplesort_get_memorycontext(RumTuplesortstate *state)
{
	return state->sortcontext;
}

RumTuplesortstate *
rum_tuplesort_begin_rum(int workMem, int nKeys, bool randomAccess,
						bool compareItemPointer)
{
	RumTuplesortstate *state = rum_tuplesort_begin_common(workMem, randomAccess);
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG,
			 "begin rum sort: nKeys = %d, workMem = %d, randomAccess = %c",
			 nKeys, workMem, randomAccess ? 't' : 'f');
#endif

	state->nKeys = nKeys;

	state->comparetup =  compareItemPointer ? comparetup_rum_true : comparetup_rum_false;

	state->copytup = copytup_rum;
	state->writetup = writetup_rum;
	state->readtup = readtup_rum;
//	state->reversedirection = reversedirection_rum;
//	state->reverse = false;

	MemoryContextSwitchTo(oldcontext);

	return state;
}

RumTuplesortstate *
rum_tuplesort_begin_rumitem(int workMem, FmgrInfo *cmp)
{
	RumTuplesortstate *state = rum_tuplesort_begin_common(workMem, false);
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG,
			 "begin rumitem sort: workMem = %d", workMem);
#endif

	//state->cmp = cmp;
	state->comparetup = comparetup_rum_false;
	state->copytup = copytup_rumitem;
	state->writetup = writetup_rumitem;
	state->readtup = readtup_rumitem;
	//state->reversedirection = reversedirection_rum;
	//state->reverse = false;

	MemoryContextSwitchTo(oldcontext);

	return state;
}

/*
 * rum_tuplesort_end
 *
 *	Release resources and clean up.
 *
 * NOTE: after calling this, any pointers returned by rum_tuplesort_getXXX are
 * pointing to garbage.  Be careful not to attempt to use or free such
 * pointers afterwards!
 */
void
rum_tuplesort_end(RumTuplesortstate *state)
{
	/* context swap probably not needed, but let's be safe */
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	long		spaceUsed;

	if (state->tapeset)
		spaceUsed = LogicalTapeSetBlocks(state->tapeset);
	else
		spaceUsed = (state->allowedMem - state->availMem + 1023) / 1024;
#endif

	/*
	 * Delete temporary "tape" files, if any.
	 *
	 * Note: want to include this in reported total cost of sort, hence need
	 * for two #ifdef TRACE_SORT sections.
	 */
	if (state->tapeset)
		LogicalTapeSetClose(state->tapeset);

#ifdef TRACE_SORT
	if (trace_sort)
	{
		if (state->tapeset)
			elog(LOG, "external sort ended, %ld disk blocks used: %s",
				 spaceUsed, pg_rusage_show(&state->ru_start));
		else
			elog(LOG, "internal sort ended, %ld KB used: %s",
				 spaceUsed, pg_rusage_show(&state->ru_start));
	}
#endif

	/* Free any execution state created for CLUSTER case */
	if (state->estate != NULL)
	{
		ExprContext *econtext = GetPerTupleExprContext(state->estate);

		ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
		FreeExecutorState(state->estate);
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Free the per-sort memory context, thereby releasing all working memory,
	 * including the Tuplesortstate struct itself.
	 */
	MemoryContextDelete(state->sortcontext);
}

RumSortItem *
rum_tuplesort_getrum(RumTuplesortstate *state, bool forward, bool *should_free)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	if (!rum_tuplesort_gettuple_common(state, forward, &stup, should_free))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	return (RumSortItem *) stup.tuple;
}

RumScanItem *
rum_tuplesort_getrumitem(RumTuplesortstate *state, bool forward, bool *should_free)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	if (!rum_tuplesort_gettuple_common(state, forward, &stup, should_free))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	return (RumScanItem *) stup.tuple;
}

/*
 * All tuples have been provided; finish the sort.
 */
void
rum_tuplesort_performsort(RumTuplesortstate *state)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG, "performsort starting: %s",
			 pg_rusage_show(&state->ru_start));
#endif

	switch (state->status)
	{
		case TSS_INITIAL:

			/*
			 * We were able to accumulate all the tuples within the allowed
			 * amount of memory.  Just qsort 'em and we're done.
			 */
			if (state->memtupcount > 1)
			{
				/* Can we use the single-key sort function? */
				if (state->onlyKey != NULL)
					qsort_ssup(state->memtuples, state->memtupcount,
							   state->onlyKey);
				else
					qsort_tuple(state->memtuples,
								state->memtupcount,
								state->comparetup,
								state);
			}
			state->current = 0;
			state->eof_reached = false;
			state->markpos_offset = 0;
			state->markpos_eof = false;
			state->status = TSS_SORTEDINMEM;
			break;

		case TSS_BOUNDED:

			/*
			 * We were able to accumulate all the tuples required for output
			 * in memory, using a heap to eliminate excess tuples.  Now we
			 * have to transform the heap to a properly-sorted array.
			 */
			sort_bounded_heap(state);
			state->current = 0;
			state->eof_reached = false;
			state->markpos_offset = 0;
			state->markpos_eof = false;
			state->status = TSS_SORTEDINMEM;
			break;

		case TSS_BUILDRUNS:

			/*
			 * Finish tape-based sort.  First, flush all tuples remaining in
			 * memory out to tape; then merge until we have a single remaining
			 * run (or, if !randomAccess, one run per tape). Note that
			 * mergeruns sets the correct state->status.
			 */
			dumptuples(state, true);
			mergeruns(state);
			state->eof_reached = false;
			state->markpos_block = 0L;
			state->markpos_offset = 0;
			state->markpos_eof = false;
			break;

		default:
			elog(ERROR, "invalid tuplesort state");
			break;
	}

#ifdef TRACE_SORT
	if (trace_sort)
	{
//		if (state->status == TSS_FINALMERGE)
//			elog(LOG, "performsort done (except %d-way final merge): %s",
//				 state->activeTapes,
//				 pg_rusage_show(&state->ru_start));
//		else
//			elog(LOG, "performsort done: %s",
//				 pg_rusage_show(&state->ru_start));
	}
#endif

	MemoryContextSwitchTo(oldcontext);
}

void
rum_tuplesort_putrum(RumTuplesortstate *state, RumSortItem * item)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	COPYTUP(state, &stup, (void *) item);

	puttuple_common(state, &stup);

	MemoryContextSwitchTo(oldcontext);
}

void
rum_tuplesort_putrumitem(RumTuplesortstate *state, RumScanItem * item)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);
	SortTuple	stup;

	/*
	 * Copy the given tuple into memory we control, and decrease availMem.
	 * Then call the common code.
	 */
	COPYTUP(state, &stup, (void *) item);

	puttuple_common(state, &stup);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Internal routine to fetch the next tuple in either forward or back
 * direction into *stup.  Returns false if no more tuples.
 * If *should_free is set, the caller must pfree stup.tuple when done with it.
 */
static bool
rum_tuplesort_gettuple_common(RumTuplesortstate *state, bool forward,
							  SortTuple *stup, bool *should_free)
{
	unsigned int tuplen;

	switch (state->status)
	{
		case TSS_SORTEDINMEM:
			Assert(forward || state->randomAccess);
			*should_free = false;
			if (forward)
			{
				if (state->current < state->memtupcount)
				{
					*stup = state->memtuples[state->current++];
					return true;
				}
				state->eof_reached = true;

				/*
				 * Complain if caller tries to retrieve more tuples than
				 * originally asked for in a bounded sort.  This is because
				 * returning EOF here might be the wrong thing.
				 */
				if (state->bounded && state->current >= state->bound)
					elog(ERROR, "retrieved too many tuples in a bounded sort");

				return false;
			}
			else
			{
				if (state->current <= 0)
					return false;

				/*
				 * if all tuples are fetched already then we return last
				 * tuple, else - tuple before last returned.
				 */
				if (state->eof_reached)
					state->eof_reached = false;
				else
				{
					state->current--;	/* last returned tuple */
					if (state->current <= 0)
						return false;
				}
				*stup = state->memtuples[state->current - 1];
				return true;
			}
			break;

		case TSS_SORTEDONTAPE:
			Assert(forward || state->randomAccess);
			*should_free = true;
			if (forward)
			{
				if (state->eof_reached)
					return false;
				if ((tuplen = getlen(TAPE2(state), true)) != 0)
				{
					READTUP(state, stup, state->result_tape, tuplen);
					return true;
				}
				else
				{
					state->eof_reached = true;
					return false;
				}
			}

			/*
			 * Backward.
			 *
			 * if all tuples are fetched already then we return last tuple,
			 * else - tuple before last returned.
			 */
			if (state->eof_reached)
			{
				/*
				 * Seek position is pointing just past the zero tuplen at the
				 * end of file; back up to fetch last tuple's ending length
				 * word.  If seek fails we must have a completely empty file.
				 */
				if (!LogicalTapeBackspace(TAPE(state->tapeset,
										  state->result_tape),
										  2 * sizeof(unsigned int)))
					return false;
				state->eof_reached = false;
			}
			else
			{
				/*
				 * Back up and fetch previously-returned tuple's ending length
				 * word.  If seek fails, assume we are at start of file.
				 */
				if (!LogicalTapeBackspace(TAPE(state->tapeset,
										  state->result_tape),
										  sizeof(unsigned int)))
					return false;
				tuplen = getlen(TAPE2(state), false);

				/*
				 * Back up to get ending length word of tuple before it.
				 */
				if (!LogicalTapeBackspace(TAPE(state->tapeset,
										  state->result_tape),
										  tuplen + 2 * sizeof(unsigned int)))
				{
					/*
					 * If that fails, presumably the prev tuple is the first
					 * in the file.  Back up so that it becomes next to read
					 * in forward direction (not obviously right, but that is
					 * what in-memory case does).
					 */
					if (!LogicalTapeBackspace(TAPE(state->tapeset,
											  state->result_tape),
											  tuplen + sizeof(unsigned int)))
						elog(ERROR, "bogus tuple length in backward scan");
					return false;
				}
			}

			tuplen = getlen(TAPE2(state), false);

			/*
			 * Now we have the length of the prior tuple, back up and read it.
			 * Note: READTUP expects we are positioned after the initial
			 * length word of the tuple, so back up to that point.
			 */
			if (!LogicalTapeBackspace(TAPE(state->tapeset,
									  state->result_tape),
									  tuplen))
				elog(ERROR, "bogus tuple length in backward scan");
			READTUP(state, stup, state->result_tape, tuplen);
			return true;

		case TSS_FINALMERGE:
			Assert(forward);
			*should_free = true;

			/*
			 * This code should match the inner loop of mergeonerun().
			 */
			if (state->memtupcount > 0)
			{
				int			srcTape = state->memtuples[0].srctape;
				Size		tuplen;
				int			tupIndex;
				SortTuple  *newtup;

				*stup = state->memtuples[0];
				/* returned tuple is no longer counted in our memory space */
				if (stup->tuple)
				{
					tuplen = GetMemoryChunkSpace(stup->tuple);
					state->availMem += tuplen;
					state->mergeavailmem[srcTape] += tuplen;
				}
				rum_tuplesort_heap_siftup(state, false);
				if ((tupIndex = state->mergenext[srcTape]) == 0)
				{
					/*
					 * out of preloaded data on this tape, try to read more
					 *
					 * Unlike mergeonerun(), we only preload from the single
					 * tape that's run dry.  See mergepreread() comments.
					 */
					mergeprereadone(state, srcTape);

					/*
					 * if still no data, we've reached end of run on this tape
					 */
					if ((tupIndex = state->mergenext[srcTape]) == 0)
						return true;
				}
				/* pull next preread tuple from list, insert in heap */
				newtup = &state->memtuples[tupIndex];
				state->mergenext[srcTape] = newtup->tupindex;
				if (state->mergenext[srcTape] == 0)
					state->mergelast[srcTape] = 0;
				rum_tuplesort_heap_insert(state, newtup, srcTape, false);
				/* put the now-unused memtuples entry on the freelist */
				newtup->tupindex = state->mergefreelist;
				state->mergefreelist = tupIndex;
				state->mergeavailslots[srcTape]++;
				return true;
			}
			return false;

		default:
			elog(ERROR, "invalid tuplesort state");
			return false;		/* keep compiler quiet */
	}
}

