PageRank
========
Blocked Computation for Page Rank using mapreduce:
We can achieve much better
convergence by partitioning the Web graph into Blocks, and letting each Reduce task operate on
an entire Block at once, propagating data along multiple edges within the Block. The idea is that
each Reduce task loads its entire Block into memory and does multiple inmemory
PageRank
iterations on the Block, possibly even iterating until the Block has converged. In principle, the
Reducer needs to know
1] The set of vertices of its Block, with their current PageRank values and their lists of
outgoing edges;
2] The set of edges entering the block from outside, with the current PageRank value that
flows along each entering edge.
Your Map tasks will need to produce this information and send it to the Reduce tasks. In some
cases information about a single Node or Edge will have to be sent to more than one Reduce
task.
