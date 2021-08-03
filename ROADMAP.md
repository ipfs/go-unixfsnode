Roadmap
=======

The purpose of this repo is to offer ways to read and write UnixFSv1 structures using standard (go-ipld-prime) `ipld.Node` and `ipld.NodeAssembler`,
and also to "path" over these structures in natural ways.

This will be done by producing several [ADL](https://github.com/ipld/docs/blob/master/docs/advanced-layouts.md)s.

- ADL to make files appear as `kind`==`bytes`, where interally layout=trickledag:
	- [ ] Readable
	- [ ] Writable
		- [ ] Chunker as a callback/interface
	- (may choose to implement this as mostly shared code with the other file ADL, where the internal layout is just a parameter; undecided)
	- [ ] Upstream work to determine how `ipld.Node` and `ipld.NodeAssembler` should work for data in the size scale that requires streaming and seeking
- ADL to make files appear as `kind`==`bytes`, where interally layout=balanceddag:
	- [ ] Readable
	- [ ] Writable
		- [ ] Chunker as a callback/interface
	- (may choose to implement this as mostly shared code with the other file ADL, where the internal layout is just a parameter; undecided)
	- [ ] Upstream work to determine how `ipld.Node` and `ipld.NodeAssembler` should work for data in the size scale that requires streaming and seeking
- ADL to make unixfsv1 unsharded directories look like a regular `kind`==`map`:
	- (e.g. just walk the list of links and select something based on the name field inside the list as if that was a map key.)
	- [x] Readable
	- [ ] Writable
- ADL to traverse the HAMT which unixfsv1 uses for sharded directories as a `kind`==`map`:
	- [ ] Readable
	- [ ] Writable
	- (may be very similar to other HAMT libraries; may choose implement this in other repo with those, if more convenient/maintainable to do so; undecided)
	- (will probably use the above ADL for unsharded directories internally as necessary when reaching leaf nodes which have that format.)
- ADL to traverse unixfsv1 directories as a `kind`==`map`, and land directly on the next file/directory (rather than on its metadata):
	- [ ] Readable
	- there will *not* be a writable side to this!  Directories can't have entries added to them without metadata, so a symmetric write operation which is symmetric to this read operation is not defined.
	- (this is how natural pathing of the form `/ipfs/{cid}/xxx/yyy/zzz.ext` is served -- the `xxx` and `yyy` segments are served by lookups across this kind of ADL.)
	- (note how this and the above ADL _read the same substrate data_ -- while _interpreting it in different ways_.)

The above list is unordered.
(E.g. the features related to directory sharding may well be implemented before the file features; they're not interdependent.)


### Why do this as ADLs?

Several reasons.

In general, ADLs mean we can use these things with "plain" IPLD concepts, and that provides a lot of value.

- IPLD standard features such as `traverse.*` functions work over ADLs transparently.
  - That includes IPLD Selectors.  Selectors can just walk over an ADL map the same way they walk over a regular map.
- That means even very advanced systems like Graphsync will work transparently underneath these structures...
	- *without* needing additional complex code to have a special understanding of any of unixfsv1 data topology.
	- *without* needing to understand what a user is doing with the synthesized view of the data.
- When developing other filesystem-like systems in the future, this is a pattern of design that we can repeat!
	- Using this pattern doesn't guarantee total interoperability... but it makes it a lot more likely for it to naturally appear.



Non-goals
---------

There are a couple of feature groups which are _not_ intended to be part of the code in this repository,
either because they have a sufficiently distinctive scope that they live in other repos,
or because they're simply misfeatures:

- This repo doesn't concern itself with data transfer strategies.
	- When using `Reify()`-style methods, loading of data is a non-issue because you've already done it.
	- When in other scenarios, loading and storing data is handled by configuring a `ipld.LinkSystem`
	  (and more specifically, the `ipld.BlockReadOpener` and `ipld.BlockWriteOpener` function interfaces),
	  as is normal for systems based on go-ipld-prime.
		- ADLs that handle multiple blocks of data internally will take an `ipld.LinkSystem` as a parameter.
		- This should pair well with other data transfer strategies, such as Graphsync, Bitswap, or who knows what --
		  as long as it can provide `ipld.BlockReadOpener` and `ipld.BlockWriteOpener`, it should integrate without issue.
- These ADLs do not provide a way to generate Selectors that describe their internals.
	- Instead: supposing that you want this in order to do something like use Graphsync to request and transfer unixfsv1 data:
	  you can accomplish this by using Selectors *on top of* the ADLs,
	  and having your remote party agree to use the same ADLs in the same places as you both walk the data.
- Mapping to other filesystems.  (Whether that be the OS filesystem, or tar formats, or etc.)
	- These are valuable features, but not included in this repo.



Replaces
--------

This repo should effectively replace several other repos:

- `ipfs/go-unixfs`
- `ipfs/go-path`
- ... and probably several others.

This is a *generational* approach to code.
That is: this repo will provide semiotically comparable features to the repos it is replacing,
but this repo will *not* replace *every* feature in those repos,
and definitely will not contain exact replicas of those APIs.

This work will be ongoing.
There may be an intermediary phase where those repos will start using this code to do their work,
but before those repos are totally replaced.
During that time, new code is encouraged to try to use this repo alone where possible.
