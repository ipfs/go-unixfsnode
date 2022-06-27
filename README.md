# go-unixfsnode

This is an IPLD ADL that provides string based pathing for protobuf nodes. The top level node behaves like a map where LookupByString returns the Hash property on the Link in the protobufs list of Links whos Name property matches the key. This should enable selector traversals that work based of paths.

Note that while it works internally with go-codec-dagpb, the `Reify()` method (used to get a UnixFSNode from a DagPB node) should actually work successfully with any schema-compliant go-ipld-prime `Node`. Using `ReifyDagPB()` will require that the incoming `Node` be of type `PBNode` from go-codec-dagpb.

## License

Apache-2.0/MIT © Protocol Labs
