package btree

import (
	"bytes"
	"encoding/binary"
)

type BTree struct {
	// pointer (a non zero page number)
	Root uint64
	// callbacks for managaing on - disk pages
	Get func(uint64) BNode // derefrence a pointer
	New func(BNode) uint64 // allocate a new page
	Del func(uint64)       // deallocate a page
}

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert(node1max > BTREE_PAGE_SIZE)
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
func NodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.Nkeys()
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than /or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.GetKey(i), key)
		if cmp <= 0 {
			found = i
		} else {
			break
		}
	}
	return found
}

// add a new key to leaf node
func LeafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()+1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx, old.Nkeys()-idx)
}

func LeafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()+1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendKV(new, idx, 0, key, val)
	NodeAppendRange(new, old, idx+1, idx, old.Nkeys()-idx)
}

// copy multiple KVs into position
func NodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	assert(dstNew+n <= old.Nkeys())
	assert(dstNew+n <= new.Nkeys())
	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.SetPtr(dstNew, old.GetPtr(srcOld+i))
	}

	// offsets
	dstBegin := new.GetOffset(dstNew)
	srcBegin := old.GetOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.GetOffset(srcOld+i) - srcBegin
		new.SetOffset(dstNew+i, offset)
	}

	// KVS
	begin := old.KvPos(srcOld)
	end := old.KvPos(srcOld + n)
	copy(new.Data[new.KvPos(dstNew):], old.Data[begin:end])
}

// copy a KV into the position
func NodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.SetPtr(idx, ptr)
	// KVs
	pos := new.KvPos(idx)
	binary.LittleEndian.PutUint16(new.Data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.Data[pos+2:], uint16(len(val)))
	copy(new.Data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.SetOffset(idx+1, new.GetOffset(idx)+4+uint16((len(key)+len(val))))
}

// insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result Node
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{Data: make([]byte, 2*BTREE_PAGE_SIZE)}

	idx := NodeLookupLE(node, key)

	switch node.Btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.GetKey(idx)) {
			// found the key, update it
			LeafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position
			LeafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it into a kid node
		NodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

func NodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	// get and deallocate kid
	kptr := node.GetPtr(idx)
	knode := tree.Get(kptr)
	tree.Del(kptr)
	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := NodeSplit3(knode)
	// update the kid links
	NodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

func NodeSplit2(left BNode, right BNode, old BNode) {
	// Todo
}

// split a node if it's too big. the result are 1 - 3 node
func NodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.Nbytes() <= BTREE_PAGE_SIZE {
		old.Data = old.Data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might split
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	NodeSplit2(left, right, old)
	if left.Nbytes() <= BTREE_PAGE_SIZE {
		left.Data = left.Data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	NodeSplit2(leftleft, middle, left)
	assert(leftleft.Nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}

// replace a link with multiple links
func NodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.SetHeader(BNODE_NODE, old.Nkeys()+inc-1)
	NodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		NodeAppendKV(new, idx+uint16(i), tree.New(node), node.GetKey(0), nil)
	}
	NodeAppendRange(new, old, idx+inc, idx+1, old.Nkeys()-(idx+1))
}

// remove a key from a leaf node
func LeafDelete(new BNode, old BNode, idx uint16) {
	new.SetHeader(BNODE_LEAF, old.Nkeys()-1)
	NodeAppendRange(new, old, 0, 0, idx)
	NodeAppendRange(new, old, idx, idx+1, old.Nkeys()-(idx+1))
}

// delete a key from a tree
func TreeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := NodeLookupLE(node, key)
	switch node.Btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.GetKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		new := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		LeafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return NodeDelete(tree, node, idx, key)
	default:
		panic("bad mode")
	}
}

func NodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.GetPtr(idx)
	updated := TreeDelete(tree, tree.Get(kptr), key)
	if len(updated.Data) == 0 {
		return BNode{} // not found
	}
	tree.Del(kptr)

	new := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
	// check for merging
	mergeDir, sibling := ShouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		NodeMerge(merged, sibling, updated)
		tree.Del(node.GetPtr(idx - 1))
		NodeReplace2Kid(new, node, idx-1, tree.New(merged), merged.GetKey(0))
	case mergeDir > 0: // right
		merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		NodeMerge(merged, updated, sibling)
		tree.Del(node.GetPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.New(merged), merged.GetKey(0))
	case mergeDir == 0:
		assert(updated.Nkeys() > 0)
		NodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// merge 2 nodes into 1
func NodeMerge(new BNode, left BNode, right BNode) {
	new.SetHeader(left.Btype(), left.Nkeys()+right.Nkeys())
	NodeAppendRange(new, left, 0, 0, left.Nkeys())
	NodeAppendRange(new, right, left.Nkeys(), 0, right.Nkeys())
}

func ShouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.Nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.Get(node.GetPtr(idx - 1))
		merged := sibling.Nbytes() + updated.Nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.Nkeys() {
		sibling := tree.Get(node.GetPtr(idx + 1))
		merged := sibling.Nbytes() + updated.Nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{}
}

func (tree * Btree) Delete(key []byte) bool {
	
}