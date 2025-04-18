package btree

import "encoding/binary"

type BNode struct {
	Data []byte
}

const (
	BNODE_NODE = 1 // intrenal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

// header
func (node BNode) Btype() uint16 {
	return binary.LittleEndian.Uint16(node.Data)
}

func (node BNode) Nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.Data[2:4])
}

func (node BNode) SetHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.Data[0:2], btype)
	binary.LittleEndian.PutUint16(node.Data[2:4], nkeys)
}

// pointers
func (node BNode) GetPtr(idx uint16) uint64 {
	assert(idx < node.Nkeys(), "GetPtr")
	pos := HEADER + 8 * idx
	return binary.LittleEndian.Uint64(node.Data[pos:])
}

func (node BNode) SetPtr(idx uint16, val uint64) {
	assert(idx < node.Nkeys(), "SetPtr")
	pos := HEADER + 8 * idx
	binary.LittleEndian.PutUint64(node.Data[pos:],val)
}

// offset list
func OffsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.Nkeys(), "OffsetPos")
	return HEADER + 8 * node.Nkeys() + 2 * (idx - 1)
}

func (node BNode) GetOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.Data[OffsetPos(node, idx):])
}

func (node BNode) SetOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.Data[OffsetPos(node, idx):], offset)
}

// Key - values
func (node BNode) KvPos(idx uint16) uint16 {
	assert(idx <= node.Nkeys(), "KvPos")
	return HEADER + 8 * node.Nkeys() + 2 * node.Nkeys() + node.GetOffset(idx)
}

func (node BNode) GetKey(idx uint16) []byte {
	assert(idx < node.Nkeys(), "GetKey")
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos:])
	return node.Data[pos + 4 :][:klen]
}

func (node BNode) GetVal(idx uint16) []byte {
	assert(idx < node.Nkeys(), "GetVal")
	pos := node.KvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos + 0:])
	vlen := binary.LittleEndian.Uint16(node.Data[pos + 2:])
	return node.Data[pos + 4 + klen:][:vlen]
}

// node size in bytes
func (node BNode) Nbytes() uint16 {
	return node.KvPos(node.Nkeys())
}


