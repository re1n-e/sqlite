package btree

import "unsafe"

type C struct {
	Tree  BTree
	Ref   map[string]string
	Pages map[uint64]BNode
}

func NewC() *C {
	pages := map[uint64]BNode{}
	return &C{
		Tree: BTree{
			Get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok, "New C 1")
				return node
			},
			New: func(node BNode) uint64 {
				assert(node.Nbytes() <= BTREE_PAGE_SIZE, "New,C 2")
				key := uint64(uintptr(unsafe.Pointer(&node.Data[0])))
				assert(pages[key].Data == nil, "New C 3")
				pages[key] = node
				return key
			},
			Del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok, "New C 4")
				delete(pages, ptr)
			},
		},
		Ref:   map[string]string{},
		Pages: pages,
	}
}

func (c *C) Add(key string, val string) {
	c.Tree.Insert([]byte(key), []byte(val))
	c.Ref[key] = val
}
func (c *C) Del(key string) bool {
	delete(c.Ref, key)
	return c.Tree.Delete([]byte(key))
}
