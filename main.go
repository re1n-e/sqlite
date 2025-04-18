package main

import (
	"database/btree"
	"fmt"
)

func main() {
	// Create a new in-memory B-tree
	c := btree.NewC()

	// Add some key-value pairs
	c.Add("apple", "red fruit")
	c.Add("banana", "yellow fruit")
	c.Add("cherry", "small red fruit")
	c.Add("date", "sweet brown fruit")
	c.Add("elderberry", "small black fruit")

	// Print the B-tree structure
	fmt.Printf("Tree has %d pages\n", len(c.Pages))

	// Delete a key
	deleted := c.Del("cherry")
	fmt.Printf("Deleted 'cherry': %v\n", deleted)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value for %s", key)
		c.Add(key, value)
	}

	fmt.Printf("After adding 100 items, tree has %d pages\n", len(c.Pages))
}
