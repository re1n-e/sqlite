package btree

func assert(a bool, message string) {
	if !a {
		panic(message)
	}
}