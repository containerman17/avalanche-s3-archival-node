//go:build exp

package main

import (
	"fmt"
	"os"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/store"
)

func main() {
	cas, err := dist.Local(os.Args[1])
	if err != nil {
		panic(err)
	}
	for _, name := range os.Args[2:] {
		f, err := store.ReadFooter(cas, name)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s v%d chain %d state %d lookup %d\n", name[:12], f.Version, f.Len[0], f.Len[1], f.Len[2])
	}
}
