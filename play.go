package main

import "fmt"
// import "regexp"
import "strconv"
// import "reflect"
import "time"

func main() {
	// one go func that adds to workers
	workers := make(map[string]bool)
	fmt.Println("got here")
	go func() {
		fmt.Println("got here")
		for i:=0; i < 1000; i++{
			workers[strconv.Itoa(i)] = true
		}
	}()

	fmt.Println(workers)

	go func() {
		fmt.Println("got here2")
		time.Sleep(200)
		for worker_info, _ := range workers {
			fmt.Println(worker_info)
		}
	}()

}