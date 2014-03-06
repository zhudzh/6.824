package main

import "fmt"
// import "regexp"
// import "strconv"
// import "reflect"
// import "time"
import "sort"


func main() {

	arr := make([]int, 2)
	for idx := range arr{
		arr[idx] = -1
	}
	sort.Ints(arr)
	fmt.Println(arr)



}