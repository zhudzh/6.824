package main

import "fmt"
// import "regexp"
import "strconv"
import "reflect"


func main() {
	str := "1"
	num, _ := strconv.Atoi(str)
	fmt.Println(reflect.TypeOf(string(num)))
	fmt.Println(num)
	fmt.Println(strconv.Itoa(num))
}