package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"
import "strings"
import "regexp"
import "strconv"
// import "reflect"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	verse := regexp.MustCompile(`^[0-9]+:[0-9]+$`)
	l := list.New()

	words := strings.Fields(value)
	for _, word := range words{
	match := verse.MatchString(word)

	if ! match {
		word = strings.Trim(word, "(),.;!?:")
		l.PushBack(mapreduce.KeyValue{Key:word, Value:"1"})
	} 
	
	}
	return l
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	sum := 0
	for e := values.Front(); e != nil; e = e.Next() {
		count, _ := strconv.Atoi(e.Value.(string))
		sum += count
	}
	str := strconv.Itoa(sum)
	return str
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
	fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
	if os.Args[3] == "sequential" {
		mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
	} else {
		//args[2] is text file
		//args[3] is address and port localhost:7777
		mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])    
		// Wait until MR is done
		<- mr.DoneChannel
	}
	} else {
	mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
