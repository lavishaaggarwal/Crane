// Reading and writing files are basic tasks needed for
// many Go programs. First we'll look at some examples of
// reading files.

package main

import (
    // "bufio"
    "fmt"
    // "io"
    "io/ioutil"
    "os"
    "strings"
)

// Reading files requires checking most calls for errors.
// This helper will streamline our error checks below.
func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {

    address, err := ioutil.ReadFile("./../../vm_info.txt") // just pass the file name
    if err != nil {
        fmt.Print(err)
    }
    //Parse the address into each server's address
    s := strings.Split(string(address), "\n")
    my_add := s[0]
    // vm_name := s[1]


    var filename = "./../membership_list.txt"
    //Open the Introducer;s membership list
    var f, err1 = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
    if err1 != nil {
        panic(err1)
        fmt.Println(f)
    }
    //Read the introducer's membership list
    file_check, err_check := ioutil.ReadFile(filename)
    if err_check != nil {
        panic(err_check)
    }


    var neighbours = strings.Split(string(file_check), "\n")
    neighbours = neighbours[0:len(neighbours)-1]
    for j := 0; j < len(neighbours); j++ {

            split_ninfo := strings.Split(neighbours[j], ";")
            //Check if the current neighbour is itself
            if strings.Contains(split_ninfo[2], my_add) && (strings.Contains(split_ninfo[4], "A") ||  strings.Contains(split_ninfo[4], "S")){
                fmt.Println("My RING ID: ", split_ninfo[1])
            } 
    }
    fmt.Print("My MEMBERSHIP LIST:\n")
    fmt.Println(string(file_check))
    // defer file_check.Close()
}
