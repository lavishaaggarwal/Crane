package main

import (
	"log"
	"fmt"
	//"testing"
	"os/exec"
	"strings"
)

func myunittest(term string, expected [5]int) int {
	output_bytes, err:= exec.Command("go", "run", "main.go", term, "-n").CombinedOutput()
        out := string(output_bytes)
        if err != nil {
                fmt.Println("My error 1")
                log.Println(err)
		return 0
        } else {
                //fmt.Println(out1)
                a1 := strings.Count(out, "vm1.log")
                a2 := strings.Count(out, "vm2.log")
                a3 := strings.Count(out, "vm3.log")
                a4 := strings.Count(out, "vm4.log")
                a5 := strings.Count(out, "vm5.log")
                if a1==expected[0] && a2==expected[1] && a3==expected[2] && a4==expected[3] && a5==expected[4] {
                        fmt.Println("Correct output for ", term)
                } else {
                        fmt.Println("For ",term,"Expected",expected[0],expected[1],expected[2],expected[3],expected[4], "got", a1,a2,a3,a4,a5)
                }
        	return 1
	}
}
func main() {
	term1 := "dsda"
	e1 := [5]int{8, 8, 9, 8, 8}
	myunittest(term1, e1)
	
	term2 := "8400,LE4"
        e2 := [5]int{1,1,1,1,2}
	myunittest(term2, e2)
	
	term3 := "42.5**65"
        e3 := [5]int{665,732,729,691,657}
	myunittest(term3, e3)
	
	term4 := "BIPD"
        e4 := [5]int{15, 15, 15, 15, 15}
	myunittest(term4, e4)
}
