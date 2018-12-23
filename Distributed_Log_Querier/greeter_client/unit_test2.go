package main

import (
	"log"
	"fmt"
	//"testing"
	"os/exec"
	"strings"
)

func myunittest_count(term string, expected [5]int) int {
	output_bytes, err:= exec.Command("go", "run", "main.go", term, "-n").CombinedOutput()
        out := string(output_bytes)
        if err != nil {
                fmt.Println("My error 1")
                log.Println(err)
		return 0
        } else {
                //fmt.Println(out)
		filename := "machine."//"vm"
                a1 := strings.Count(out, filename + "1.log")
                a2 := strings.Count(out, filename + "2.log")
                a3 := strings.Count(out, filename + "3.log")
                a4 := strings.Count(out, filename + "4.log")
                a5 := strings.Count(out, filename + "5.log")
                if a1==expected[0] && a2==expected[1] && a3==expected[2] && a4==expected[3] && a5==expected[4] {
                        fmt.Println("Correct output for ", term)
                } else {
                        fmt.Println("For ",term,"Expected",expected[0],expected[1],expected[2],expected[3],expected[4], "got", a1,a2,a3,a4,a5)
                }
        	return 1
	}
}

func myunittest_line(term string, expected []string) int {
        output_bytes, err:= exec.Command("go", "run", "main.go", term, "-n").CombinedOutput()
        out := string(output_bytes)
        if err != nil {
                //fmt.Println("My error 1")
                log.Println(err)
                return 0
        } else {
                //fmt.Println(out)
                filename := "machine."//"vm"
		for i := 0; i<len(expected); i++{
			a := strings.Count(out, filename + expected[i])
			if a == 1 {
				fmt.Println("Correct output for ", term, " for ", expected[i])
			} else {
				fmt.Println("For ", term, "Expected ", expected[i], " but did not get it.")
			}
        	}
		return 1
	}
}
func main() {
	term1 := "4big"
	e1 := [5]int{1, 1, 1, 3, 1}
	myunittest_count(term1, e1)
	l1 :=[]string{"4.log : 1", "4.log : 2"}	
	myunittest_line(term1, l1)

	term2 := "One"
        e2 := [5]int{2,2,2,2,2}
	myunittest_count(term2, e2)
	l2 :=[]string{"1.log : 1", "2.log : 1", "3.log : 1", "4.log : 1", "5.log : 1"}	
	myunittest_line(term2, l2)
	
	term3 := "big"
        e3 := [5]int{4,4,4,4,4}
	myunittest_count(term3, e3)
	l3 :=[]string{"1.log : 1", "1.log : 2", "1.log : 5", "2.log : 1", "2.log : 2", "2.log : 5","3.log : 1", "3.log : 2", "3.log : 5","4.log : 1", "4.log : 2", "4.log : 5", "5.log : 1", "5.log : 2", "5.log : 5"}
	myunittest_line(term3, l3)
	
	term4 := "10minutes"
        e4 := [5]int{1,1,1,1,1}
	myunittest_count(term4, e4)

	term5 := "2hour"
        e5 := [5]int{1,2,1,1,1}
        myunittest_count(term5, e5)
	l5 :=[]string{"2.log : 3"}
	myunittest_line(term5, l5)	


        term6 := "0elephant"
        e6 := [5]int{1,2,1,2,1}
        myunittest_count(term6, e6)
	l6 := []string{"2.log : 4", "4.log : 4"}
	myunittest_line(term6, l6)

        term7 := "1kid"
        e7 := [5]int{2,1,1,2,1}
        myunittest_count(term7, e7)
	l7 :=[]string{"1.log : 4", "4.log : 4"}
	myunittest_line(term7, l7)	

}	
