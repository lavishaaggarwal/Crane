/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package main

import (
	"log"
	"os"
	"time"
	//"bufio"
	"sync"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

const (
	//address     = "localhost:50051"
	defaultName = "world"
)

func main() {

	// Read in server addresses from a file
        address, err := ioutil.ReadFile("server_addresses.txt") // just pass the file name
        if err != nil {
                fmt.Print(err)
        }
	
	//Input from the client
	text := os.Args[1]
	grep_flags := os.Args[2]
	
	//Parse the address into each server's address
	s := strings.Split(string(address), "\n")
	s = s[:len(s)-1]
	
	// Get the results from all the servers
	var length = len(s)

	//start synchronization
	var wg sync.WaitGroup
	wg.Add(length)

	fmt.Println("Started running in parallel\n")
	
	for i:=0; i<length; i++{
	
		//concurrent processing
		go func(i int){
			
			defer wg.Done()
			
			// Set up a connection to the server.
			conn, err := grpc.Dial(s[i], grpc.WithInsecure())
			if err != nil {
				fmt.Print("Failed to dail")
				log.Fatalf("did not connect: %v", err)
			}
			defer conn.Close()
			c := pb.NewGreeterClient(conn)
			
			// Set timeout
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()
			
			//call the function on the server
			r, err := c.SayHello(ctx, &pb.HelloRequest{Name: text+"#"+grep_flags})
			if err != nil {
				//log.Fatalf("could not greet: %v", err)
				fmt.Println("Could not greet",err)
			} else {			
		        		
	      		for {
	      			//receive the streamed data
				feature, err1:= r.Recv()
				if(feature == nil){
					break
				}
				fmt.Println(feature.Message)
				
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
					
				}
				fmt.Println(len(feature.Message))
			}
			}	
				
		}(i)	
	}
	wg.Wait()
	fmt.Println("Stopped parallel process\n")
}
