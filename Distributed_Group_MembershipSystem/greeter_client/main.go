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
	//"sync"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"hash/fnv"
)

const (

	local_add = "localhost:50052"
	// vm9_add = "172.22.156.101:50051"
	introducer_add = "172.22.156.101:50052"
	// VM 9 is the introducer; VM9 IP - 172.22.156.101
	// my_add = "172.22.154.101:50051"
	// vm_name = "VM8"
	// defaultName = "world"
)

func hash(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}
var my_add string
var vm_name string

func main() {


	//Input from the client
	//Flag 1: Join and 2:leave
	flag := os.Args[1]


	// Read in server addresses from a file
	address, err := ioutil.ReadFile("./../../vm_info.txt") // just pass the file name
	if err != nil {
		fmt.Print(err)
	}
	//Parse the address into each server's address
	s := strings.Split(string(address), "\n")
	my_add = s[0]
	vm_name = s[1]

	fmt.Println("\n"+my_add + vm_name + "\n")


	// For the introducer node, remove the existing membership list and create a new blac=nk file
	if strings.Contains(my_add, introducer_add){
		var err2 = os.Remove("./../membership_list.txt")
		if err2 != nil {
			fmt.Println("ERROR renaming file", err2)
		}

		file, err3 := os.Create("./../membership_list.txt")
		if err3 != nil {
			fmt.Println("ERROR creating file", err3)
		}
		defer file.Close()
	}




	//Make a connection with the introdocer
    conn, err := grpc.Dial(introducer_add, grpc.WithInsecure())
    if err != nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)

    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()


    timestamp := string(time.Now().Format("20060102150405"))
    hash_add := hash(my_add+timestamp)
    fmt.Println("My new hash Id will be:" + fmt.Sprint(hash_add)+"\n")

	my_info := vm_name+";"+fmt.Sprint(hash_add)+";"+my_add+";"

	 r, err := c.SayHello(ctx, &pb.HelloRequest{Name: my_info + "#" + string(flag)})
	//r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "VM8;"+my_add+";" })

	var present string

	if err != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the Introducer!!",err)
	} else {	
		//If it is able to contact the introducer, then only run its server		   		
		//for {
			//receive the streamed data
			feature, err1:= r.Recv()
			present = string(feature.Message)
			// fmt.Println("present=", present)

			/*if(feature == nil){
				fmt.Println("Nil")
				break
			}*/
			// fmt.Println(r)
			fmt.Println("Received introducer's memlist: \n", feature.Message)
			
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				//break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}

			// If it is not already present in the introducer do:
			// Create its new membership list and start its server


			// If the node is present or it wants to leave the group
			if !(strings.Contains(present, "Present")) {

				introducer_memlist := []byte(feature.Message)

	            f, err2 := os.Create("./../membership_list.txt")
	            if err2 != nil {
	                panic(err2)
	            }
	            f.Write(introducer_memlist)
				//defer f.close()

				//fmt.Println(len(feature.Message))
				// Start its server
				conn, err = grpc.Dial(my_add, grpc.WithInsecure())
				if err != nil {
					fmt.Print("Failed to dail")
					log.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				c = pb.NewGreeterClient(conn)

				var r1 pb.Greeter_SayRepeatClient
				if strings.Compare(flag, "1")==0 {
					fmt.Println("Starting " + vm_name + " server\n")
					r1, err2 = c.SayRepeat(ctx, &pb.HelloRequest{Name: "JOIN&Please Start--"+ my_info})
				} else if strings.Compare(flag, "2")==0 {
					fmt.Println("I am leaving the group!\n")
					r1, err2 = c.SayRepeat(ctx, &pb.HelloRequest{Name: "LEAVE&"+ my_info})
				} else{
					fmt.Println("Starting " + vm_name + " server\n")
					r1, err2 = c.SayRepeat(ctx, &pb.HelloRequest{Name: "JOIN&Please Start(PACKET_LOSS)--"+ my_info})				
				}


				if err2 != nil {
					//log.Fatalf("could not greet: %v", err)
					fmt.Println("Could not greet",err)
				} else {				        		
					// for {
					// 	//receive the streamed data
					feature, err3:= r1.Recv()
					// if(feature == nil){
					// 	break
					// }
					fmt.Println("Received: "+feature.Message+"\n")
					
					// if err3 == io.EOF {
					// 	fmt.Println("EOF Error")
					// 	break
					// }
					if err3 != nil {
						fmt.Println("ERROR")
					}
					
				}
			}	
		// }
	}			
}