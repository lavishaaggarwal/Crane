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
	"os/exec"
	"time"
	//"bufio"
	//"sync"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"hash/fnv"
)

const (
	port = ":50053"
	local_add = "localhost:50053"
	// vm9_add = "172.22.156.101:50051"
	introducer_add = "172.22.156.101:50053"
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
var leader = port

func main() {

	leaderinfo, errl := ioutil.ReadFile("./../leader.txt") // just pass the file name
	if errl != nil {
		fmt.Print(errl)
	}
	//Parse the address into each server's address
	sl := strings.Split(string(leaderinfo), "\n")
	leader = sl[0]+port
	fmt.Println("Leader is:", leader)



	//Input from the client
	//Flag 1: Join and 2:leave 4:Put
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

	if strings.Compare(string(os.Args[1]), "get")== 0{

     //introducer_add = leader
	    conn, err := grpc.Dial(leader, grpc.WithInsecure())
	    if err != nil {
	             fmt.Print("Failed to dail")
	             log.Fatalf("did not connect: %v", err)
	    }
	    defer conn.Close()
	    c := pb.NewGreeterClient(conn)

	    // Set timeout
	    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel()

	    fmt.Println("WE WILL call MyGet")
	    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: string(os.Args[2])})
	    fmt.Println(r, err)


	    var myfeature string
	    for{
		
		    feature, err1:= r.Recv()

			if(feature == nil){
					break
			}
				
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
				
			}


		    fmt.Println(feature, err1)
		    myfeature +=feature.Message
	    
		}
	    //Write feature.Message intop os.Args[3]
	    part := strings.Split(myfeature, "FOUND#")
	    if strings.Contains(part[0], "YES"){
	    	fmt.Println(part[0])
		    fmt.Println("We just called myget")
		    file, err3 := os.Create("./../ldfs/"+string(os.Args[3]))
		    if err3 != nil {
		            log.Println("ERROR creating file", err3)
		    }
		    defer file.Close()

		    file1, err4 := os.OpenFile("./../ldfs/"+string(os.Args[3]), os.O_RDWR, 0644)
		    if err4 != nil {
		            log.Println("ERROR opening file", err4)
		    }
		    defer file1.Close()

		    _, err5 := file1.WriteString(string(part[1]))
		    if err5 != nil {
		            log.Println("ERROR writing into the file", err5)
		    }

	    } else{
	    	fmt.Println(part[0])
	    	fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
	    }

	    return
   } 



	if strings.Compare(string(os.Args[1]), "delete")== 0{

	     //introducer_add = leader
	     conn, err := grpc.Dial(leader, grpc.WithInsecure())
	     if err != nil {
	             fmt.Print("Failed to dail")
	             log.Fatalf("did not connect: %v", err)
	    }
	    defer conn.Close()
	    c := pb.NewGreeterClient(conn)

	    // Set timeout
	    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel()

	    r, err := c.MyDelete(ctx, &pb.HelloRequest{Name: string(os.Args[2])})
	    fmt.Println(r, err)
	    feature, err1:= r.Recv()
	    fmt.Println(feature.Message, err1)
	    return

	} 


	if strings.Compare(string(os.Args[1]), "get-versions")== 0{

	     //introducer_add = leader
	     conn, err := grpc.Dial(leader, grpc.WithInsecure())
	     if err != nil {
	             fmt.Print("Failed to dail")
	             log.Fatalf("did not connect: %v", err)
	    }
	    defer conn.Close()
	    c := pb.NewGreeterClient(conn)

	    // Set timeout
	    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel()

	    send_data := string(os.Args[2]) + "#" + string(os.Args[3]) + "#" +  string(os.Args[4]) + "#" + my_add
	    //sdfsfilename#versions#localfilename
	    r, err := c.MyGetversions(ctx, &pb.HelloRequest{Name: send_data})


	    // fmt.Println(r, err)
	    // feature, err1:= r.Recv()
	    // fmt.Println(feature, err1)

	    var version_output string
		for {
	      			//receive the streamed data
				feature, err1:= r.Recv()
				if(feature == nil){
					break
				}
				// fmt.Println(feature.Message)
				
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
					
				}
				fmt.Println(len(feature.Message))
				version_output += feature.Message
			}
			all_versions := strings.Split(version_output, "NEW_VERSION###")
			all_versions = all_versions[0:len(all_versions)-1]
			
		fmt.Println("Fetched ", len(all_versions), " versions")
		get_versions := string(os.Args[3])
		get_v_num, errv := strconv.Atoi(get_versions)
		if errv!=nil{
			fmt.Println(errv)
		}
		for i:=0; i<len(all_versions); i++{

			num_version := get_v_num-i
			fmt.Println("Version number", num_version)

			fname := "./../ldfs/"+string(os.Args[4])+"_v"+strconv.Itoa(num_version)
			file, err3 := os.Create(fname)
		    if err3 != nil {
		            log.Println("ERROR creating file", err3)
		    }
		    defer file.Close()

		    file1, err4 := os.OpenFile(fname, os.O_RDWR, 0644)
		    if err4 != nil {
		            log.Println("ERROR opening file", err4)
		    }
		    defer file1.Close()

		    _, err5 := file1.WriteString(all_versions[i])
		    if err5 != nil {
		            log.Println("ERROR writing into the file", err5)
		    }
		 }

	    return

	}

	if strings.Compare(string(os.Args[1]), "ls")== 0{

	     //introducer_add = leader
	     conn, err := grpc.Dial(leader, grpc.WithInsecure())
	     if err != nil {
	             fmt.Print("Failed to dail")
	             log.Fatalf("did not connect: %v", err)
	    }
	    defer conn.Close()
	    c := pb.NewGreeterClient(conn)

	    // Set timeout
	    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel()

	    r, err := c.MyLs(ctx, &pb.HelloRequest{Name: string(os.Args[2])})
	    fmt.Println(r, err)
	    feature, err1:= r.Recv()
	    fmt.Println("Result of LS is ", feature.Message)
	    fmt.Println(err1)
	    return

	} 

	if strings.Compare(string(os.Args[1]), "store") == 0{

        output_files, err := exec.Command("ls", "./../sdfs/").Output()
        if err != nil {
                fmt.Println("ls error")
                log.Println(err)
        }
        output := string(output_files)
        fmt.Println("List of files are")
        fmt.Println(output)
        return

	}


	if strings.Compare(flag, "4")==0 {
		localfilename := os.Args[2]
		sdfsfilename := os.Args[3]
	
		fmt.Println("Okay! I will put"+ localfilename + " as " + sdfsfilename)
		conn, err := grpc.Dial(leader, grpc.WithInsecure())
	    if err != nil {
	         fmt.Print("Failed to dail")
	         log.Fatalf("did not connect: %v", err)
	    }
	    defer conn.Close()
	    c := pb.NewGreeterClient(conn)

	    local_fpath := "./../ldfs/"+localfilename


	 //    filetext, err6 := ioutil.ReadFile(local_fpath)
		// 	if err6 != nil {
		// 		panic(err6)
		// }
	    // Set timeout
	    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel()
		// r, err := c.IntroPut(ctx, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(filetext)})

		r, err := c.IntroPut(ctx, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+local_fpath})
		//r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "VM8;"+my_add+";" })

		if err != nil {
			//log.Fatalf("could not greet: %v", err)
			fmt.Println("Could not greet the leader for introput!!",err)
		} else {	
			//If it is able to contact the introducer, then only run its server		   		
			// for {
			// 	//receive the streamed data
			// 	feature, err1:= r.Recv()
		
			// 	if err1 == io.EOF {
			// 		fmt.Println("EOF Error")
			// 		//break
			// 	}
			// 	if err1 != nil {
			// 		fmt.Println("ERROR")
			// 	} else{
			// 		fmt.Println("Received nodes to put this file on: \n", feature.Message)
			// 	}
			// }

	 		// var myfeature string
		    for{
			
			    feature, err1:= r.Recv()

				// if(feature == nil){
				// 		break
				// }
					
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR", err1)
					
				} else{
					fmt.Println("Received nodes to put this file on: \n", feature.Message)
				    // fmt.Println(feature, err1)
				    // myfeature +=feature.Message
				}		    
			}

		}
		return
	}


	if strings.Compare(string(os.Args[1]), "1")== 0{
		files, err := filepath.Glob("./../sdfs/*")
		if err != nil {
		    panic(err)
		}
		for _, f := range files {
		    if err := os.Remove(f); err != nil {
		        panic(err)
		    }
		}

	}

	if strings.Compare(string(os.Args[1]), "1")== 0 || strings.Compare(string(os.Args[1]), "2")== 0  || strings.Compare(string(os.Args[1]), "3")== 0 {
		//do nothing
	} else{
		fmt.Println("Incorrect argument")
		return
	}

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

				mem_leader := strings.Split(feature.Message, "###")


				introducer_memlist := []byte(mem_leader[0])

	            f, err2 := os.Create("./../membership_list.txt")
	            if err2 != nil {
	                panic(err2)
	            }
	            f.Write(introducer_memlist)


	            myleader := strings.Split(mem_leader[1], "\n")[0]
				leads := []byte(myleader)

	            fl, err2l := os.Create("./../leader.txt")
	            if err2l != nil {
	                panic(err2l)
	            }
	            fl.Write(leads)
	            leader = mem_leader[1] + port
	            fmt.Println("Leader is ", leader)
				


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