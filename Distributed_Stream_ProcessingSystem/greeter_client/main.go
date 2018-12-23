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
	"math"
	//"strconv"
	"io"
	"io/ioutil"
	"path/filepath"
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
	// introducer_add = "172.22.156.101:50053"//vm9
	// introducer_add = "172.22.154.101:50053"//vm8
	introducer_add = "172.22.158.100:50053"//vm7
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

	// leader = introducer_add


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




	 if strings.Compare(string(os.Args[1]), "task1") == 0{

        file := string(os.Args[2])

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

        fmt.Println("WE WILL call TaskOneSpout")
        r, err := c.TaskOneSpout(ctx, &pb.HelloRequest{Name: file})
        // fmt.Println(r, err)
        // feature, err1:= r.Recv()
        // fmt.Println(feature.Message, err1)

        var ans string
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
				// fmt.Println(feature.Message)
				
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
					
				}
				// fmt.Println(len(feature.Message))
				ans += feature.Message
			}
		}	


		fmt.Println(ans)


        fmt.Println("We just called taskonespout")

        output_file := "./../ldfs/taskone_output.txt"
        file_o, err3 := os.Create(output_file)
	    if err3 != nil {
	            log.Println("ERROR creating file", err3)
	    }
	    defer file_o.Close()

	    file1, err4 := os.OpenFile(output_file, os.O_RDWR, 0644)
	    if err4 != nil {
	            log.Println("ERROR opening file", err4)
	    }
	    defer file1.Close()

	    _, err5 := file1.WriteString(ans)
	    if err5 != nil {
	            log.Println("ERROR writing into the file", err5)
	    }

        return
    }

	 if strings.Compare(string(os.Args[1]), "task2") == 0{

        file := string(os.Args[2])

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

        fmt.Println("WE WILL call TaskTwoSpout")
        r, err := c.TaskTwoSpout(ctx, &pb.HelloRequest{Name: file})
        // fmt.Println(r, err)
        // feature, err1:= r.Recv()
        // fmt.Println(feature.Message, err1)

        var ans string
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
				// fmt.Println(feature.Message)
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				fmt.Println(feature.Message, "\n \n")
				ans += feature.Message
			}
		}	
		// fmt.Println(ans)


        fmt.Println("We just called tasktwospout")

     //    output_file := "./../ldfs/"+string(file)+"tasktwo_output.txt"
     //    file_o, err3 := os.Create(output_file)
	    // if err3 != nil {
	    //         log.Println("ERROR creating file", err3)
	    // }
	    // defer file_o.Close()

	    // file1, err4 := os.OpenFile(output_file, os.O_RDWR, 0644)
	    // if err4 != nil {
	    //         log.Println("ERROR opening file", err4)
	    // }
	    // defer file1.Close()

	    // _, err5 := file1.WriteString(ans)
	    // if err5 != nil {
	    //         log.Println("ERROR writing into the file", err5)
	    // }

        return
    }



	 if strings.Compare(string(os.Args[1]), "task3") == 0{
        file := string(os.Args[2])
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
        fmt.Println("WE WILL call TaskThreeSpout")
        r, err := c.TaskThreeSpout(ctx, &pb.HelloRequest{Name: file})
        // fmt.Println(r, err)
        // feature, err1:= r.Recv()
        // fmt.Println(feature.Message, err1)
        var ans string
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
				// fmt.Println(feature.Message)
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				fmt.Println(feature.Message, "\n \n")
				ans += feature.Message
			}
		}	
        fmt.Println("We just called taskthreespout")
	        return
    }


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
	    if err!=nil{
	    	fmt.Println(r, err)
	    }
	    feature, err1:= r.Recv()
	    if err1!=nil{
	    	fmt.Println(err1)
	    }
	    fmt.Println(feature)
	    //Write feature.Message intop os.Args[3]
	    part := strings.Split(feature.Message, "FOUND#")
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
	    if err !=nil{
	    	fmt.Println(err)
	    }
	    fmt.Println(r)
	    feature, err1:= r.Recv()
	    if err1!=nil{
	    	fmt.Println(err1)
	    }
	    fmt.Println(feature.Message)
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
	    if err!=nil{
	    	fmt.Println(err)
	    }
	    fmt.Println(r)
	    feature, err1:= r.Recv()
	    if err1!=nil{
	    	fmt.Println(err1)
	    }
	    fmt.Println(feature)
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
	    filetext, err6 := ioutil.ReadFile(local_fpath)
			if err6 != nil {
				panic(err6)
		}

	    // Set timeout
	    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel()
		r, err := c.IntroPut(ctx)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(filetext)})
		//r, err := c.IntroPut(ctx, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(filetext)})
		//r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "VM8;"+my_add+";" })

		if err != nil {
			//log.Fatalf("could not greet: %v", err)
			fmt.Println("Could not greet the leader for introput!!",err)
		} else {
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}

			var len_formout = float64(len(string(filetext)))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = string(filetext)[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
				fmt.Println("Sending next")
			}

			// for k:=0; k<7; k++{
			// 	var send_data pb.HelloRequest
			// 	send_data.Name = "word "+ strconv.Itoa(k) + "PUT#"+my_add+"#"+sdfsfilename + "#"+string(filetext)
			// 	if errq := r.Send(&send_data); errq != nil {
			// 		log.Fatalf("Failed to send a note: %v", errq)
			// 	}
			// }


			// for _, note := range notes {
			// 	if err := r.Send(note); err != nil {
			// 		log.Fatalf("Failed to send a note: %v", err)
			// 	}
			// }
			r.CloseSend()



			//If it is able to contact the introducer, then only run its server		   		
			//for {
				//receive the streamed data
				feature, err1:= r.Recv()
				// present = string(feature.Message)
				// fmt.Println("present=", present)

				/*if(feature == nil){
					fmt.Println("Nil")
					break
				}*/
				// fmt.Println(r)
				//fmt.Println("Received nodes to put this file on: \n", feature.Message)
			/*	all_put_nodeid := strings.Split(feature.Message, "#")
				all_put_nodeid = all_put_nodeid[1:len(all_put_nodeid)-1]
				for i := 0; i < len(all_put_nodeid); i++ {
					node := all_put_nodeid[i]
					node_ip := strings.Split(node, ";")[2]
					fmt.Println("Putting it on", node_ip)

					//Establish a connection to this node and make a replica on this
					node_ip = introducer_add
					conn2, err2 := grpc.Dial(node_ip, grpc.WithInsecure())
				    if err2 != nil {
				         fmt.Print("Failed to dail")
				         log.Fatalf("did not connect: %v", err2)
				    }
				    defer conn2.Close()
				    c2 := pb.NewGreeterClient(conn2)

				    // Set timeout
				    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				    defer cancel()
				    var mytext string
				    // var filetext string

				    local_fpath := "./../ldfs/"+localfilename
				    filetext, err6 := ioutil.ReadFile(local_fpath)
						if err6 != nil {
							panic(err6)
					}
				    // filetext = "This is file data"

				    mytext = my_add+"#"+ sdfsfilename + "#" + string(filetext)
					r2, err3 := c2.SayPut(ctx, &pb.HelloRequest{Name: "Please make a replica of this file on yourself#"+mytext})
					//r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "VM8;"+my_add+";" })

					if err3 != nil {
						//log.Fatalf("could not greet: %v", err)
						fmt.Println("Could not make a replica!!",err)
					} else {	
						//If it is able to contact the introducer, then only run its server		   		
						//for {
							//receive the streamed data
							feature, err4:= r2.Recv()
							fmt.Println("Sent from replica node", feature, err4)
					}	
				}	*/			
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					//break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				} else{
					fmt.Println("Received nodes to put this file on: \n", feature.Message)
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
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
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
