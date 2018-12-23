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

//go:generate protoc -I ../helloworld --go_out=plugins=grpc:../helloworld ../helloworld/helloworld.proto

package main

import (
	"log"
	"net"
	// "os/exec"
	"fmt"
	"strings"
	"io/ioutil"
	"os"
	"io"
	// "reflect"
	"time"
	"math/rand"
	"strconv"
	// "math"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50052"
	t_suspect = 500
	t_fail =500
	t_cleanup = 1000
	repeat_period = 500
			
	// my_add = "172.22.154.101:50052"
	// vm_name = "VM8"
)
var my_add string
var vm_name string
var my_ringid string
var simulate int
var packet_loss = 30

// server is used to implement helloworld.GreeterServer.
type server struct{
	
}

func doEvery(d time.Duration, f func(time.Time)) {
    for x := range time.Tick(d) {
        f(x)
    }
}

func (s *server) SayGossip(in *pb.HelloRequest, stream pb.Greeter_SayGossipServer) error {
	
	log.Println(vm_name + " receives the gossip message:")
	log.Println(in.Name +"\n")
	
	new_info := strings.Split(in.Name, "&")
	new_info = new_info[1:len(new_info)-1]

	file_read, err := ioutil.ReadFile("./../membership_list.txt")
    if err != nil {
        panic(err)
    }
    //fmt.Println(string(file_read))

    var neighbours = strings.Split(string(file_read), "\n")
    neighbours = neighbours[0:len(neighbours)-1]

    var add_string string

    for i := 0; i < len(new_info); i++ {
    	node_exists := 0
    	split_ninfo := strings.Split(new_info[i], ";")

    	for j := 0; j < len(neighbours); j++ {

			split_nbr := strings.Split(neighbours[j], ";")

			//Check if the current neighbour is the one gossip is about
    		if strings.Compare(split_ninfo[1], split_nbr[1])!=0 {
    			node_exists = 0
    			continue
    		}

    		//The gossip is about me and they are suspecting me or have marked me as failed
			if strings.Compare(split_ninfo[1], my_ringid)==0 && (strings.Contains(split_ninfo[4], "S")|| strings.Contains(split_ninfo[4], "F")) {

				log.Println("I am still alive! Increasing my incarnation number")
				s, errs := strconv.Atoi(split_ninfo[5])
				if errs != nil {
					log.Println(errs)
				}
				
				split_ninfo[5] = strconv.FormatInt(int64(s+1), 10)
				log.Println("OLD INCARNATION NUMBER", s, "NEW INC NUMBER", split_ninfo[5])
				//Increment my incarnation number
				split_ninfo[4] = "A"
				//Mark myself as alive
				timestamp := time.Now().UnixNano()/1000000
				split_ninfo[3] = strconv.FormatInt(timestamp, 10)
				neighbours[j] = split_ninfo[0] +";" + split_ninfo[1] + ";" + split_ninfo[2] + ";"+ split_ninfo[3] +";" + split_ninfo[4] + ";" + split_ninfo[5] + ";"
				node_exists = 1
				break
				//It will break out of the neighbours loop and will move to the next gossip entry
			}

    		node_exists = 1
		    //The new info has a greater incarnation number
		    if split_ninfo[5]>split_nbr[5]{

		    	neighbours[j] = new_info[i]
		    
		    } else if split_ninfo[5] == split_nbr[5] {
		    
		    	//Both incarnation numbers are same
				//split_ninfo[3] is the timestamp
    			new_time, errq := strconv.Atoi(split_ninfo[3])
    			if errq != nil {
    				log.Println(errq)
			    }
    			
    			nbr_time, errr := strconv.Atoi(split_nbr[3])
    			if errr != nil {
    				log.Println(errr)
			    }
    			if  new_time > nbr_time {
    				neighbours[j] = new_info[i]
				} else if new_time == nbr_time{
					if strings.Contains(split_nbr[4], "F")||strings.Contains(split_ninfo[4], "F"){
						
						if strings.Contains(split_ninfo[4], "F") && !strings.Contains(split_nbr[4], "F"){
							log.Println("Marking node", split_ninfo[1], "as FAILED!")
						}
						split_ninfo[4] = "F"
						// log.Println("Detected FAILURE!!!", split_nbr[1])
						neighbours[j] = split_ninfo[0] +";" + split_ninfo[1] + ";" + split_ninfo[2] + ";"+ split_ninfo[3] +";" + split_ninfo[4] + ";" + split_ninfo[5] + ";"
						
					} else if strings.Contains(split_nbr[4], "S")||strings.Contains(split_ninfo[4], "S"){
						if strings.Contains(split_ninfo[4], "S"){
								log.Println("Marking node", split_ninfo[1], "as SUSPECTED!")
							}

						split_ninfo[4] = "S"
						// log.Println("SUSPECTED node!!!", split_nbr[1])
						if simulate == 1{
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
							fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
						}
							

						neighbours[j] = split_ninfo[0] +";" + split_ninfo[1] + ";" + split_ninfo[2] + ";"+ split_ninfo[3] +";" + split_ninfo[4] + ";" + split_ninfo[5] + ";"
					}
					//else both are Alive and we do nothing

				}
				//else new_time is stale and we do nothing

		    } 
		    //else split_ninfo[5] < split_nbr[5]
		    //The new info has a stale incarnation number, do nothing
			break
			//It will break out of the neighbours loop and will move to the next gossip entry
    	}

    	if(node_exists == 0){
    		add_string = add_string + new_info[i]+"\n"
    		log.Println("Adding a new node ", new_info[i], "\n")
    	}
    }

    var new_memlist string
	for i := 0; i < len(neighbours); i++ {
		new_memlist += neighbours[i] + "\n"
	}
	new_memlist +=add_string

	overwritefile(new_memlist)

	if(strings.Contains(in.Name, "PING")){
		var j pb.HelloReply
		if simulate == 1{
			var randum_num = rand.Intn(100)
			if randum_num < packet_loss{
				j.Message = ""
				log.Println("DROPPED THIS ACK MESSAGE")
			} else{
				j.Message = "ACK"
			}
		} else{
			j.Message = "ACK"
		}


		
		stream.Send(&j)
		// if err := stream.Send(&j); err != nil {
		// 	fmt.Println(err)
		// 	return err
		// }
	}
	return nil
}

func overwritefile(file_data string){

	var err2 = os.Remove("./../membership_list.txt")
	if err2 != nil {
		log.Println("ERROR renaming file", err2)
	}

	file, err3 := os.Create("./../membership_list.txt")
	if err3 != nil {
		log.Println("ERROR creating file", err3)
	}
	defer file.Close()

	file1, err4 := os.OpenFile("./../membership_list.txt", os.O_RDWR, 0644)
	if err4 != nil {
		log.Println("ERROR opening file", err4)
	}
	defer file1.Close()

	_, err5 := file1.WriteString(file_data)
	if err5 != nil {
		log.Println("ERROR writing into the file", err5)
	}

	return
}

//A function which is called infinitely after a fixed period of time which keeps the server alive allowing it ping and send gossip.
func fun_periodic(t time.Time) {

	
	log.Println("Calling periodically ", t, " \n")

	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	if err != nil {
		panic(err)
	}
	//fmt.Println(string(file_read))

	var neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	var i int
	for i = 0; i < len(neighbours); i++ {
		if( strings.Contains(neighbours[i], my_ringid)){
			break;
		}
		// fmt.Println(i, neighbours[i])
	}

    log.Println("My memlist \n ", string(file_read), "\n")
    log.Println("My Index in memlist: ",my_add, i, "\n")

    var l int
    var visited_neighbors string

    // var count

    // contacts := [2]int{-1,1}
    // var count = 0
    for l =-1; l<=1; l++{
	// for count<4{

		// l = contacts[count] + 
		// l = contacts[ind]
		if l==0 {
			continue
		}

		neighbour_ind := ((i+l)+len(neighbours))%len(neighbours)
		//fmt.Println("l", l, "neighbour_ind",neighbour_ind, "len", len(neighbours))

		nbr := neighbours[neighbour_ind]
		// nbr_ip := strings.Split(nbr, ";")[2]
		// nbr_ring := strings.Split(nbr, ";")[1]
		row := strings.Split(nbr, ";")
		if strings.Compare(row[1], my_ringid) == 0 || strings.Contains(visited_neighbors, row[1]) || strings.Contains(row[4], "F") {
			
			timestamp2 := time.Now().UnixNano()/1000000
			last_timestamp, err1 := strconv.Atoi(row[3])
			if err1!=nil{
				log.Println(err1)	
			}

			if int(timestamp2) - last_timestamp > t_cleanup && strings.Contains(row[4],"F"){
				
				row[0] = row[0] + "Cleanup"
				row[3] = strconv.FormatInt(timestamp2, 10)
				log.Println("Marked node ", neighbours[neighbour_ind], " for Cleanup!")
				neighbours[neighbour_ind] = row[0] +";" + row[1] + ";" + row[2] + ";"+ row[3] +";" + row[4] + ";" + row[5] + ";"

			}
				continue
		}
		// if  strings.Contains(row[4], "F"){
		// 	l = contacts[count] + next*sign()
		// 	continue
		// }
		//In case the number of nodes are very few in the group so that we do not get 4 disjoint neighbors, visited_neighbors prevent
		//calling the same node with a given ring id again
		visited_neighbors = visited_neighbors + row[1]


		// count += 1
		// overflow = 0
		// lnew = 

		log.Println("nbr: ", nbr, "\n")

		conn, err := grpc.Dial(row[2], grpc.WithInsecure())
		if err != nil {
	       log.Print("Failed to dail")
           log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		c := pb.NewGreeterClient(conn)

		// Set timeout
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
		defer cancel()
		
		var gossip string

        for i := 0; i < len(neighbours); i++ {
        	// var last_timestamp = strings.Split(neighbours[i], ";")[2]
        	// if int(timenow) - int(last_time) < t_threshold{
        	if !strings.Contains(neighbours[i], "Cleanup"){
    			gossip = gossip + neighbours[i] + "&"
    		}
        		// gossip = append(gossip, neighbours[i])		        		
        	// }		            
        }
		//call the function on the server
		r, err := c.SayGossip(ctx, &pb.HelloRequest{Name: vm_name + " sent you a PING + GOSSIP + memlist + &"+ string(gossip)})
		
		var receive_ack int
		if err != nil {
			//It received an error. Gossip was not sent
			log.Println("Could not send gossip!",err)
			receive_ack = 0
		} else {
			
			feature, err1:= r.Recv()
			log.Println("Gossip successfully sent to", nbr, "and received",feature.Message, "\n")

			if err1 != nil {
				log.Println("Did not receive ACK ", err1)
			}
			
			if(strings.Contains(feature.Message, "ACK") && !(strings.Contains(row[4], "L"))){

				row[3] = strconv.FormatInt(time.Now().UnixNano()/1000000, 10)
				row[4] = "A"
				receive_ack = 1
				log.Println("Updated alive timestamp for nbr: ", nbr, "\n")
				neighbours[neighbour_ind] = row[0] +";" + row[1] + ";" + row[2] + ";"+ row[3] +";" + row[4] + ";" + row[5] + ";"
			} else if (!strings.Contains(feature.Message, "ACK") && !(strings.Contains(row[4], "L"))){
				receive_ack = 0
			}
		}

		if receive_ack == 0 {
			//If an ack was not received, we check if the node has failed or is it in the suspected zone
			timestamp2 := time.Now().UnixNano()/1000000
			last_timestamp, err1 := strconv.Atoi(row[3])
			if err1!=nil{
				log.Println(err1)	
			}

			if int(timestamp2) - last_timestamp > t_fail && strings.Contains(row[4],"S"){
				
				row[4] = "F"
				row[3] = strconv.FormatInt(timestamp2, 10)
				log.Println("Marked node ", neighbours[neighbour_ind], " as FAILED!")

			} else if int(timestamp2) - last_timestamp > t_suspect && strings.Contains(row[4],"A"){
				
				row[4] = "S"
				if simulate == 1{
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
					fmt.Println("SUUUUUUUUUUUUUUUSPECTED----------------------------------")
				}
				row[3] = strconv.FormatInt(timestamp2, 10)
				log.Println("Marked node ", neighbours[neighbour_ind], " as SUSPECTED")

			}
			neighbours[neighbour_ind] = row[0] +";" + row[1] + ";" + row[2] + ";"+ row[3] +";" + row[4] + ";" + row[5] + ";"
			
		}
    }

 //    t_suspect := 10000
 //    t_fail :=10000
	// timestamp2 := time.Now().UnixNano()/1000000
	// var nbr_split [] string
 //    for i = 0; i < len(neighbours); i++ {
	//     last_timestamp, err1 := strconv.Atoi(strings.Split(neighbours[i], ";")[3])
	//     if err1!=nil{
	//     	fmt.Println(err1)	
	//     }
	//        if int(timestamp2) - last_timestamp > t_fail + t_suspect{
	// 			nbr := neighbours[i]
	// 			nbr_split = strings.Split(nbr, ";")
	// 			nbr_split[4] = "F"
	// 			nbr_split[3] = strconv.FormatInt(timestamp2, 10)
 //    		} else if int(timestamp2) - last_timestamp > t_suspect {
 //    			nbr := neighbours[i]
	// 			nbr_split := strings.Split(nbr, ";")
	// 			nbr_split[4] = "S"
	// 			nbr_split[3] = strconv.FormatInt(timestamp2, 10)
 //    		}

	// 		neighbours[i] = nbr_split[0] +";" + nbr_split[1] + ";" + nbr_split[2] + ";"+ nbr_split[3] +";" + nbr_split[4] + ";" + nbr_split[5] + ";"
	// 		fmt.Println("neighbour:", neighbours[i])		

 //    	}

	var new_memlist string
	for i = 0; i < len(neighbours); i++ {
		if !strings.Contains(neighbours[i], "Cleanup"){
			new_memlist += neighbours[i] + "\n"
		}
	}
	overwritefile(new_memlist)

}

// SayHello implements helloworld.GreeterServer
// The same VM's client calls SayRepeat()

func (s *server) SayRepeat(in *pb.HelloRequest, stream pb.Greeter_SayRepeatServer) error {
	
	log.Println(in.Name)

	if strings.Contains(in.Name, "PACKET_LOSS"){
		simulate = 1
	}


	input_split := strings.Split(in.Name, "&")
	if strings.Contains(input_split[0], "JOIN"){

		my_ringid = strings.Split(input_split[1], ";")[1]
		// formatted_output := "Successfully started VM10 server.. SayRepeat"
		// var len_formout = float64(len(formatted_output))	
		// fmt.Println(len_formout)
		
		//Start server streaming assuming that the chuck size is 2 million characters
		// for g:= float64(0); g < len_formout; g = g+2000000 {

			var j pb.HelloReply
			// j.Message = formatted_output[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
			
			j.Message = "Successfully started "+vm_name+" server (SayRepeat function).."
			
			stream.Send(&j)

			// if err := stream.Send(&j); err != nil {
			// 	fmt.Println(err)
			// 	return err
			// }

		// }
	} else{
		log.Println("I am LEAVING the group. \n")
		var j pb.HelloReply
		j.Message = "Leaving " + input_split[1] + "from system"
		stream.Send(&j)
		os.Exit(3)

	}


	
	doEvery(repeat_period*time.Millisecond, fun_periodic)

	return nil 
}

// SayHello is only required in introducer and is called by a new node's client for joining the group
// SayHello implements helloworld.GreeterServer
func introduce(new_info string) (pb.HelloReply) {
	
	var j pb.HelloReply
	var filename = "./../membership_list.txt"
	//Open the Introducer;s membership list
	var f, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	//Read the introducer's membership list
	file_check, err_check := ioutil.ReadFile(filename)
	if err_check != nil {
		panic(err_check)
	}

	//Find the node IP and whether it wants to leave the group or join the group
	flag := strings.Split(new_info, "#")[1]
	new_node := strings.Split(new_info, "#")[0]
	new_node_ip := strings.Split(new_node, ";")[2]

	if strings.Compare(flag, "3")==0{
		simulate = 1
	}

	//2 means it wants to leave the group
	if strings.Compare(flag, "2")==0 {

		log.Println("Some node is LEAVING the group.", new_info)
		var neighbours = strings.Split(string(file_check), "\n")
	    neighbours = neighbours[0:len(neighbours)-1]

	    //We search in introducer's memlist to find where it is present and mark it as Leaving by F+L and update timestamp
		for i := 0; i < len(neighbours); i++ {

			if strings.Contains(neighbours[i], new_node_ip) && (strings.Contains(neighbours[i], "S") ||strings.Contains(neighbours[i], "A")){
				node_split := strings.Split(neighbours[i], ";")
				node_split[4] = "F+L"
				node_split[3] = strconv.FormatInt(time.Now().UnixNano()/1000000, 10)
				log.Println("Marking the node for LEAVE", node_split[1], "\n")

				neighbours[i] = node_split[0] + ";" + node_split[1] + ";"+ node_split[2] + ";" + node_split[3] + ";" + node_split[4] + ";"+ node_split[5] + ";"
				break
			}
		}

		//Write those changes to file
	    var new_memlist string
		for i := 0; i < len(neighbours); i++ {
			new_memlist += neighbours[i] + "\n"
		}

		overwritefile(new_memlist)

		j.Message = "Updated membership list with Leave for "+string(new_node_ip)
		return j

	} else{
		//The node wants to join

		// We check if the new_node which is the IP+ringid+VMi is already present!
		// This if statement actually does not work because every time a node will
		//come with a new ringid because it is a function of the timestamp. Instead we should check if the ip exists with an A or S, then its present
		//If the Ip exists with an F or L them we can introduce
		if( strings.Contains(string(file_check), new_node)){
			j.Message = "Present! You are already part of the group!"
			return j
		}

		timestamp := time.Now().UnixNano()/1000000
	    t := strconv.FormatInt(timestamp, 10)

	    new_row := new_node + t + ";" + "A" + ";" + "0" + ";" + "\n"
		if _, err = f.WriteString(new_row)
	    // if _, err = f.WriteString(new_node+";"+"\n")
		err != nil {
	        panic(err)
	    }

	    // file_read, err := ioutil.ReadFile(filename)
	    // if err != nil {
	    //     panic(err)
	    // }

	 	reply_to_client := string(file_check) + new_row
	    // fmt.Println("Sending updated introducer memlist to the new node\n" + string(file_read))
		log.Println("Sending this updated introducer memlist to the new node\n\n" + reply_to_client)

	    // j.Message = string(file_read)	
	    j.Message = reply_to_client	
		return j
	}
}


//SayHello is the Introducer function. Every VM's client calls SayHello for joining the group
// SayHells implements helloworld.GreeterServer
func (s *server) SayHello(in *pb.HelloRequest, stream pb.Greeter_SayHelloServer) error {

	log.Println("Introducer on VM9 has been called!\n")
	log.Println("New incoming node: ", in.Name+"\n")

	j := introduce(in.Name)

	stream.Send(&j)
	return nil 
}

func main() {
	// Read in server addresses from a file
	address, err := ioutil.ReadFile("./../../vm_info.txt") // just pass the file name
	if err != nil {
		log.Print(err)
	}
	//Parse the address into each server's address
	vm_info := strings.Split(string(address), "\n")
	my_add = vm_info[0]
	vm_name = vm_info[1]

	var err2 = os.Remove("./../../"+vm_name+"mp2_log.txt")
	if err2 != nil {
		log.Println("ERROR removing log file", err2)
	}

	f_log, errl := os.OpenFile("./../../"+vm_name+"mp2_log.txt", os.O_RDWR | os.O_CREATE , 0666)
	if errl != nil {
	    log.Fatalf("error opening file: %v", errl)
	}
	defer f_log.Close()

	// log.SetOutput(f_log)
	

	mw := io.MultiWriter(os.Stdout, f_log)
	log.SetOutput(mw)

	log.Println("Welcome to "+ vm_name + " with the IP and Port " + my_add + "\n")

	//Listen to the port
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
