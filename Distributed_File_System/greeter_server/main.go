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
	"os/exec"
	"fmt"
	"strings"
	"io/ioutil"
	"os"
	"io"
	// "reflect"
	"time"
	"math/rand"
	"strconv"
	"math"
	"path/filepath"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50053"
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
var leader = "172.22.158.101"+port
var num_replica = 4

var sdfsfile_version = make(map[string]int)
var sdfsfile_machine = make(map[string]string)


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
					// Some failure exists start
					if strings.Contains(split_nbr[4], "F")||strings.Contains(split_ninfo[4], "F"){
						//Detected new failure start	
						if strings.Contains(split_ninfo[4], "F") && !strings.Contains(split_nbr[4], "F"){
							fmt.Println("Marking node", split_ninfo[1], "as FAILED!")
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
						}
						neighbours[j] = split_ninfo[0] +";" + split_ninfo[1] + ";" + split_ninfo[2] + ";"+ split_ninfo[3] +";" + split_ninfo[4] + ";" + split_ninfo[5] + ";"
					}
					//else both are Alive and we do nothing
				}
				//Both kinds of failure detection done i.e. when timestamp is same or when gossip timestamp is newer
				if strings.Contains(neighbours[j], "F"){
					handlenodefailure(neighbours[j], neighbours)
				}

				//else new_time is stale and we do nothing
		    } 
		    //else split_ninfo[5] < split_nbr[5]
		    //The new info has a stale incarnation number, do nothing
			break
			//It will break out of the neighbours loop and will move to the next gossip entry
    	}
    	//j loop over neighbours got over

    	if(node_exists == 0){
    		add_string = add_string + new_info[i]+"\n"
    		log.Println("Adding a new node ", new_info[i], "\n")
    	}
    }
    // i loop over the new gets over

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

func handleleaderfailure(neighbours []string ){

	fmt.Println("Oh shit the leader has failed")
	var maxnum = 0
	var max_ip string
	for l := 0; l < len(neighbours); l++ {
		node_nbr := strings.Split(neighbours[l], ";")
		if !strings.Contains(node_nbr[4], "A"){
			continue
		}
		node_vm := node_nbr[0]
		node_num := strings.Split(node_vm, "M")[1]
		node_num_int, err8 := strconv.Atoi(node_num)
		fmt.Println("max num", node_vm, node_num, node_num_int)
		if err8!=nil{
			fmt.Println(err8)
		} else{
			if node_num_int > maxnum{
				maxnum = node_num_int
				max_ip  = node_nbr[2]
				fmt.Println("Max ip now", max_ip, maxnum)
			}
		}
	}
	leader = max_ip
	fmt.Println("MY NEW LEADER IS ", leader)
	//write this to the leader file so that the client can also recognize who the leader is
	write_leader := strings.Split(leader, ":")[0]
	var err2 = os.Remove("./../leader.txt")
	if err2 != nil {
		log.Println("ERROR renaming file", err2)
	}

	file, err3 := os.Create("./../leader.txt")
	if err3 != nil {
		log.Println("ERROR creating file", err3)
	}
	defer file.Close()

	file1, err4 := os.OpenFile("./../leader.txt", os.O_RDWR, 0644)
	if err4 != nil {
		log.Println("ERROR opening file", err4)
	}
	defer file1.Close()

	_, err5 := file1.WriteString(write_leader)
	if err5 != nil {
		log.Println("ERROR writing into the file", err5)
	}
	//file overwriting over

	if strings.Compare(max_ip, my_add) == 0{
		//Oh shit I am the leader. I will have to create the dictionary on myself now
		fmt.Println("Damn! I have become the leader now", max_ip, my_add)
		createdictionary()
	}
}

func handlefollowerfailure(split_ninfo [] string, neighbours [] string){
	
	failed_ip := split_ninfo[2]
	for k, v := range sdfsfile_machine {
	    if !strings.Contains(sdfsfile_machine[k], failed_ip){
	    	continue
	    }
    	//
    	fmt.Println("File found", k, v)
    	//version := sdfsfile_version[k]
    	//search_file := k + "_v" + strconv.Itoa(version)
    	//node1 := strings.Split(v,"#")[0]
    	//node1_ip := strings.Split(node1, "&")[1]
    	new_value := strings.Split(sdfsfile_machine[k], "#")
    	new_value = new_value[0:len(new_value)-1]
    	var value_update string = ""
    	for i:=0; i < len(new_value); i++{
    		if(!strings.Contains(new_value[i], failed_ip)){
    			value_update = value_update+new_value[i]+"#"
    			fmt.Println("value_update Set to ", value_update)	    
    		}
    	}
    	fmt.Println("value_update Set to ", value_update)	    	
    	//go through the membership list and find an alive node which is not present in value_update
    	for m := 0; m < len(neighbours); m++ {
    		if strings.Contains(neighbours[m],"A"){
    			//This neighbour should be alive
    			possible_ip := strings.Split(neighbours[m], ";")[2]
    			if !strings.Contains(value_update, possible_ip){
    				//This neighbour should not already be a replica of this specfic file
    				possible_vm := strings.Split(neighbours[m], ";")[0]	
    				filetransfer(possible_ip, value_update, sdfsfile_version[k], k)
    				value_update +=possible_vm + "&" + possible_ip + "#"
    				break
    			}
			}
		}
		sdfsfile_machine[k] = value_update
	}
	//File k replicated on another node
	
	//All files present on failed node replicated
}

func handlenodefailure(failed_guy string, neighbours [] string){
	//detected failure
	split_ninfo := strings.Split(failed_guy, ";")
	fmt.Println("Detected Failure ", leader, split_ninfo[2])

	if strings.Compare(split_ninfo[2], leader)==0{
		// The leader has failed. Look through the neighbour list and find the node with the highest ringid and make it 
		//the  leader
		handleleaderfailure(neighbours)
	} else{
		//Detected old failure
		//Some node other than the master has failed
		//Check if this node is the master (my_add == leader) then create a new replica
		if strings.Compare(my_add, leader)==0{
		// Then find all the files on the fauled node
			handlefollowerfailure(split_ninfo, neighbours)
		}
		//else if I am not the master then I dont have to do anything. Whoever the master is will take care of it.
	}	
}

func filetransfer(send_to_ip string, value_update string, num_versions int, k string){
	// get the file from one of the existing vm's from value_update and send to
	node1 := strings.Split(value_update, "#")[0]
	from_ip := strings.Split(node1, "&")[1]


	fmt.Println("Failure recovery file transfer of ", num_versions, " files")

	  for i := 0; i < num_versions; i++ {

	  	fmt.Println("Recovering from failure copying version", i)

		  conn, err := grpc.Dial(from_ip, grpc.WithInsecure())
		  if err != nil {
		        log.Print("Failed to dail")
		        log.Fatalf("did not connect: %v", err)
		  }
		  defer conn.Close()

		  c := pb.NewGreeterClient(conn)

		  // Set timeout
		  ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
		  defer cancel()
		  //get the file

		  filename :=  k + "_v" + strconv.Itoa(i+1)
		  r, err5 := c.FetchFile(ctx, &pb.HelloRequest{Name: filename})

		  fmt.Println("Failure recovery", filename)
	      fmt.Println("Returned from FetchFile in Filetransfer", err5)
	      file_output, err1 := r.Recv()
	      if err1 == io.EOF {
	            fmt.Println("EOF Error")
	            //break
	      } else {
	            fmt.Println("Output of fetch file in filetransfer", file_output.Message)
	            var j pb.HelloReply
				 j.Message = file_output.Message
	           // break
	      }
	      //Now put the file
	      fmt.Println("Now sending to ", send_to_ip)
		conn2, err2 := grpc.Dial(send_to_ip, grpc.WithInsecure())
		  if err2 != nil {
		        log.Print("Failed to dail")
		        log.Fatalf("did not connect: %v", err2)
		  }
		  defer conn2.Close()

		  c2 := pb.NewGreeterClient(conn2)

		  // Set timeout
		  ctx2, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
		  defer cancel()
		  //get the file
		  fmt.Println("Now we will send this file")

		  r2, err3 := c2.SayPut(ctx2, &pb.HelloRequest{Name:"Some one has failed, please be a replica for this file"+"#"+my_add+"#"+filename + "#" + file_output.Message})
	      fmt.Println("Returned from sayput in Filetransfer", err3)
	      file_output2, err4 := r2.Recv()
	      if err4 == io.EOF {
	            fmt.Println("EOF Error")
	            //break
	      } else {
	            fmt.Println("Output of sayput in filetransfer", file_output2.Message)
	            var j pb.HelloReply
				 j.Message = file_output.Message
	            //break
	      }


	}
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


func (s *server) LeaderLs(in *pb.HelloRequest, stream pb.Greeter_LeaderLsServer) error {

        fmt.Println("Inside leaderLS", in.Name)

        output_files, err := exec.Command("ls", "./../sdfs/").Output()
        fmt.Println(string(output_files))
        if err != nil {
                fmt.Println("ls error")
                log.Println(err)
        }
        output1 := string(output_files)
        fmt.Println("Sending LS output from LeaderLs", output1)
        var j pb.HelloReply
        j.Message = output1
        stream.Send(&j)
        return nil

 }

func createdictionary(){

	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	if err != nil {
		panic(err)
	}
	//fmt.Println(string(file_read))
	var neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	// var i int
	// for i = 0; i < len(neighbours); i++ {
	// 	split_nbr := strings.Split(neighbours[i], ";")
	// 	// fmt.Println(i, neighbours[i])
	// 	if !strings.Contains(split_nbr[4], "A"){
	// 		continue
		// }
	for j := 0; j < len(neighbours); j++ {
		split_nbr := strings.Split(neighbours[j], ";")
		fmt.Println(split_nbr)
		if strings.Compare(split_nbr[4], "A") != 0 {
			continue
		}

		fmt.Println("Address to be connected", split_nbr[2])
		conn, err := grpc.Dial(split_nbr[2], grpc.WithInsecure())
		if err != nil {
		    log.Print("Failed to dail")
		    log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()

		c := pb.NewGreeterClient(conn)

		// Set timeout
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
		defer cancel()

		// file_name := strings.Split(new_info, "@")
		// fmt.Println("File name", file_name[1])

		r, err := c.LeaderLs(ctx, &pb.HelloRequest{Name:"Please LS"})

		file_output, err1 := r.Recv()
		if err1 == io.EOF {
		    fmt.Println("EOF Error")
		    //break
		} else {
			fmt.Println("Received LS output as", file_output.Message)
			//Break the output by \n
			stored_files := strings.Split(file_output.Message, "\n")
			stored_files = stored_files[0:len(stored_files)-1]
			fmt.Println("This gus had", len(stored_files), " files")
			for k :=0; k<len(stored_files); k++{
				filename := strings.Split(stored_files[k], "_v")[0]
				filenumber, errf := strconv.Atoi(strings.Split(stored_files[k],"_v")[1])
				if errf!=nil{
					fmt.Println(errf)
				}
				if val, ok := sdfsfile_machine[filename]; ok {
					//check if this filename is present in dictionary
					//check if this vm has already been added to the machine dictionary
					if !strings.Contains(val, split_nbr[2]){
						//If not then add it
						sdfsfile_machine[filename] = val + split_nbr[0]+"&"+split_nbr[2]+"#"
					}
					sdfsfile_version[filename] = int(math.Max(float64(sdfsfile_version[filename]), float64(filenumber)))
				} else{
					//if it is not present
					sdfsfile_machine[filename] = split_nbr[0]+"&"+split_nbr[2]+"#"
					sdfsfile_version[filename] = 1
				}
			}
			fmt.Println("This node has made the dictionaries to ", sdfsfile_version, sdfsfile_machine)

		}
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

				neighbours[neighbour_ind] = row[0] +";" + row[1] + ";" + row[2] + ";"+ row[3] +";" + row[4] + ";" + row[5] + ";"
				handlenodefailure(neighbours[neighbour_ind], neighbours)

			} else if int(timestamp2) - last_timestamp > t_suspect && strings.Contains(row[4],"A"){
				
				row[4] = "S"
				if simulate == 1{
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

	leaderinfo, errl := ioutil.ReadFile("./../leader.txt") // just pass the file name
	if errl != nil {
		fmt.Print(errl)
	}
	//Parse the address into each server's address
	sl := strings.Split(string(leaderinfo), "\n")
	leader = sl[0]+port
	fmt.Println("Leader is:", leader)






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


	var filename = "./../leader.txt"
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
	j.Message += "###" + string(file_check)

	stream.Send(&j)
	return nil 
}

func (s *server) MyDelete(in *pb.HelloRequest, stream pb.Greeter_MyDeleteServer) error {

 	msg := mynewdelete(in.Name)
	var jj pb.HelloReply
	jj.Message = msg
	stream.Send(&jj)
	return nil
}

func mynewdelete(deletefile string) string {

	if val, ok := sdfsfile_machine[deletefile]; ok {
		k := deletefile
		v := val
		fmt.Println("File found", k, v)
		// version :=sdfsfile_version[k]
		search_file := k + "_v*"
		nodes := strings.Split(v,"#")
		nodes = nodes[0:len(nodes)-1]
		for i := 0; i < len(nodes); i++ {
			node1_ip := strings.Split(nodes[i], "&")[1]
			conn, err := grpc.Dial(node1_ip, grpc.WithInsecure())
			if err != nil {
			    log.Print("Failed to dail")
			    log.Fatalf("did not connect: %v", err)
			}
			defer conn.Close()

			c := pb.NewGreeterClient(conn)

			// Set timeout
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()
			r, err := c.LocalDelete(ctx, &pb.HelloRequest{Name: search_file})
			fmt.Println("Returned from LocalDelete")
			file_output, err1 := r.Recv()
			if err1 == io.EOF {
			    fmt.Println("EOF Error")
			    //break
			} else {
			    fmt.Println("Output of delete file", file_output.Message)
			    var j pb.HelloReply
				 j.Message = file_output.Message
			  //   break
			}

		}
		delete(sdfsfile_machine, deletefile)

		delete(sdfsfile_version, deletefile)	
		deletemessage := " Deleted file from SDFS " + deletefile
		return deletemessage

	} else{
		fmt.Println(" File not present ins SDFS: ", deletefile)
		deletemessage := " File not present ins SDFS: " + deletefile
		return deletemessage
	}

	// for k, v := range sdfsfile_machine {
	// 	if strings.Compare(k,deletefile ) ==0{
	// 		fmt.Println("File found", k, v)
	// 		// version :=sdfsfile_version[k]
	// 		search_file := k + "_v*"
	// 		nodes := strings.Split(v,"#")
	// 		nodes = nodes[0:len(nodes)-1]
	// 		for i := 0; i < len(nodes); i++ {

	// 		  node1_ip := strings.Split(nodes[i], "&")[1]
	// 		  conn, err := grpc.Dial(node1_ip, grpc.WithInsecure())
	// 	      if err != nil {
	// 	            log.Print("Failed to dail")
	// 	            log.Fatalf("did not connect: %v", err)
	// 	      }
	// 	      defer conn.Close()

	// 	      c := pb.NewGreeterClient(conn)

	// 	      // Set timeout
	// 	      ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	// 	      defer cancel()
	// 	      r, err := c.LocalDelete(ctx, &pb.HelloRequest{Name: search_file})
	// 	      fmt.Println("Returned from LocalDelete")
	// 	      file_output, err1 := r.Recv()
	// 	      if err1 == io.EOF {
	// 	            fmt.Println("EOF Error")
	// 	            //break
	// 	      } else {
	// 	            fmt.Println("Output of delete file", file_output.Message)
	// 	            var j pb.HelloReply
	// 				 j.Message = file_output.Message
	// 	          //   break
	// 	      }

	// 		}
        

	// 	}
	// }
	// delete(sdfsfile_machine, deletefile)
	// delete(sdfsfile_version, deletefile)
//	return

}

func (s *server) LocalDelete(in *pb.HelloRequest, stream pb.Greeter_LocalDeleteServer) error{

	fmt.Println("Inside LocalDelete", in.Name)

	files, err := filepath.Glob("./../sdfs/"+in.Name)
	if err != nil {
	    panic(err)
	}
	for _, f := range files {
	    if err := os.Remove(f); err != nil {
	        panic(err)
	    }
	}


    var j pb.HelloReply
    j.Message = "Deleted all versions on myself"
     stream.Send(&j)

    return nil
}

func (s *server) FetchFile(in *pb.HelloRequest, stream pb.Greeter_FetchFileServer) error {


		var fpath string
		if strings.Contains(in.Name, "CALL_FROM_SAYPUT"){
			fpath = strings.Split(in.Name, "CALL_FROM_SAYPUT###")[1]
		} else{
			fpath = "./../sdfs/"+in.Name
		}


        fmt.Println("Inside Fetch Files", in.Name)
        // output_files, err := exec.Command("ls", "./../sdfs/").Output()
        // if err != nil {
        //         fmt.Println("ls error")
        //         log.Println(err)
        // }
        // output := string(output_files)
        // if strings.Contains(output, in.Name) {
                fmt.Println("File name", in.Name)
                file_read, err := ioutil.ReadFile(fpath)
                fmt.Println("File contains", string(file_read))
                if err != nil {
                        panic(err)
                }

                formatted_output := string(file_read)

				var len_formout = float64(len(string(file_read)))	
				fmt.Println(len_formout)
					
				//Start server streaming assuming that the chuck size is 2 million characters
				for g:= float64(0); g < len_formout; g = g+1000000 {

					var j pb.HelloReply
					j.Message = formatted_output[int(g):int(math.Min(float64(len_formout), float64(g+1000000)))]
					fmt.Println(len(j.Message))
					
					if err := stream.Send(&j); err != nil {
							fmt.Println(err)
							return err
					}
				}



                // var j pb.HelloReply
                // j.Message = string(file_read)
                // stream.Send(&j)
        // }
        return nil
}

func (s *server) MyGet(in *pb.HelloRequest, stream pb.Greeter_MyGetServer) error {

	//var j pb.HelloReply
	// j:= GetLiveNodes(in.Name)
	//Iterate
	file_found := 0
	fmt.Println("In Myget")
	myoutput := "File--"


	var myfeature string
	if val, ok := sdfsfile_machine[in.Name]; ok {
		k := in.Name
		v := val
		file_found = 1
	// for k, v := range sdfsfile_machine {
	//     if strings.Compare(k,in.Name ) ==0{

    	fmt.Println("File found", k, v)
    	myoutput += "File found" + k + v
    	version :=sdfsfile_version[k]
    	search_file := k + "_v" + strconv.Itoa(version)

    	my_replicas := strings.Split(v,"#")
    	my_replicas = my_replicas[0:len(my_replicas)-1]
    	done_for_r :=0
    	for i := 0; i < len(my_replicas); i++ {

	    	node1 := my_replicas[i]
	    	node1_ip := strings.Split(node1, "&")[1]
	    	myoutput += node1

	    	 conn, err := grpc.Dial(node1_ip, grpc.WithInsecure())
	          if err != nil {
	                log.Print("Failed to dail")
	                log.Fatalf("did not connect: %v", err)
	          }
	          defer conn.Close()

	          c := pb.NewGreeterClient(conn)

	          // Set timeout
	          ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	          defer cancel()

	          r, err := c.FetchFile(ctx, &pb.HelloRequest{Name: search_file})
	          if err!=nil{
					myoutput +="Error is not nil"
	          } else{
				fmt.Println("Returned from fetchfile")
				myoutput += "Returned from fetchfile"
				
				for{
					file_output, err1 := r.Recv()
					if err1 != nil {
						fmt.Println("ERROR")			
					}
					if(file_output == nil){
						break
					}
					if err1 == io.EOF {
						fmt.Println("EOF Error")
						break
					} else {
						fmt.Println("Output of fetch file", file_output.Message)
						 myfeature += file_output.Message
						// file_found = 1
						 myoutput += "InnerlOOP"
						 done_for_r = 1
						// break
					}
				}
	          }
	         if done_for_r ==1{
	         	break
	         }
	    }
	}
	if file_found ==0{
		myfeature = myoutput+"NOFOUND#"+ string(myfeature)
	} else{
		myfeature = myoutput+"YESFOUND#"+ string(myfeature)
	}

	formatted_output := myfeature
	var len_formout = float64(len(formatted_output))	
	fmt.Println(len_formout)
	
	//Start server streaming assuming that the chuck size is 2 million characters
	for g:= float64(0); g < len_formout; g = g+1000000 {

		var jj pb.HelloReply
		jj.Message = formatted_output[int(g):int(math.Min(float64(len_formout), float64(g+1000000)))]
		//fmt.Println(len(jj.Message))
		
		if err := stream.Send(&jj); err != nil {
				fmt.Println(err)
				return err
		}
	}


    // stream.Send(&j)
	return nil 
}


func (s *server) MyLs(in *pb.HelloRequest, stream pb.Greeter_MyLsServer) error {
	
	var jj pb.HelloReply
	
	for k, v := range sdfsfile_machine {

		if strings.Compare(k,in.Name ) ==0{
			fmt.Println("File found", k, v)
			// version :=sdfsfile_version[k]
			jj.Message = v

		}
	}



	// jj.Message = "Deleted files"
	stream.Send(&jj)
	return nil
}


func (s *server) SayPut(in *pb.HelloRequest, stream pb.Greeter_SayPutServer) error {

	var myfeature string
	log.Println("I will create replicas on myself!\n")
	log.Println("Receiving new file ", in.Name+"\n")

	myfeature += "I will create replicas on myself!"
	newinfo := strings.Split(in.Name, "#")
	ip := newinfo[1]
	fname := newinfo[2]
	//ftext := newinfo[3]
	local_fpath := newinfo[3]
		var ftext string

	fmt.Println("Receiving request from ", ip, " and creating replica of ", fname, ftext)
	//Now call a function on ip and read the file local_fpath into ftext



	 conn, err := grpc.Dial(ip, grpc.WithInsecure())
      if err != nil {
            log.Print("Failed to dail")
            log.Fatalf("did not connect: %v", err)
      }
      defer conn.Close()

      c := pb.NewGreeterClient(conn)

      // Set timeout
      ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
      defer cancel()

      r, err := c.FetchFile(ctx, &pb.HelloRequest{Name: "CALL_FROM_SAYPUT###"+local_fpath})
      if err!=nil{
			// myoutput +="Error is not nil"
      	fmt.Println("Error not nill")
      } else{
		fmt.Println("Returned from fetchfile")
		// myoutput += "Returned from fetchfile"
		
		for{
			file_output, err1 := r.Recv()
			if err1 != nil {
				fmt.Println("ERROR")			
			}
			if(file_output == nil){
				break
			}
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			} else {
				fmt.Println("Output of fetch file", file_output.Message)
				//myfeature += file_output.Message
				 ftext +=file_output.Message
				// file_found = 1
				 // myoutput += "InnerlOOP"
				//  done_for_r = 1
				// // break
			}
		}
      }


	fpath := "./../sdfs/"+fname
	file, err3 := os.Create(fpath)
	if err3 != nil {
		log.Println("ERROR creating file", err3)
		myfeature += "ERROR creating file"
	}
	defer file.Close()

	file1, err4 := os.OpenFile(fpath, os.O_RDWR, 0644)
	if err4 != nil {
		log.Println("ERROR opening file", err4)
		myfeature += "ERROR opening file"
	}
	defer file1.Close()

	_, err5 := file1.WriteString(ftext)
	if err5 != nil {
		log.Println("ERROR writing into the file", err5)
		myfeature += "ERROR cwriting file"
	}

	var j pb.HelloReply	
	j.Message = "Created replica! You are all set!" + myfeature
	stream.Send(&j)
	return nil 
}



func (s *server) MyGetversions(in *pb.HelloRequest, stream pb.Greeter_MyGetversionsServer) error {


	fmt.Println("Inside MyGetversions")
	var j pb.HelloReply
	mydata := strings.Split(in.Name, "#")
	sdfsfilename := mydata[0]
	versions, errv := strconv.Atoi(mydata[1])
	if errv!=nil{
		fmt.Println(errv)
	}
	//get versions last versions
	//localfilename := mydata[2]
	client_ip := mydata[3]

	fmt.Println("Client IP is ", client_ip)
	num_versions := sdfsfile_version[sdfsfilename]
	vm_names := sdfsfile_machine[sdfsfilename]

	node1 := strings.Split(vm_names, "#")[0]
	node1ip := strings.Split(node1, "&")[1]

	get_versions := int(math.Min(float64(versions), float64(num_versions)))
	for i := 0; i < get_versions; i++ {
		search_version := num_versions - i
		fmt.Println("Version = ", search_version)
		//i will go from 1, 2, 3
		//suppose I want last 3 versions and have total 5 versions num_versions = 5 and version = 3
		//5-0, 5-1, 5-2

		fmt.Println("Address to be connexted", node1ip)
		conn, err := grpc.Dial(node1ip, grpc.WithInsecure())
	      if err != nil {
	            log.Print("Failed to dail")
	            log.Fatalf("did not connect: %v", err)
	      }
	      defer conn.Close()

	      c := pb.NewGreeterClient(conn)

	      // Set timeout
	      ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	      defer cancel()


		search_file := sdfsfilename + "_v" + strconv.Itoa(search_version)
  		r, err := c.FetchFile(ctx, &pb.HelloRequest{Name: search_file})
		fmt.Println("Returned from fetchfile")


		for{
			file_output, err1 := r.Recv()
			if(file_output == nil){
					break
			}
			if err1 != nil {
					fmt.Println("ERROR")

					
			}

			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			} else {
				fmt.Println("Output of fetch file", file_output.Message)
				 j.Message += file_output.Message
				// break
			}	
		}
		j.Message += "NEW_VERSION###"



/*
		//Now create a file on the client with this data
		fmt.Println("Address of Client IP", client_ip)
		conn, err = grpc.Dial(client_ip, grpc.WithInsecure())
	      if err != nil {
	            log.Print("Failed to dail")
	            log.Fatalf("did not connect: %v", err)
	      } 
	      // else{

		      defer conn.Close()

		      c = pb.NewGreeterClient(conn)

		      // Set timeout
		      ctx, cancel = context.WithTimeout(context.Background(), time.Second*1000000)
		      defer cancel()


		     putfile_name := localfilename + "_v" + strconv.Itoa(search_version)
		     send_data := putfile_name + "#" + file_output.Message
	  		r, err = c.PutFile(ctx, &pb.HelloRequest{Name: send_data})
	  		 if err != nil {
	            fmt.Println("Error calling PutFile")
	      } else {
			fmt.Println("Returned from Putfile")
			file_output, err1 = r.Recv()
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			//break
			} else {
				fmt.Println("Output of Put file", file_output.Message)
				 j.Message += file_output.Message
				// break
			// }
		}

		}
*/




	}
	formatted_output := j.Message
	var len_formout = float64(len(formatted_output))	
	fmt.Println(len_formout)
	
	//Start server streaming assuming that the chuck size is 2 million characters
	for g:= float64(0); g < len_formout; g = g+1000000 {

		var jj pb.HelloReply
		jj.Message = formatted_output[int(g):int(math.Min(float64(len_formout), float64(g+1000000)))]
		fmt.Println(len(jj.Message))
		
		if err := stream.Send(&jj); err != nil {
				fmt.Println(err)
				return err
		}
	}


	// stream.Send(&j)
	return nil
}

func (s *server) PutFile(in *pb.HelloRequest, stream pb.Greeter_PutFileServer) error {


	fmt.Println("Inside Putfile")

	var j pb.HelloReply
	
	file_data := strings.Split(in.Name, "#")
	filename := file_data[0]
	all_data := file_data[1]


	fmt.Println("Creating file ", "./../ldfs/", filename)
	file, err3 := os.Create("./../ldfs/"+filename)
	if err3 != nil {
	        log.Println("ERROR creating file", err3)
	}
	defer file.Close()

	file1, err4 := os.OpenFile("./../ldfs/"+filename, os.O_RDWR, 0644)
	if err4 != nil {
	        log.Println("ERROR opening file", err4)
	}
	defer file1.Close()

	_, err5 := file1.WriteString(string(all_data))
	if err5 != nil {
	        log.Println("ERROR writing into the file", err5)
	}

	j.Message = "Put the file"

	stream.Send(&j)
	return nil
}


func (s *server) IntroPut(in *pb.HelloRequest, stream pb.Greeter_IntroPutServer) error {

	log.Println("Put function on Leader has been called!\n")
	log.Println("New incoming node: ", in.Name+"\n")

	var j pb.HelloReply	
	j.Message = "I am send your file to these nodes .. #"
	
	incoming_nodeid := strings.Split(in.Name, "#")[1]
	fmt.Println(incoming_nodeid, " wants to insert a file")
	sdfsfilename := strings.Split(in.Name, "#")[2]
	// filetext :=strings.Split(in.Name, "#")[3]
	local_fpath :=strings.Split(in.Name, "#")[3]
	


	var send_nodes string

	//Check if that sdfsfilename already exists
	if val, ok := sdfsfile_machine[sdfsfilename]; ok {
	    //do something here
	    prev_version := sdfsfile_version[sdfsfilename]
	    sdfsfile_version[sdfsfilename] = prev_version + 1
	    //Get on what replicas does this file exist
	    all_replicas := strings.Split(val, "#")
	    all_replicas = all_replicas[0:len(all_replicas)-1]
	    send_nodes = val
	    //Send it on each of those replicas only
	    fmt.Println("There are", len(all_replicas), "replicas which are", val)
	    for i := 0; i < len(all_replicas); i++ {
	    	fmt.Println("Next replica number ", i, " it is ", all_replicas[i])
	    	replica := all_replicas[i]
	    	replica_ip := strings.Split(replica, "&")[1]
			//replica_ip = my_add




			conn2, err2 := grpc.Dial(replica_ip, grpc.WithInsecure())
		    if err2 != nil {
		         fmt.Print("Failed to dail")
		         log.Fatalf("did not connect: %v", err2)
		    }
		    defer conn2.Close()
		    c2 := pb.NewGreeterClient(conn2)

		    // Set timeout
		    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
		    defer cancel()
		    var mytext string

		  //  mytext = my_add+"#"+ sdfsfilename + "_v" + strconv.Itoa(prev_version+1) + "#" + filetext

		    mytext = incoming_nodeid+"#"+ sdfsfilename + "_v" + strconv.Itoa(prev_version+1) +"#" + local_fpath
			r2, err3 := c2.SayPut(ctx, &pb.HelloRequest{Name: "Please make a replica of this file on yourself#"+mytext})
			//r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "VM8;"+my_add+";" })

			if err3 != nil {
				//log.Fatalf("could not greet: %v", err)
				fmt.Println("Could not make a replica!!",err3)
			} else {	
				//If it is able to contact the introducer, then only run its server		   		
				//for {
					//receive the streamed data
					feature, err4:= r2.Recv()
					fmt.Println("Sent from replica node", feature, err4)
			}	
	    	
	    }

	} else{
	// If it does not exist then take any 4 random numbers and put it on those machines
	// Insert this into the dictionary with version 1
	// Insert into the dictionary to what all VM's we are adding it
		sdfsfile_version[sdfsfilename] = 1
		var vmstore string

		file_read, err := ioutil.ReadFile("./../membership_list.txt")
		if err != nil {
			panic(err)
		}
		//fmt.Println(string(file_read))

		var neighbours = strings.Split(string(file_read), "\n")
		neighbours = neighbours[0:len(neighbours)-1]
		var ptr = rand.Intn(len(neighbours))
		var i = 0
		for i < num_replica{
			nbr :=neighbours[ptr]
			//[4] is A/F/L
			nbr_info := strings.Split(nbr, ";")
			node_alive := nbr_info[4]
			if( strings.Contains(node_alive, "A")){
				fmt.Println("Okay! Creating replica on ", nbr)
				i = i+1
				
				node_ip := nbr_info[2]
				send_nodes += nbr
				send_nodes += "#"
			//	node_ip = my_add
				//vmstore = vmstore + nbr_info[0] + "&"+nbr_info[2] + "#"
				vmstore = vmstore + nbr_info[0] + "&"+node_ip + "#"
				conn2, err2 := grpc.Dial(node_ip, grpc.WithInsecure())
			    if err2 != nil {
			         fmt.Print("Failed to dail")
			         log.Fatalf("did not connect: %v", err2)
			    }
			    defer conn2.Close()
			    c2 := pb.NewGreeterClient(conn2)

			    // Set timeout
			    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
			    defer cancel()
			    var mytext string

			    //mytext = my_add+"#"+ sdfsfilename + "_v1" +"#" + filetext
				mytext = incoming_nodeid+"#"+ sdfsfilename + "_v1" +"#" + local_fpath
			    

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
						if err4!=nil{
							//There is some error
							fmt.Println("There is some error", err4)
						} else{
							fmt.Println(feature.Message)
						}
						
				}	

			}
			

			ptr = (ptr + 1)%len(neighbours)
		}
		sdfsfile_machine[sdfsfilename] = vmstore
	}

	// var i int
	// for i = 0; i < len(neighbours); i++ {
	// 	if( strings.Contains(neighbours[i], put_nodeid)){
	// 		break;
	// 	}
	// 	// fmt.Println(i, neighbours[i])
	// }
	// var send_nodes string
	// for l :=-1; l<=1; l++{
	// 	if l==0 {
	// 		continue
	// 	}
	// 	neighbour_ind := ((i+l)+len(neighbours))%len(neighbours)
	// 	//fmt.Println("l", l, "neighbour_ind",neighbour_ind, "len", len(neighbours))
	// 	nbr := neighbours[neighbour_ind]
	// 	send_nodes += nbr
	// 	send_nodes += "#"
	// }

	j.Message +=send_nodes
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
