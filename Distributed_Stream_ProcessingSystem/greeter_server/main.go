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
	"sync"
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
var leader = "172.22.154.101"+port//vm8
// var introducer_add = "172.22.156.101"+port//vm9
// var introducer_add = "172.22.154.101"+port//vm8
var introducer_add = "172.22.158.100"+port//vm7
// var leader = introducer_add
var num_replica = 3
var num_bolt = 1



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

	try_times :=10
	done := 0

	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
		fmt.Println(err)
		file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	    	done = 1

	    }
	    try_times -=1	
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
					log.Println(errs, "error increasing inc number")
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
				if strings.Contains(neighbours[j], "F") && !strings.Contains(split_nbr[4], "F"){
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
			fmt.Println("error atoi", err8)
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
		log.Println("ERROR removing file", err2)
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
		taskrecovery()
	}

}

func handlefollowerfailure(split_ninfo [] string, neighbours [] string){
	
	fmt.Println("Handling follower failure")
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
    	var value_update string
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
	fmt.Println("Detected Failure of ", split_ninfo[2], " Leader is ", leader )

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
		  if err5!=nil{
		  	fmt.Println("Error in fetch file", err5)
		  } else{

			  fmt.Println("Failure recovery", filename)
		      fmt.Println("Returned from FetchFile in Filetransfer", err5)
		      file_output, err1 := r.Recv()
		      if err1 == io.EOF {
		            fmt.Println("EOF Error")
		            //break
		      } else {
		          //  fmt.Println("Output of fetch file in filetransfer", file_output.Message)
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

			  r2, err3 := c2.SayPut(ctx2)//, &pb.HelloRequest{Name:"Some one has failed, please be a replica for this file"+"#"+my_add+"#"+filename + "#" + file_output.Message})
		      if err3 !=nil{
		      	fmt.Println("Error in say put", err3)
		      } else{
		     // fmt.Println("Returned from sayput in Filetransfer", err3)
						var send_data pb.HelloRequest
						send_data.Name = "Some one has failed, please be a replica for this file"+"#!#"+my_add+"#!#"+filename + "#!#"
						if errq := r2.Send(&send_data); errq != nil {
							log.Fatalf("Failed to send a note: %v", errq)
						}
						var len_formout = float64(len(file_output.Message))	
						fmt.Println(len_formout)
						//Start server streaming assuming that the chuck size is 2 million characters
						for g:= float64(0); g < len_formout; g = g+2000000 {
							var jq pb.HelloRequest
							jq.Name = file_output.Message[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
							//fmt.Println(len(j.Message))
							if errq := r2.Send(&jq); errq != nil {
									fmt.Println(errq)
							}
						}
						r2.CloseSend()

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


func taskrecovery(){

	var file_data string
	logfile := "task_status.txt"
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
        log.Print("Failed to dail")
        log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()
    // gotfile := 0
    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: logfile})
    if err!=nil{
            fmt.Println("Error in myget", err)
    } else{
        fmt.Println("Returned from myget")
        run_times := 0
		//receive the streamed data
		for {	
			feature_q, err1:= r.Recv()
			if(feature_q == nil){
				break
			}
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			if len(feature_q.Message)>1000{
				fmt.Println("Output of myget", feature_q.Message[0:1000])
			} else{
				fmt.Println("Output of myget", feature_q.Message)
			}
			fmt.Println(len(feature_q.Message))
			if run_times == 0{
				//want this to run only on the first piece of streaming data
				part := strings.Split(feature_q.Message, "FOUND#")
				file_data += part[1]
				if strings.Contains(part[0], "YES"){
					if len(part[1])>500{
						fmt.Println("Text in file:", part[1][0:500])
					} else{
						fmt.Println("Text in file:", part[1])
					}
					// gotfile = 1
				} else{
					fmt.Println(part[0])
					fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
					fmt.Println("No tasks have been run up till now! No task recovery required.")
					return
					break
				}
				run_times +=1
			} else{
				run_times +=1
				file_data +=feature_q.Message
			}
		}
	}
	// if gotfile == 0{
	// 	var j pb.HelloReply
	// 	j.Message = "Input data source not found"	
	// 	stream.Send(&j)
	// 	return nil
	// }
	data_split := strings.Split(file_data,":")
	if !strings.Contains(data_split[1], "begin"){
		return
	}
	//Okay some task was begun but not complete
	task_number := 0
	if strings.Contains(data_split[0], "task1"){
		task_number = 1
	} else if strings.Contains(data_split[0], "task2"){
		task_number = 2
	} else if strings.Contains(data_split[0],"task3"){
		task_number = 3
	} else{
		return
	}
	recoveryfile := data_split[2]
	var chunk_size int
	var errch error
	if task_number !=1{
		chunk_size, errch = strconv.Atoi(data_split[3])
		if errch!=nil{
			fmt.Println("NOT ABLE TO CONVERT", errch)
			return
		}
	}
	// Now get the file

	var recovery_data string
	conn, err = grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
        log.Print("Failed to dail")
        log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c = pb.NewGreeterClient(conn)
    // Set timeout
    ctx, cancel = context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()
    gotfile := 0
    r, err = c.MyGet(ctx, &pb.HelloRequest{Name: recoveryfile})
    if err!=nil{
            fmt.Println("Error in myget", err)
    } else{
        fmt.Println("Returned from myget")
        run_times := 0
		//receive the streamed data
		for {	
			feature_q, err1:= r.Recv()
			if(feature_q == nil){
				break
			}
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			if len(feature_q.Message)>1000{
				fmt.Println("Output of myget", feature_q.Message[0:1000])
			} else{
				fmt.Println("Output of myget", feature_q.Message)
			}
			fmt.Println(len(feature_q.Message))
			if run_times == 0{
				//want this to run only on the first piece of streaming data
				part := strings.Split(feature_q.Message, "FOUND#")
				recovery_data += part[1]
				if strings.Contains(part[0], "YES"){
					if len(part[1])>500{
						fmt.Println("Text in file:", part[1][0:500])
					} else{
						fmt.Println("Text in file:", part[1])
					}
					gotfile = 1
				} else{
					fmt.Println(part[0])
					fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
					break
				}
				run_times +=1
			} else{
				run_times +=1
				recovery_data +=feature_q.Message
			}
		}
	}
	if gotfile == 0{
		fmt.Println("Input data source not found")	
		return
	}

	//In case of task3 also need the user database:
	var user_data_processed string
	if task_number == 3{
		//Also retrieve the USER DATABASE
		gotfile = 0
		var user_data string
		r, err = c.MyGet(ctx, &pb.HelloRequest{Name: "users.txt"})
	    if err!=nil{
	            fmt.Println("Error in myget", err)
	    } else{
	        fmt.Println("Returned from myget")
	        run_times := 0
			//receive the streamed data
			for {	
				feature_q, err1:= r.Recv()
				if(feature_q == nil){
					break
				}
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				if len(feature_q.Message)>1000{
					fmt.Println("Output of myget", feature_q.Message[0:1000])
				} else{
					fmt.Println("Output of myget", feature_q.Message)
				}
				fmt.Println(len(feature_q.Message))
				if run_times == 0{
					//want this to run only on the first piece of streaming data
					part := strings.Split(feature_q.Message, "FOUND#")
					user_data += part[1]
					if strings.Contains(part[0], "YES"){
						if len(part[1])>500{
							fmt.Println("Text in file:", part[1][0:500])
						} else{
							fmt.Println("Text in file:", part[1])
						}
						gotfile = 1
					} else{
						fmt.Println(part[0])
						fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
						break
					}
					run_times +=1
				} else{
					run_times +=1
					user_data +=feature_q.Message
				}
			}
		}
		if gotfile == 0{
			fmt.Println("Can retrive the user database.")
			return
		}
		//Iterate through the database and process it for a JOIN
		user_lines := strings.Split(user_data, "\n")
		user_lines = user_lines[0:len(user_lines)-1]
		fmt.Println("len of user_lines ", len(user_lines))
		var topic_users = make(map[string]string)
		for i:=0;i<len(user_lines);i++{
			fmt.Println("Data here: ", user_lines[i])
			split1 := strings.Split(user_lines[i], ":")
			user := split1[0]
			topics := strings.Split(split1[1], ",")
			for j:=0;j<len(topics);j++{
				if val, ok := topic_users[topics[j]]; ok {
				    topic_users[topics[j]] = val + user + ","
				} else{
					topic_users[topics[j]] = user + ","
				}
			}
		}

		for k, v := range topic_users { 
			user_data_processed += k + ":" + v + "\n"
		}

	}





	
	//Okay, finally received all data
	all_lines := strings.Split(recovery_data, "\n")
	line_number := len(all_lines)
	total_calls := int(math.Ceil(float64(len(all_lines))/float64(chunk_size)))
	fmt.Println("RECOVERING ", recoveryfile, chunk_size, task_number, line_number, total_calls)

	//1. read the membership list to get the alive nodes	
	//2. now go from 0 to total_calls-1, for each find out if its bolt result is present in sdfs, if yes do nothing
	// if no, call a bolt on one of the machines and do the processing
	// While doing the above, do not use threads rather do everything sequentially
	//3. At the end call the sink to do the final processing

	//1. Read the membership list
	try_times :=10
	done := 0
	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
        fmt.Println(err)
        file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	        done = 1
	    }
    	try_times -=1
	}
	var alive_nodes string
	var neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	var ptr = rand.Intn(len(neighbours))
	var k = 0
	// var startptr = ptr
	for j:=0; j < len(neighbours);j++{
		nbr :=neighbours[ptr]
		nbr_info := strings.Split(nbr, ";")
		node_alive := nbr_info[4]
		node_ip_check := nbr_info[2]
		// if( strings.Contains(node_alive, "A") && !strings.Contains(node_ip_check, introducer_add) && !strings.Contains(node_ip_check, leader)){
		// 	alive_nodes += node_ip_check + "#"
		// }
		if( strings.Contains(node_alive, "A")){
			alive_nodes += node_ip_check + "#"
			k+=1
		}
		ptr = (ptr + 1)%len(neighbours)
			// if ptr == startptr{
			// 	break
			// }
	}
	fmt.Println("Sending to these to k=", strconv.Itoa(k)," bolts: ", alive_nodes)
	alive_split := strings.Split(alive_nodes, "#")
	alive_split = alive_split[0:len(alive_split)-1]


	// 2. Now go and check each of the total_Calls chunks, if any is not present call a bolt for it
	var chunkend int
	for i:=0; i < total_calls; i++{
			
			// 2.1 First check if the work for this bolt/chunk is already done or not
			chunk_file_name := "task"+strconv.Itoa(task_number)+"_bolt_"+strconv.Itoa(i)+".txt"
			conn7, err7 := grpc.Dial(leader, grpc.WithInsecure())
		    if err7 != nil {
		        log.Print("Failed to dail")
		        log.Fatalf("did not connect: %v", err7)
		    }
		    defer conn7.Close()
		    c7 := pb.NewGreeterClient(conn7)
		    // Set timeout
		    ctx7, cancel7 := context.WithTimeout(context.Background(), time.Second*1000000)
		    defer cancel7()
		    gotfile := 0
		    r7, err7 := c7.MyGet(ctx7, &pb.HelloRequest{Name: chunk_file_name})
		    if err7!=nil{
		            fmt.Println("Error in myget", err7)
		    } else{
		        fmt.Println("Returned from myget")
		        run_times := 0
				//receive the streamed data
				for {	
					feature_q, err1:= r7.Recv()
					if(feature_q == nil){
						break
					}
					if err1 == io.EOF {
						fmt.Println("EOF Error")
						break
					}
					if err1 != nil {
						fmt.Println("ERROR")
					}
					if len(feature_q.Message)>1000{
						fmt.Println("Output of myget", feature_q.Message[0:1000])
					} else{
						fmt.Println("Output of myget", feature_q.Message)
					}
					fmt.Println(len(feature_q.Message))
					if run_times == 0{
						//want this to run only on the first piece of streaming data
						part := strings.Split(feature_q.Message, "FOUND#")
						file_data += part[1]
						if strings.Contains(part[0], "YES"){
							if len(part[1])>500{
								fmt.Println("Text in file:", part[1][0:500])
							} else{
								fmt.Println("Text in file:", part[1])
							}
							gotfile = 1
						} else{
							fmt.Println(part[0])
							fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
							break
						}
						run_times +=1
					} else{
						run_times +=1
						file_data +=feature_q.Message
					}
				}
			}
			if gotfile != 0{
				fmt.Println("BOLT FILE for this chunk present")
				continue
			}

			// 2.2 If the work is not done, then do the work
			chunk_i := i	
			chunkstart:= chunk_size*chunk_i
			if chunk_i == total_calls-1{
				chunkend = line_number-1 
				fmt.Println("Last chunl	", strconv.Itoa(chunkend), strconv.Itoa(line_number), strconv.Itoa(i), strconv.Itoa(chunkstart))
			}else{
				chunkend = chunk_size*(chunk_i+1) - 1
				fmt.Println("unlast chunp", strconv.Itoa(chunkend), strconv.Itoa(line_number), strconv.Itoa(i), strconv.Itoa(chunkstart))
			}
			// file_chunk := file_data[chunkstart:chunkend]
			var file_chunk string
			for j:=chunkstart; j<=chunkend; j++{
				file_chunk +=all_lines[j] + "\n"
			}
			// chunk_info[i] = file_chunk
			fmt.Println("Chunk i zz", strconv.Itoa(i), strconv.Itoa(chunkstart), strconv.Itoa(chunkend))
			if len(file_chunk)>500{
				fmt.Println(file_chunk[0:500]," chunk i over")
			} else{
				fmt.Println(file_chunk," chunk i over")
			}
			fmt.Println(strconv.Itoa(len(file_chunk)))
			node_number := i%len(alive_split)
			conn, err := grpc.Dial(alive_split[node_number], grpc.WithInsecure())
			if err != nil {
				log.Print("Failed to dail")
				log.Fatalf("did not connect: %v", err)
			} else{
					defer conn.Close()
					c := pb.NewGreeterClient(conn)
					// Set timeout
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
					defer cancel()
					//file_chunk = strconv.Itoa(i) + "####" + file_chunk
					fmt.Println("Will callf tasktwobolt")
					// var err error
					// var r pb.Greeter_TaskTwoBoltClient
					if task_number == 2{
						r11, err :=  c.TaskTwoBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})
						fmt.Println("Done callfing tasktwobolt")
						if err!=nil{
							fmt.Println("Error in tasktwospout after tasktwobolt", err)
						} else{
							var send_data pb.HelloRequest
							send_data.Name =  user_data_processed+ "$$$$USER$$$$" + strconv.Itoa(chunk_i) + "####"			
							
							if errq := r11.Send(&send_data); errq != nil {
								log.Fatalf("Failed to send a note: %v", errq)
							}
							var len_formout = float64(len(file_chunk))	
							fmt.Println(len_formout)
							//Start server streaming assuming that the chuck size is 2 million characters
							for g:= float64(0); g < len_formout; g = g+2000000{
								var jq pb.HelloRequest
								jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
								//fmt.Println(len(j.Message))							
								if errq := r11.Send(&jq); errq != nil {
										fmt.Println(errq)
								}
							}
							r11.CloseSend()
							fmt.Println("Returned from tasktwobolt___HHHHHHH")
							file_output, err1 := r11.Recv()
							if err1 == io.EOF {
								fmt.Println("EOF Error")
								//break
							} else {
								fmt.Println("Output of tasktwobolt YYYYYYYY", file_output.Message)
								// ok_string += strings.Split(file_output.Message, "&&")[0]
								// j.Message += file_output.Message
								fmt.Println("AAAA DOne", strconv.Itoa(i), " ",strconv.Itoa(k))
							}
						}

					} else if task_number == 3{
						r11, err :=  c.TaskThreeBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})
						fmt.Println("Done callfing tasktwobolt")
						if err!=nil{
							fmt.Println("Error in tasktwospout after tasktwobolt", err)
						} else{
							var send_data pb.HelloRequest
							send_data.Name =  user_data_processed+ "$$$$USER$$$$" + strconv.Itoa(chunk_i) + "####"			
							
							if errq := r11.Send(&send_data); errq != nil {
								log.Fatalf("Failed to send a note: %v", errq)
							}
							var len_formout = float64(len(file_chunk))	
							fmt.Println(len_formout)
							//Start server streaming assuming that the chuck size is 2 million characters
							for g:= float64(0); g < len_formout; g = g+2000000{
								var jq pb.HelloRequest
								jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
								//fmt.Println(len(j.Message))							
								if errq := r11.Send(&jq); errq != nil {
										fmt.Println(errq)
								}
							}
							r11.CloseSend()
							fmt.Println("Returned from tasktwobolt___HHHHHHH")
							file_output, err1 := r11.Recv()
							if err1 == io.EOF {
								fmt.Println("EOF Error")
								//break
							} else {
								fmt.Println("Output of tasktwobolt YYYYYYYY", file_output.Message)
								// ok_string += strings.Split(file_output.Message, "&&")[0]
								// j.Message += file_output.Message
								fmt.Println("AAAA DOne", strconv.Itoa(i), " ",strconv.Itoa(k))
							}
						}

					}
					
					
					//j.Message += "NEWWWWWW " + alive_split[i] + " WW"
			}
	}
	// fmt.Println("Outside bolt loop. Opening mem again")
	fmt.Println("SSSSS")


	//3. call the sink to do the final processing

	fmt.Println("Connect to sink with ", strconv.Itoa(total_calls))
	conn1, err1 := grpc.Dial(leader, grpc.WithInsecure())
	if err1 != nil {
		log.Print("Failed to dail")
		log.Fatalf("did not connect: %v", err1)
	}
	defer conn1.Close()
	c1 := pb.NewGreeterClient(conn1)
	ctx1, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	defer cancel()
	// var ans string
	sink_input := strconv.Itoa(total_calls)
	sdfs_filename_sink := "task" +strconv.Itoa(task_number) +"_result_"+sink_input
	if task_number == 2{
		r1, err2 := c1.TaskTwoSink(ctx1, &pb.HelloRequest{Name: sdfs_filename_sink+"#"+sink_input})
		if err2!=nil{
			fmt.Println("Error in Task1 Sink", err2)
		} else{
			fmt.Println("Returned from Task1 Sink")
			for {
	      		//receive the streamed data
				feature_w, err1:= r1.Recv()
				if(feature_w == nil){
					break
				}
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				fmt.Println(len(feature_w.Message))
				// ans +=feature_w.Message
			}
		}


	} else if task_number == 3{
		r1, err2 := c1.TaskThreeSink(ctx1, &pb.HelloRequest{Name: sdfs_filename_sink+"#"+sink_input})
		if err2!=nil{
			fmt.Println("Error in Task1 Sink", err2)
		} else{
			fmt.Println("Returned from Task1 Sink")
			for {
	      		//receive the streamed data
				feature_w, err1:= r1.Recv()
				if(feature_w == nil){
					break
				}
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				fmt.Println(len(feature_w.Message))
				// ans +=feature_w.Message
			}
		}
	}
	
	
	// // j.Message += ans
	// var jj pb.HelloReply
	// jj.Message = ans
	// if err := stream.Send(&jj); err != nil {
	// 	fmt.Println(err)
	// 	return err
	// }

	fmt.Println("DOne recovering task: ", task_number)
	return
}

func createdictionary(){

	// file_read, err := ioutil.ReadFile("./../membership_list.txt")
	// if err != nil {
	// 	panic(err)
	// 	fmt.Println("Error in read file in dictionary")
	// }

	try_times :=10
	done := 0

	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
		fmt.Println(err)
		file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	    	done = 1

	    }
	    try_times -=1	
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
		if err!=nil{
			fmt.Println("THERE IS AN ERROR", err)
		} else{
		file_output, err1 := r.Recv()
		if err1 == io.EOF {
		    fmt.Println("EOF Error")
		    //break
		} 
		    // else {
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

	// file_read, err := ioutil.ReadFile("./../membership_list.txt")
	// if err != nil {
	// 	panic(err)
	// }
	//fmt.Println(string(file_read))


	try_times :=10
	done := 0

	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
		fmt.Println(err)
		file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	    	done = 1

	    }
	    try_times -=1	
	}



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
    for l =-2; l<=2; l++{
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
	
			if err1 != nil {
				log.Println("Did not receive ACK ", err1)
			}else{
				fmt.Println("Gossip successfully sent to", nbr, "and received",feature.Message, "\n")
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

//func (s *server) TaskOneBolt(in *pb.HelloRequest, stream pb.Greeter_TaskOneBoltServer) error {
func (s *server) TaskOneBolt(stream pb.Greeter_TaskOneBoltServer) error {
	

	var j2 pb.HelloRequest
	for {
		input_data, err := stream.Recv()
		if err == io.EOF {
			//return nil
			break
		}
		if err != nil {
			//return err
			fmt.Println("client streaming errir", err)
			break
		}
		j2.Name+=input_data.Name
		if len(input_data.Name)>500{
			fmt.Println("input_data.Name: ",input_data.Name[0:500])
		} else{
			fmt.Println(input_data)
		}
		// key := serialize(in.Location)
  //               ... // look for notes to be sent to client
		// for _, note := range s.routeNotes[key] {
		// 	if err := stream.Send(note); err != nil {
		// 		return err
		// 	}
		// }
	}
	var in pb.HelloRequest
	in.Name = j2.Name

	var task1_words = make(map[string]int)

	// leader = introducer_add
	split_text := strings.Split(in.Name, "####")
	chunk_num := split_text[0]
	fmt.Println("Inside Task One Bolt\n\n   QQQWQQWQQQQQ", chunk_num)
	all_words := strings.Split(split_text[1], " ")
	// if strings.Contains(in.Name[len(in.Name)-1], " "){
	// 	all_words = all_words[0:len(all_words)-1]
	// }
	for i := 0;i<len(all_words);i++{
		i_word := all_words[i]
		these_words := strings.Split(i_word, "\n")
		//We assume that if there is a \n at the end of a word, we will remove \n. Ex: file\n becomes file
		for k:=0; k<len(these_words);k++{
			this_word := these_words[k]
			// fmt.Println(this_word)
			if val, ok := task1_words[this_word]; ok {
				task1_words[this_word] = val + 1
				//fmt.Println("Prev val: ", val, "new val: ", task1_words[this_word])
			} else{
				task1_words[this_word] = 1
			}
		}

		
	}
	fmt.Println("The dictionary now is:", task1_words)
	// Now 	check if the file already exists, if so replace it. Else, create a file and write task1_words to it
	// Can send back the name of the file
	var file_data string
	for k, v := range task1_words {
        file_data += k + " " + strconv.Itoa(v) + "\n"
    }

	sdfsfilename := "task1result_"+chunk_num+".txt"
	
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)


	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", string(file_data))

	r, err := c.IntroPut(ctx)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	
	if err != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err)
	} else {	

			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}

			var len_formout = float64(len(string(file_data)))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {

				var jq pb.HelloRequest
				jq.Name = string(file_data)[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				
				if errq := r.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r.CloseSend()

			fmt.Println("Returned from introput")	
			feature, err1:= r.Recv()
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}


	var j pb.HelloReply
	j.Message = "OK"+chunk_num+"&&"+"Finished from this TaskOneBolt"+file_data
	stream.Send(&j)
	return nil
}

func (s *server) TaskOneSink(in *pb.HelloRequest, stream pb.Greeter_TaskOneSinkServer) error {

	var task1_merge = make(map[string]int)

    filename := strings.Split(in.Name, "#")[0]
    number_b :=  strings.Split(in.Name, "#")[1]
    number_bolts, err := strconv.Atoi(number_b)
    if err!=nil{
    	fmt.Println("prpoblem in strconv", err)
    }
     conn, err := grpc.Dial(leader, grpc.WithInsecure())
     if err != nil {
    log.Print("Failed to dail")
    log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()

    c := pb.NewGreeterClient(conn)

    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()

    for i:= 0; i < number_bolts; i++{

	    file_name := strings.Split(filename, ".txt")[0]
	    file_chunk_name := file_name + "_" + strconv.Itoa(i) +".txt"


	    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: file_chunk_name})
	    if err!=nil{
	            fmt.Println("Error in myget in task1 sink", err)
	    } else{
	        fmt.Println("Returned from myget in task1 sink, val of i", strconv.Itoa(i))
	    //     file_output, err1 := r.Recv()
	    //     if err1 == io.EOF {
	    //         fmt.Println("EOF Error")
	    //         //break
	    //     } else {
	    //         fmt.Println("Output of myget", file_output.Message)
	    //         splitdata := strings.Split(file_output.Message, "YESFOUND#")[1]
	    //         chunk_words := strings.Split(splitdata, "\n")
	    //         chunk_words = chunk_words[0:len(chunk_words)-1]
	    //         for j :=0; j<len(chunk_words);j++{

	    //         	word_info := strings.Split(chunk_words[j], " ")
	    //         	word := word_info[0]
	    //         	word_count, err := strconv.Atoi(word_info[1])
	    //         	//fmt.Println("word ", word, " ", word_count)
	    //         	if err!=nil{
	    //         		fmt.Println("Error in strconv", err)
	    //         	}
	    //         	if val, ok := task1_merge[word]; ok {
					// 	task1_merge[word] = val + word_count
					// } else{ 
					// 	task1_merge[word] = word_count
					// }
					// fmt.Println("DIIIIIIIICTTTTTTTTIONARY now", task1_merge)
	    //         }

	    //     }
			//receive the streamed data
			var file_output_q string
			for {
				feature_q, err1:= r.Recv()
				if(feature_q == nil){
					break
				}
				// fmt.Println(feature_q.Message)
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				fmt.Println(len(feature_q.Message))
				file_output_q +=feature_q.Message
			}
			splitdata := strings.Split(file_output_q, "YESFOUND#")[1]
			chunk_words := strings.Split(splitdata, "\n")
			chunk_words = chunk_words[0:len(chunk_words)-1]
			for j :=0; j<len(chunk_words);j++{
				word_info := strings.Split(chunk_words[j], " ")
				word := word_info[0]
				word_count, err := strconv.Atoi(word_info[1])
				//fmt.Println("word ", word, " ", word_count)
				if err!=nil{
					fmt.Println("Error in strconv", err)
				}
				if val, ok := task1_merge[word]; ok {
					task1_merge[word] = val + word_count
				} else{ 
					task1_merge[word] = word_count
				}
				fmt.Println("DIIIIIIIICTTTTTTTTIONARY now", task1_merge)
			}
	    }
    }
    var final_result string
    for k, v := range task1_merge {
        final_result += k + " " + strconv.Itoa(v) + "\n"
    }

    // var j pb.HelloReply
    // j.Message = final_result
    // stream.Send(&j)

	var len_formout = float64(len(final_result))	
	fmt.Println(len_formout)
	
	//Start server streaming assuming that the chuck size is 2 million characters
	for g:= float64(0); g < len_formout; g = g+2000000 {

		var j pb.HelloReply
		j.Message = final_result[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
		fmt.Println(len(j.Message))
		
		if err := stream.Send(&j); err != nil {
				fmt.Println(err)
				return err
		}
	}

    return nil
}

func (s *server) TaskTwoSink(in *pb.HelloRequest, stream pb.Greeter_TaskTwoSinkServer) error {

	// leader = introducer_add
	output_filename := strings.Split(in.Name, "#")[0]
    number_b :=  strings.Split(in.Name, "#")[1]
    number_bolts, err := strconv.Atoi(number_b)
    if err!=nil{
    	fmt.Println("prpoblem in strconv", err)
    }
     conn, err := grpc.Dial(leader, grpc.WithInsecure())
     if err != nil {
	    log.Print("Failed to dail")
	    log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()

    var final_result string
    total_num := 0
    total_score := 0
    total_comments := 0
    for i:= 0; i < number_bolts; i++{
		sdfsfilename := "task2_bolt_"+strconv.Itoa(i)+".txt"
	    // file_name := strings.Split(filename, ".txt")[0]
	    // file_chunk_name := file_name + "_" + strconv.Itoa(i) +".txt"
	    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: sdfsfilename})
	    if err!=nil{
	            fmt.Println("Error in myget in task1 sink", err)
	    } else{
	        fmt.Println("Returned from myget in task1 sink, val of i", strconv.Itoa(i))
			var file_output_q string
			for {
				feature_q, err1:= r.Recv()
				if(feature_q == nil){
					break
				}
				// fmt.Println(feature_q.Message)
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				fmt.Println(len(feature_q.Message))
				file_output_q +=feature_q.Message
			}
			splitdata := strings.Split(file_output_q, "YESFOUND#")[1]
			final_result +=splitdata
			i_data := strings.Split(splitdata, "$")
			i_num, err1 := strconv.Atoi(i_data[0])
			if err1!=nil{
				fmt.Println("problem in i_num ", err1)
			}
			i_comment, err2 := strconv.Atoi(i_data[1])
			if err2!=nil{
				fmt.Println("problem in i_comment ", err2)
			}
			i_score, err3 := strconv.Atoi(i_data[2])
			if err3!=nil{
				fmt.Println("problem in i_score ", err3)
			}
			total_num += i_num
			total_score += i_score
			total_comments +=i_comment
	    }
    }
    cumulated_result := "ANS:"+strconv.Itoa(total_num) + "$" + strconv.Itoa(total_comments) + "$" + strconv.Itoa(total_score) + "$"
    // final_result += cumulated_result
    final_result = cumulated_result
    //Store this result in the sdfs
		conn1, err1 := grpc.Dial(leader, grpc.WithInsecure())
	    if err1 != nil {
	         fmt.Print("Failed to dail")
	         log.Fatalf("did not connect: %v", err1)
	    }
	    defer conn1.Close()
	    c1:= pb.NewGreeterClient(conn1)
	    // Set timeout
	    ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel1()
		r, err := c1.IntroPut(ctx1)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(filetext)})
		if err != nil {
			//log.Fatalf("could not greet: %v", err)
			fmt.Println("Could not greet the leader for introput!!",err)
		} else {
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+output_filename + "#!#"
			if errq := r.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(final_result))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = final_result[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r.CloseSend()
			//If it is able to contact the introducer, then only run its server		   		
			//for {
				//receive the streamed data
				feature, err1:= r.Recv()		
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

    //Send back output
	var len_formout = float64(len(final_result))	
	fmt.Println(len_formout)
	//Start server streaming assuming that the chuck size is 2 million characters
	for g:= float64(0); g < len_formout; g = g+2000000 {
		var j pb.HelloReply
		j.Message = final_result[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
		fmt.Println(len(j.Message))
		if err := stream.Send(&j); err != nil {
				fmt.Println(err)
				return err
		}
	}
    return nil
}

func (s *server) TaskThreeSink(in *pb.HelloRequest, stream pb.Greeter_TaskThreeSinkServer) error {

	// leader = introducer_add
	output_filename := strings.Split(in.Name, "#")[0]
    number_b :=  strings.Split(in.Name, "#")[1]
    number_bolts, err := strconv.Atoi(number_b)
    if err!=nil{
    	fmt.Println("prpoblem in strconv", err)
    }
     conn, err := grpc.Dial(leader, grpc.WithInsecure())
     if err != nil {
	    log.Print("Failed to dail")
	    log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()

    var final_result string
    users_score := make(map[string]int)
    for i:= 0; i < number_bolts; i++{
		sdfsfilename := "task3_bolt_"+strconv.Itoa(i)+".txt"
	    // file_name := strings.Split(filename, ".txt")[0]
	    // file_chunk_name := file_name + "_" + strconv.Itoa(i) +".txt"
	    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: sdfsfilename})
	    if err!=nil{
	            fmt.Println("Error in myget in task1 sink", err)
	    } else{
	        fmt.Println("Returned from myget in task3 sink, val of i", strconv.Itoa(i))
			var file_output_q string
			for {
				feature_q, err1:= r.Recv()
				if(feature_q == nil){
					break
				}
				// fmt.Println(feature_q.Message)
				if err1 == io.EOF {
					fmt.Println("EOF Error")
					break
				}
				if err1 != nil {
					fmt.Println("ERROR")
				}
				fmt.Println(len(feature_q.Message))
				file_output_q +=feature_q.Message
			}
			splitdata := strings.Split(file_output_q, "YESFOUND#")[1]
			all_users := strings.Split(splitdata, "\n")
			all_users = all_users[0:len(all_users)-1]
			for j:=0;j<len(all_users);j++{
				// fmt.Println("Data there: ", all_users[j])
				split2 := strings.Split(all_users[j], ":")
				user := split2[0]
				number,errn := strconv.Atoi(split2[1])
				if errn!=nil{
					fmt.Println("problem in number",errn)
				}
				if val, ok := users_score[user]; ok {
				    users_score[user] = val + number
				} else{
					users_score[user] = number
				}
				// fmt.Println("GERE", user, users_score[user])
			}
			final_result +=splitdata
	    }
    }

    var cumulated_result string
    for k, v := range users_score { 
    	cumulated_result += k + ":"+strconv.Itoa(v)+"\n"
	}
	fmt.Println("cumulated_result: ", cumulated_result)
    // final_result += cumulated_result
    final_result = cumulated_result
    //Store this result in the sdfs
		conn1, err1 := grpc.Dial(leader, grpc.WithInsecure())
	    if err1 != nil {
	         fmt.Print("Failed to dail")
	         log.Fatalf("did not connect: %v", err1)
	    }
	    defer conn1.Close()
	    c1:= pb.NewGreeterClient(conn1)
	    // Set timeout
	    ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*10000000)
	    defer cancel1()
		r, err := c1.IntroPut(ctx1)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(filetext)})
		if err != nil {
			//log.Fatalf("could not greet: %v", err)
			fmt.Println("Could not greet the leader for introput!!",err)
		} else {
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+output_filename + "#!#"
			if errq := r.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(final_result))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = final_result[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r.CloseSend()
			//If it is able to contact the introducer, then only run its server		   		
			//for {
				//receive the streamed data
				feature, err1:= r.Recv()		
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

    //Send back output
	var len_formout = float64(len(final_result))	
	fmt.Println(len_formout)
	//Start server streaming assuming that the chuck size is 2 million characters
	for g:= float64(0); g < len_formout; g = g+2000000 {
		var j pb.HelloReply
		j.Message = final_result[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
		fmt.Println(len(j.Message))
		if err := stream.Send(&j); err != nil {
				fmt.Println(err)
				return err
		}
	}
    return nil
}

func (s *server) TaskOneSpout(in *pb.HelloRequest, stream pb.Greeter_TaskOneSpoutServer) error {

	//Get the file from which it has to take the data
	var file_data string
	file := string(in.Name)

	//log this information to the file
	fmt.Println("LOGGING IN THE BEGINNING: task_status.txt")
	log_info := "task1:begin:" + file + ":"
	sdfsfilename := "task_status.txt"
	conn5, err5 := grpc.Dial(leader, grpc.WithInsecure())
    if err5!= nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err5)
    }
    defer conn5.Close()
    c5 := pb.NewGreeterClient(conn5)
	ctx5, cancel5 := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel5()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", log_info)
	r5, err6 := c5.IntroPut(ctx5)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err6 != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err6)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r5.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(log_info))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = log_info[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r5.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r5.CloseSend()
			fmt.Println("Returned from introput inside task1 - spout - loginfo")	
			feature, err1:= r5.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}
	//file overwriting over



	// leader = introducer_add
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
        log.Print("Failed to dail")
        log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()
    gotfile := 0
    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: file})
    if err!=nil{
            fmt.Println("Error in myget", err)
    } else{
        fmt.Println("Returned from myget")
        run_times := 0
		//receive the streamed data
		for {	
			feature_q, err1:= r.Recv()
			if(feature_q == nil){
				break
			}
			// fmt.Println(feature_q.Message)
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			if len(feature_q.Message)>1000{
				fmt.Println("Output of myget", feature_q.Message[0:1000])
			} else{
				fmt.Println("Output of myget", feature_q.Message)
			}
			fmt.Println(len(feature_q.Message))
			if run_times == 0{
				part := strings.Split(feature_q.Message, "FOUND#")
				file_data += part[1]
				if strings.Contains(part[0], "YES"){
					if len(part[1])>500{
						fmt.Println("Text in file:", part[1][0:500])
					} else{
						fmt.Println("Text in file:", part[1])
					}
					gotfile = 1
				} else{
					fmt.Println(part[0])
					fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
					break
				}
				run_times +=1
			} else{
				run_times +=1
				file_data +=feature_q.Message
			}
		}
		// file_output, err1 := r.Recv()
  //       if err1 == io.EOF {
  //           fmt.Println("EOF Error")
  //           //break
  //       } else {
  //           fmt.Println("Output of myget", file_output.Message)
		// 	part := strings.Split(file_output.Message, "FOUND#")
		// 	file_data = part[1]
		// 	if strings.Contains(part[0], "YES"){
		// 		fmt.Println("Text in file:", part[1])
		// 		gotfile = 1
		// 	} else{
		// 		fmt.Println(part[0])
		// 		fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
		// 	}
		// }
	}
	if gotfile == 0{
		var j pb.HelloReply
		j.Message = "Input data source not found"	
		stream.Send(&j)
		return nil
	}
	

	//Read the membership list to find alive nodes which can be used as bolts
	try_times :=10
	done := 0
	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
        fmt.Println(err)
        file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	        done = 1
	    }
    	try_times -=1
	}
	var alive_nodes string
	var neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	var ptr = rand.Intn(len(neighbours))
	var i = 0
	var startptr = ptr
	for i < num_bolt{
		nbr :=neighbours[ptr]
		//[4] is A/F/L
		nbr_info := strings.Split(nbr, ";")
		node_alive := nbr_info[4]
		node_ip_check := nbr_info[2]
		// if( strings.Contains(node_alive, "A") && !strings.Contains(node_ip_check, introducer_add) && !strings.Contains(node_ip_check, leader)){
		// 	alive_nodes += node_ip_check + "#"
		// }
		if( strings.Contains(node_alive, "A")){
			alive_nodes += node_ip_check + "#"
			i+=1
		}
		ptr = (ptr + 1)%len(neighbours)
		if ptr == startptr{
			break
		}
	}
	fmt.Println("Sending to these bolts: ", alive_nodes)
	alive_split := strings.Split(alive_nodes, "#")
	alive_split = alive_split[0:len(alive_split)-1]

	//chunk the file into 4 chunks
	all_words := strings.Split(file_data, " ")
	// for z:=0; z<len(all_words); z++{
	// 	fmt.Println(strconv.Itoa(z), all_words[z])
	// }
	// file_len := len(file_data)
	words_number := len(all_words)
	chunk_size := words_number/num_bolt
	
	var wg sync.WaitGroup
	wg.Add(len(alive_split))

	var chunkend int
	var j pb.HelloReply
	// j.Message = "Finished all bolts in task1"

	//var chunk_info [3]string
	chunk_info := make([]string, num_bolt)

	var ok_string string
	fmt.Println("Creating", num_bolt, " chunks", strconv.Itoa(words_number), strconv.Itoa(chunk_size))
	// rem_failures := 0
	for i:=0; i < num_bolt; i++{

		go func(i int) {	

			defer wg.Done()

			chunkstart:= chunk_size*i
			if i == num_bolt-1{
				chunkend = words_number-1 
				fmt.Println("Last chunl	", strconv.Itoa(chunkend), strconv.Itoa(words_number), strconv.Itoa(i), strconv.Itoa(chunkstart))
			}else{
				chunkend = chunk_size*(i+1)-1
				fmt.Println("unlast chunp", strconv.Itoa(chunkend), strconv.Itoa(words_number), strconv.Itoa(i), strconv.Itoa(chunkstart))
			}


			// file_chunk := file_data[chunkstart:chunkend]
			var file_chunk string
			for j:=chunkstart; j<=chunkend; j++{
				file_chunk +=all_words[j] + " "
			}
			chunk_info[i] = file_chunk

			fmt.Println("Chunk i zz", strconv.Itoa(i), strconv.Itoa(chunkstart), strconv.Itoa(chunkend))
			if len(file_chunk)>500{
				fmt.Println(file_chunk[0:500]," chunk i over")
			} else{
				fmt.Println(file_chunk," chunk i over")
			}

			fmt.Println(strconv.Itoa(len(file_chunk)))
			conn, err := grpc.Dial(alive_split[i], grpc.WithInsecure())
			if err != nil {
				log.Print("Failed to dail")
				log.Fatalf("did not connect: %v", err)
			} else{

				// if rem_failures>0{
				// 	rem_failures -=1
				// } else{
					defer conn.Close()
					c := pb.NewGreeterClient(conn)
					// Set timeout
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
					defer cancel()
					//file_chunk = strconv.Itoa(i) + "####" + file_chunk
					fmt.Println("Will callf taskonebolt")
					r, err :=  c.TaskOneBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})

					fmt.Println("Done callfing taskonebolt")

					if err!=nil{
						fmt.Println("Error in taskonespout before taskonebolt", err)
					} else{

						var send_data pb.HelloRequest
						send_data.Name = strconv.Itoa(i) + "####"
						if errq := r.Send(&send_data); errq != nil {
							log.Fatalf("Failed to send a note: %v", errq)
						}
						var len_formout = float64(len(file_chunk))	
						fmt.Println(len_formout)
						//Start server streaming assuming that the chuck size is 2 million characters
						for g:= float64(0); g < len_formout; g = g+2000000 {
							var jq pb.HelloRequest
							jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
							//fmt.Println(len(j.Message))		
							if errq := r.Send(&jq); errq != nil {
									fmt.Println(errq)
							}
						}
						r.CloseSend()

						fmt.Println("Returned from taskonebolt")
						file_output, err1 := r.Recv()
						if err1 == io.EOF {
							fmt.Println("EOF Error")
							//break
						} else {
							fmt.Println("Output of taskonebolt", file_output.Message)
							ok_string += strings.Split(file_output.Message, "&&")[0]
							j.Message += file_output.Message
						}
					}
					//j.Message += "NEWWWWWW " + alive_split[i] + " WW"
				// }
			}
		}(i)
	}
	wg.Wait()

	fmt.Println("Outside bolt loop. Opening mem again")
	//call the spout to collect the data from all files. Get the output and send it back to the
	file_read, err = ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
        fmt.Println(err)
        file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	        done = 1
	    }
    	try_times -=1
	}
	//fmt.Println(string(file_read))
	

	//FAILURE DETECTION
	neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	// ptr = 0 
	for i:=0; i<num_bolt; i++{
		check_string :=  "OK"+strconv.Itoa(i)
		if strings.Contains(ok_string,check_string){
			fmt.Println("ASuccess in first go for Chunk ", strconv.Itoa(i))
			continue
		}
		for z:=0; z<len(neighbours); z++{
			gotok := 0
			nbr :=neighbours[z]
			//[4] is A/F/L
			nbr_info := strings.Split(nbr, ";")
			node_alive := nbr_info[4]
			node_ip_check := nbr_info[2]
			if( strings.Contains(node_alive, "A")){
				//reschedule chunk i on this
				// chunkstart:= chunk_size*i
				// if i == num_bolt-1{
				// 	chunkend = words_number-1 
				// }else{
				// 	chunkend = chunk_size*(i+1)-1
				// }


				// // file_chunk := file_data[chunkstart:chunkend]
				// var file_chunk string
				// for j:=chunkstart; j<=chunkend; j++{
				// 	file_chunk +=all_words[j] + " "
				// }
				file_chunk := chunk_info[i]
				fmt.Println("FAIL Chunk i ", strconv.Itoa(i), file_chunk )
				conn, err := grpc.Dial(node_ip_check, grpc.WithInsecure())
				if err != nil {
					log.Print("Failed to dail")
					log.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				c := pb.NewGreeterClient(conn)
				// Set timeout
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
				defer cancel()
				// file_chunk = strconv.Itoa(i) + "####" + file_chunk
				r, err :=  c.TaskOneBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})
				if err!=nil{
					fmt.Println("FAIL Error in taskonespout before taskonebolt", err)
				} else{
					fmt.Println("FAIL Returned from taskonebolt")
					file_output, err1 := r.Recv()
					if err1 == io.EOF {
						fmt.Println("EOF Error")
						//break
					} else {
						var send_data pb.HelloRequest
						send_data.Name = strconv.Itoa(i) + "####"
						if errq := r.Send(&send_data); errq != nil {
							log.Fatalf("Failed to send a note: %v", errq)
						}
						var len_formout = float64(len(file_chunk))	
						fmt.Println(len_formout)
						//Start server streaming assuming that the chuck size is 2 million characters
						for g:= float64(0); g < len_formout; g = g+2000000 {
							var jq pb.HelloRequest
							jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
							//fmt.Println(len(j.Message))
							if errq := r.Send(&jq); errq != nil {
									fmt.Println(errq)
							}
						}
						r.CloseSend()
						fmt.Println("FAIL Output of taskonebolt", file_output.Message)
						// ok_failure_string += strings.Split(file_output.Message, "&&")[0]
						j.Message += "HANDLING FAILURE"+file_output.Message
						if strings.Contains(file_output.Message, check_string){
							gotok = 1
						}
					}
				}
				j.Message += "FAILNFEWWWWWWWW"
			}
			if gotok ==1{
				break
			}
		}

	}


	//connect to sink
	fmt.Println("Connect to sink")
	conn1, err1 := grpc.Dial(leader, grpc.WithInsecure())
	if err1 != nil {
		log.Print("Failed to dail")
		log.Fatalf("did not connect: %v", err1)
	}
	defer conn1.Close()
	c1 := pb.NewGreeterClient(conn1)
	// Set timeout
	ctx1, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	defer cancel()
	var ans string
	sdfs_filename_sink := "task1result"
	r1, err2 := c1.TaskOneSink(ctx1, &pb.HelloRequest{Name: sdfs_filename_sink+"#"+strconv.Itoa(num_bolt)})
	if err2!=nil{
		fmt.Println("Error in Task1 Sink", err2)
	} else{
		fmt.Println("Returned from Task1 Sink")
		for {
      		//receive the streamed data
			feature_w, err1:= r1.Recv()
			if(feature_w == nil){
				break
			}
			// fmt.Println(feature_w.Message)
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			fmt.Println(len(feature_w.Message))
			ans +=feature_w.Message
		}
	}


	
	//Start server streaming assuming that the chuck size is 2 million characters
	var len_formout = float64(len(ans))	
	for g:= float64(0); g < len_formout; g = g+2000000 {
		var j pb.HelloReply
		j.Message = ans[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
		fmt.Println(len(j.Message))
		if err := stream.Send(&j); err != nil {
				fmt.Println(err)
				return err
		}
	}
	//stream.Send(&j)
	

	//log this information to the file
	log_info = "task1:over:"
	fmt.Println("LOGGING ONCE: final task_status.txt")
	sdfsfilename = "task_status.txt"


	conn9, err9 := grpc.Dial(leader, grpc.WithInsecure())
    if err9 != nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err9)
    }
    defer conn9.Close()
    c9:= pb.NewGreeterClient(conn9)
	ctx9, cancel9 := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel9()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", log_info)
	r9, err9 := c9.IntroPut(ctx9)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err9 != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err9)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r9.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(log_info))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = log_info[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r9.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r9.CloseSend()
			fmt.Println("Returned from introput inside task1 at the end")	
			feature, err1:= r9.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}
	//file overwriting over



	return nil
}

func (s *server) TaskThreeBolt(stream pb.Greeter_TaskThreeBoltServer) error {
	//This bolt is basically a FILTER BOLT
	// leader = introducer_add
	var j2 pb.HelloRequest
	var chunk_num string
	var user_data string
	run_times := 0
	for {
		input_data, err := stream.Recv()
		if err == io.EOF {
			//return nil
			break
		}
		if err != nil {
			//return err
			fmt.Println("client streaming errir", err)
			break
		}
		if run_times == 0{
			//want this to run only on the first piece of streaming data
			user_data = strings.Split(input_data.Name, "$$$$USER$$$$")[0]
			chunk_num = strings.Split(strings.Split(input_data.Name, "###")[0], "$$$$USER$$$$")[1]
			fmt.Println("Inside bolt NUMBER ", chunk_num)
			run_times +=1
		} else{
			run_times +=1
			// file_data +=feature_q.Message
			j2.Name+=input_data.Name
			if len(input_data.Name)>500{
				fmt.Println("input_data.Name: ",input_data.Name[0:500])
			} else{
				fmt.Println(input_data)
			}
		}		
	}
	var in pb.HelloRequest
	in.Name = j2.Name
	//Finally received all data in in.Name

	var topic_users = make(map[string]string)
	user_split := strings.Split(user_data, "\n")
	user_split = user_split[0:len(user_split)-1]
	for i:=0;i<len(user_split);i++{
		split1 := strings.Split(user_split[i], ":")
		topic := split1[0]
		val := split1[1]
		topic_users[topic] = val
	}


	all_lines := strings.Split(in.Name, "\n")
	all_lines = all_lines[0:len(all_lines)-1]
	var joined_data [] string

	//JOIN OPERATION
	for i:=0;i<len(all_lines);i++{
		all_parts := strings.Split(all_lines[i], ",")
		if len(all_parts)<13{
			continue
		}
		category := all_parts[len(all_parts) - 6]
		if val, ok := topic_users[category]; ok {
    		people := strings.Split(val, ",")
			people = people[0:len(people)-1]
			for j:=0;j<len(people);j++{
				new_tuple := people[j] + "$#$" + all_lines[i]
				joined_data = append(joined_data, new_tuple)
			}
		}
	}

	fmt.Println("In Join Received ", strconv.Itoa(len(all_lines)), " total lines. ")
	fmt.Println("After joining have total ", strconv.Itoa(len(joined_data)), " lines.")
	ans_string := taskthreebolt_transform(joined_data, chunk_num)
	if strings.Contains(ans_string, "DONE"){
		var j pb.HelloReply
		j.Message = "YESOK"+chunk_num+"&&"+"Finished from this TaskThreeBolt"
		fmt.Println("From task-3-bolt final answer to spout YESOK")
		stream.Send(&j)
	} else{
		var j pb.HelloReply
		j.Message = "NOOK"+"&&"+"Not Finished from this TaskThreeBolt"
		fmt.Println("From task-3-bolt final answer to spout NOOK")
		stream.Send(&j)
	}
	return nil
}


func taskthreebolt_transform(data []string, chunk_num string)(string){
	//Now iterate through the data received and perform the transform operation
	// leader = introducer_add
	//TRANSFORM OPERATION
	fmt.Println("In transform Received ", strconv.Itoa(len(data)), " total lines. ")
	users_score := make(map[string]int)
	for _, line := range data {
        all_parts := strings.Split(line, "$#$")
        user := all_parts[0]
        if val, ok := users_score[user]; ok {
		    users_score[user] = val + 1
		} else{
			users_score[user] = 1
		}
    }

    //Now just store this information in the sdfs in file_chunknum.txt
	sdfsfilename := "task3_bolt_"+chunk_num+".txt"

	// file_data := strconv.Itoa(all_num)+"$"+strconv.Itoa(all_comments)+"$"+strconv.Itoa(all_score)+"$"
	var file_data string
	for k, v := range users_score {
		file_data += k + ":" + strconv.Itoa(v)+"\n"
	}
	fmt.Println("We will store: ", file_data)
	
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", string(file_data))
	r, err := c.IntroPut(ctx)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(file_data))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = file_data[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r.CloseSend()
			fmt.Println("Returned from introput inside task2 - transform - bolt")	
			feature, err1:= r.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}
	return "DONE"
}

func (s *server) TaskTwoBolt(stream pb.Greeter_TaskTwoBoltServer) error {
	//This bolt is basically a FILTER BOLT
	// leader = introducer_add
	var j2 pb.HelloRequest
	var chunk_num string
	run_times := 0
	for {
		input_data, err := stream.Recv()
		if err == io.EOF {
			//return nil
			break
		}
		if err != nil {
			//return err
			fmt.Println("client streaming errir", err)
			break
		}
		if run_times == 0{
			//want this to run only on the first piece of streaming data
			chunk_num = strings.Split(input_data.Name, "###")[0]
			fmt.Println("Inside bolt NUMBER ", chunk_num)
			run_times +=1
		} else{
			run_times +=1
			// file_data +=feature_q.Message
			j2.Name+=input_data.Name
			if len(input_data.Name)>500{
				fmt.Println("input_data.Name: ",input_data.Name[0:500])
			} else{
				fmt.Println(input_data)
			}
		}		
	}

	var in pb.HelloRequest
	in.Name = j2.Name
	//Finally received all data in in.Name

	// FILTER OPERATION
	all_lines := strings.Split(in.Name, "\n")
	all_lines = all_lines[0:len(all_lines)-1]

	//Read the filter spec file to determine the categories to filter
		try_timesx :=10
		donex := 0
		file_readx, errx := ioutil.ReadFile("./../filter_spec.txt")
		for try_timesx>0 && (donex==0 && errx!=nil){
	        fmt.Println(errx)
	        file_readx, errx = ioutil.ReadFile("./../filter_spec.txt")
		    if errx != nil {
		        //panic(err)
		        fmt.Println("Errod reading membership list ", errx)
		    } else{
		        donex = 1
		    }
	    	try_timesx -=1
		}
	//
		fmt.Println("Categories we will filter: ", string(file_readx))

	var filtered_data []string
	for i:=0;i<len(all_lines);i++{
		all_parts := strings.Split(all_lines[i], ",")
		if len(all_parts)<13{
			continue
		}
		category := all_parts[len(all_parts) - 6]
		if strings.Contains(category, "funny") || strings.Contains(category, "gifs") || strings.Contains(category, "pics"){
			filtered_data = append(filtered_data, all_lines[i])
			fmt.Println("Keeping ", category)
		}
	}
	fmt.Println("In filter Received ", strconv.Itoa(len(all_lines)), " total lines. ")
	fmt.Println("After filtering left with ", strconv.Itoa(len(filtered_data)), " lines.")
	ans_string := tasktwobolt_transform(filtered_data, chunk_num)
	if strings.Contains(ans_string, "DONE"){
		var j pb.HelloReply
		j.Message = "YESOK"+chunk_num+"&&"+"Finished from this TaskTwoBolt"
		fmt.Println("From task-2-bolt final answer to spout YESOK")
		stream.Send(&j)
	} else{
		var j pb.HelloReply
		j.Message = "NOOK"+"&&"+"Not Finished from this TaskTwoBolt"
		fmt.Println("From task-2-bolt final answer to spout NOOK")
		stream.Send(&j)
	}
	return nil
}

func tasktwobolt_transform(data []string, chunk_num string)(string){
	//Now iterate through the data received and perform the transform operation
	// leader = introducer_add
	//TRANSFORM OPERATION
	all_score := 0
	all_comments := 0
	all_num := 0
	fmt.Println("In transform Received ", strconv.Itoa(len(data)), " total lines. ")

	//read the transform spec file to determine the transform specs to be used for this task

	try_timesx :=10
		donex := 0
		file_readx, errx := ioutil.ReadFile("./../transform_spec.txt")
		for try_timesx>0 && (donex==0 && errx!=nil){
	        fmt.Println(errx)
	        file_readx, errx = ioutil.ReadFile("./../transform_spec.txt")
		    if errx != nil {
		        //panic(err)
		        fmt.Println("Errod reading membership list ", errx)
		    } else{
		        donex = 1
		    }
	    	try_timesx -=1
		}
	//
		fmt.Println("Columns we will use in the transform: ", string(file_readx))


	for _, line := range data {

        all_parts := strings.Split(line, ",")
    	all_num +=1
    	
    	comment,errs := strconv.Atoi(all_parts[len(all_parts) -2])
    	if errs!=nil{
    		fmt.Println("task2 - bolt- transform", errs)
    	}
    	all_comments += comment
    	
    	score, errq := strconv.Atoi(all_parts[len(all_parts) -3])
    	if errq!=nil{
    		fmt.Println("task2 - bolt- transform", errq)
    	}
    	all_score += score
    }

    fmt.Println("In transform: all_num",strconv.Itoa(all_num),"all_score",strconv.Itoa(all_score),"all_comments",strconv.Itoa(all_comments))
    //Now just store this information in the sdfs in file_chunknum.txt
	sdfsfilename := "task2_bolt_"+chunk_num+".txt"
	file_data := strconv.Itoa(all_num)+"$"+strconv.Itoa(all_comments)+"$"+strconv.Itoa(all_score)+"$"
	fmt.Println("We will store: ", file_data)
	
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", string(file_data))
	r, err := c.IntroPut(ctx)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}

			var len_formout = float64(len(file_data))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = file_data[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r.CloseSend()
			fmt.Println("Returned from introput inside task2 - transform - bolt")	
			feature, err1:= r.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}
	return "DONE"
}

func (s *server) TaskTwoSpout(in *pb.HelloRequest, stream pb.Greeter_TaskTwoSpoutServer) error {
	// First of all get the data file on the spout (introducer) so that it can read the contents and then send it the bolts
	var file_data string
	chunk_size := 40
	file := string(in.Name)
	
	//log this information to the file
	fmt.Println("LOGGING IN THE BEGINNING: task_status.txt")
	log_info := "task2:begin:" + file + ":" + strconv.Itoa(chunk_size) + ":"
	sdfsfilename := "task_status.txt"
	conn5, err5 := grpc.Dial(leader, grpc.WithInsecure())
    if err5!= nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err5)
    }
    defer conn5.Close()
    c5 := pb.NewGreeterClient(conn5)
	ctx5, cancel5 := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel5()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", log_info)
	r5, err6 := c5.IntroPut(ctx5)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err6 != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err6)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r5.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(log_info))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = log_info[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r5.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r5.CloseSend()
			fmt.Println("Returned from introput inside task2 - spout - loginfo")	
			feature, err1:= r5.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}
	//file overwriting over




	// leader = introducer_add
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
        log.Print("Failed to dail")
        log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()
    gotfile := 0
    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: file})
    if err!=nil{
            fmt.Println("Error in myget", err)
    } else{
        fmt.Println("Returned from myget")
        run_times := 0
		//receive the streamed data
		for {	
			feature_q, err1:= r.Recv()
			if(feature_q == nil){
				break
			}
			// fmt.Println(feature_q.Message)
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			if len(feature_q.Message)>1000{
				fmt.Println("Output of myget", feature_q.Message[0:1000])
			} else{
				fmt.Println("Output of myget", feature_q.Message)
			}
			fmt.Println(len(feature_q.Message))
			if run_times == 0{
				//want this to run only on the first piece of streaming data
				part := strings.Split(feature_q.Message, "FOUND#")
				file_data += part[1]
				if strings.Contains(part[0], "YES"){
					if len(part[1])>500{
						fmt.Println("Text in file:", part[1][0:500])
					} else{
						fmt.Println("Text in file:", part[1])
					}
					gotfile = 1
				} else{
					fmt.Println(part[0])
					fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
					break
				}
				run_times +=1
			} else{
				run_times +=1
				file_data +=feature_q.Message
			}
		}
	}
	if gotfile == 0{
		var j pb.HelloReply
		j.Message = "Input data source not found"	
		stream.Send(&j)
		return nil
	}
	//Okay, finally received all data

	all_lines := strings.Split(file_data, "\n")
	line_number := len(all_lines)
	// chunk_size := line_number/num_bolt
	total_calls := int(math.Ceil(float64(len(all_lines))/float64(chunk_size)))
	num_calls := 0
	var j pb.HelloReply
for num_calls<total_calls {
	//Now find out which all nodes can be used as bolts from the membership list
	fmt.Println("Num_calls now ", strconv.Itoa(num_calls))
	try_times :=10
	done := 0
	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
        fmt.Println(err)
        file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	        done = 1
	    }
    	try_times -=1
	}
	var alive_nodes string
	var neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	var ptr = rand.Intn(len(neighbours))
	var k = 0
	var startptr = ptr
	for j:=0; j < len(neighbours);j++{
		nbr :=neighbours[ptr]
		//[4] is A/F/L
		nbr_info := strings.Split(nbr, ";")
		node_alive := nbr_info[4]
		node_ip_check := nbr_info[2]
		// if( strings.Contains(node_alive, "A") && !strings.Contains(node_ip_check, introducer_add) && !strings.Contains(node_ip_check, leader)){
		// 	alive_nodes += node_ip_check + "#"
		// }
		if( strings.Contains(node_alive, "A")){
			alive_nodes += node_ip_check + "#"
			k+=1
		}
		ptr = (ptr + 1)%len(neighbours)
		if ptr == startptr{
			break
		}
	}
	fmt.Println("Sending to these to k=", strconv.Itoa(k)," bolts: ", alive_nodes)
	alive_split := strings.Split(alive_nodes, "#")
	alive_split = alive_split[0:len(alive_split)-1]

	// k2 := min(total_calls - num_calls, k)
	// k2 := k
	var k2 int
	if total_calls-num_calls <k{
		k2 = total_calls - num_calls
		fmt.Println("We have smaller k2= ", strconv.Itoa(k2))
	} else{
		k2 = k
	}
	if k2>5{
		k2 = 5
		fmt.Println("We have an even smaller k2= ", strconv.Itoa(k2))
	}
	//chunk the file into 4 chunks and send it to the respective bolts
	var wg sync.WaitGroup
	wg.Add(k2)
	var chunkend int
	
	//var chunk_info [3]string
	// chunk_info := make([]string, num_bolt)
	var ok_string string
	fmt.Println("Creating", num_bolt, " chunks", strconv.Itoa(line_number), strconv.Itoa(chunk_size))
	fmt.Println("Total number of reddit lines: ", strconv.Itoa(line_number))
	rem_failures := 0
	for i:=0; i < k2; i++{
		go func(i int) {
			chunk_i := num_calls + i	
			defer wg.Done()
			chunkstart:= chunk_size*chunk_i
			if chunk_i == total_calls-1{
				chunkend = line_number-1 
				fmt.Println("Last chunl	", strconv.Itoa(chunkend), strconv.Itoa(line_number), strconv.Itoa(i), strconv.Itoa(chunkstart))
			}else{
				chunkend = chunk_size*(chunk_i+1) - 1
				fmt.Println("unlast chunp", strconv.Itoa(chunkend), strconv.Itoa(line_number), strconv.Itoa(i), strconv.Itoa(chunkstart))
			}
			// file_chunk := file_data[chunkstart:chunkend]
			var file_chunk string
			for j:=chunkstart; j<=chunkend; j++{
				file_chunk +=all_lines[j] + "\n"
			}
			// chunk_info[i] = file_chunk
			fmt.Println("Chunk i zz", strconv.Itoa(i), strconv.Itoa(chunkstart), strconv.Itoa(chunkend))
			if len(file_chunk)>500{
				fmt.Println(file_chunk[0:500]," chunk i over")
			} else{
				fmt.Println(file_chunk," chunk i over")
			}
			fmt.Println(strconv.Itoa(len(file_chunk)))
			conn, err := grpc.Dial(alive_split[i], grpc.WithInsecure())
			if err != nil {
				log.Print("Failed to dail")
				log.Fatalf("did not connect: %v", err)
			} else{
				if rem_failures>0{
					rem_failures -=1
				} else{
					defer conn.Close()
					c := pb.NewGreeterClient(conn)
					// Set timeout
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
					defer cancel()
					//file_chunk = strconv.Itoa(i) + "####" + file_chunk
					fmt.Println("Will callf tasktwobolt")
					r, err :=  c.TaskTwoBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})
					fmt.Println("Done callfing tasktwobolt")
					if err!=nil{
						fmt.Println("Error in tasktwospout after tasktwobolt", err)
					} else{
						var send_data pb.HelloRequest
						send_data.Name = strconv.Itoa(chunk_i) + "####"
						if errq := r.Send(&send_data); errq != nil {
							log.Fatalf("Failed to send a note: %v", errq)
						}
						var len_formout = float64(len(file_chunk))	
						fmt.Println(len_formout)
						//Start server streaming assuming that the chuck size is 2 million characters
						for g:= float64(0); g < len_formout; g = g+2000000{
							var jq pb.HelloRequest
							jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
							//fmt.Println(len(j.Message))							
							if errq := r.Send(&jq); errq != nil {
									fmt.Println(errq)
							}
						}
						r.CloseSend()
						fmt.Println("Returned from tasktwobolt___HHHHHHH")
						file_output, err1 := r.Recv()
						if err1 == io.EOF {
							fmt.Println("EOF Error")
							//break
						} else {
							fmt.Println("Output of tasktwobolt YYYYYYYY", file_output.Message)
							ok_string += strings.Split(file_output.Message, "&&")[0]
							j.Message += file_output.Message
							fmt.Println("AAAA DOne", strconv.Itoa(num_calls), " ",strconv.Itoa(k2))
						}
					}
					//j.Message += "NEWWWWWW " + alive_split[i] + " WW"
				}
			}
		}(i)
		// fmt.Println("22222")
	}
	wg.Wait()
	// fmt.Println("Outside bolt loop. Opening mem again")
	fmt.Println("SSSSS")



	//FAILURE DETECTION
	fmt.Println("FAILURE DETECTION. Opening mem again")
	file_read, err = ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
        fmt.Println(err)
        file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	        done = 1
	    }
    	try_times -=1
	}
	neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	
	//FAILURE DETECTION
	// ptr = 0 
	for i:=0; i<k2; i++{
		chunk_i := num_calls + i	
		check_string :=  "OK"+strconv.Itoa(chunk_i)
		if strings.Contains(ok_string,check_string){
			fmt.Println("A Success in first go for Chunk ", strconv.Itoa(chunk_i))
			continue
		}

		chunkstart:= chunk_size*chunk_i
		if chunk_i == total_calls-1{
			chunkend = line_number-1 
		}else{
			chunkend = chunk_size*(chunk_i+1) - 1
		}
		// file_chunk := file_data[chunkstart:chunkend]
		var file_chunk string
		for j:=chunkstart; j<=chunkend; j++{
			file_chunk +=all_lines[j] + "\n"
		}

		for z:=0; z<len(neighbours); z++{
			gotok := 0
			nbr :=neighbours[z]
			nbr_info := strings.Split(nbr, ";")
			node_alive := nbr_info[4]
			node_ip_check := nbr_info[2]
			if( !strings.Contains(node_alive, "A")){
				continue
			}
			// if( strings.Contains(node_alive, "A")){
			
			// fmt.Println("FAIL Chunk i ", strconv.Itoa(chunk_i), file_chunk )
			conn, err := grpc.Dial(node_ip_check, grpc.WithInsecure())
			if err != nil {
				log.Print("Failed to dail")
				log.Fatalf("did not connect: %v", err)
			} else{

				defer conn.Close()
				c := pb.NewGreeterClient(conn)
				// Set timeout
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
				defer cancel()
				// file_chunk = strconv.Itoa(i) + "####" + file_chunk
				r, err :=  c.TaskTwoBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})
				if err!=nil{
					fmt.Println("FAIL Error in tasktwospout before tasktwobolt", err)
				} else{
					fmt.Println("FAIL Returned from tasktwobolt")
					// if err1 == io.EOF {
					// 	fmt.Println("EOF Error")
					// 	//break
					// } else {
						var send_data pb.HelloRequest
						send_data.Name = strconv.Itoa(chunk_i) + "####"
						if errq := r.Send(&send_data); errq != nil {
							log.Fatalf("Failed to send a note: %v", errq)
						}
						var len_formout = float64(len(file_chunk))	
						fmt.Println(len_formout)
						//Start server streaming assuming that the chuck size is 2 million characters
						for g:= float64(0); g < len_formout; g = g+2000000 {
							var jq pb.HelloRequest
							jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
							//fmt.Println(len(j.Message))
							if errq := r.Send(&jq); errq != nil {
									fmt.Println(errq)
							}
						}
						r.CloseSend()
						
						fmt.Println("Returned from tasktwobolt___HHHHHHH")
						file_output, err1 := r.Recv()
						if err1 == io.EOF {
							fmt.Println("EOF Error")
							//break
						} else {
							fmt.Println("FAIL Output of tasktwobolt YYYYYYYY", file_output.Message)
							j.Message += file_output.Message
							fmt.Println("AAAA DOne", strconv.Itoa(num_calls), " ",strconv.Itoa(k2))
						
							// ok_failure_string += strings.Split(file_output.Message, "&&")[0]
							j.Message += "HANDLING FAILURE"+file_output.Message
							if strings.Contains(file_output.Message, check_string){
								gotok = 1
							}
						}
						
					}
				}
				j.Message += "FAILNFEWWWWWWWW"
			// }
			if gotok ==1{
				break
			}
		}
	}

	num_calls += k2
	//Now call the sink and pass it the number of chunks along with the file names, so that he sink calls all those files and
	// computes the answer and saves it as the output
	//connect to sink
	fmt.Println("Connect to sink with ", strconv.Itoa(num_calls))
	conn1, err1 := grpc.Dial(leader, grpc.WithInsecure())
	if err1 != nil {
		log.Print("Failed to dail")
		log.Fatalf("did not connect: %v", err1)
	}
	defer conn1.Close()
	c1 := pb.NewGreeterClient(conn1)
	ctx1, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	defer cancel()
	var ans string
	sink_input := strconv.Itoa(num_calls)
	sdfs_filename_sink := "task2_result_"+sink_input
	r1, err2 := c1.TaskTwoSink(ctx1, &pb.HelloRequest{Name: sdfs_filename_sink+"#"+sink_input})
	if err2!=nil{
		fmt.Println("Error in Task1 Sink", err2)
	} else{
		fmt.Println("Returned from Task1 Sink")
		for {
      		//receive the streamed data
			feature_w, err1:= r1.Recv()
			if(feature_w == nil){
				break
			}
			// fmt.Println(feature_w.Message)	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			fmt.Println(len(feature_w.Message))
			ans +=feature_w.Message
		}
	}


	// j.Message += ans
	var jj pb.HelloReply
	jj.Message = ans
	if err := stream.Send(&jj); err != nil {
		fmt.Println(err)
		return err
	}

}

	//Start server streaming assuming that the chuck size is 2 million characters
	// var len_formout = float64(len(j.Message))	
	// for g:= float64(0); g < len_formout; g = g+2000000 {
	// 	var jj pb.HelloReply
	// 	jj.Message = j.Message[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
	// 	fmt.Println(len(jj.Message))		
	// 	if err := stream.Send(&jj); err != nil {
	// 			fmt.Println(err)
	// 			return err
	// 	}
	// }
	//stream.Send(&j)


	//log this information to the file
	log_info = "task2:over:"
	fmt.Println("LOGGING ONCE: final task_status.txt")
	sdfsfilename = "task_status.txt"


	conn9, err9 := grpc.Dial(leader, grpc.WithInsecure())
    if err9 != nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err9)
    }
    defer conn9.Close()
    c9:= pb.NewGreeterClient(conn9)
	ctx9, cancel9 := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel9()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", log_info)
	r9, err9 := c9.IntroPut(ctx9)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err9 != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err9)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r9.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(log_info))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = log_info[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r9.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r9.CloseSend()
			fmt.Println("Returned from introput inside task2 at the end")	
			feature, err1:= r9.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}



	//file overwriting over



	return nil
}

func (s *server) TaskThreeSpout(in *pb.HelloRequest, stream pb.Greeter_TaskThreeSpoutServer) error {

	// First of all get the data file on the spout (introducer) so that it can read the contents and then send it the bolts
	var file_data string
	file := string(in.Name)
	chunk_size := 40
	
	//log this information to the file
	fmt.Println("LOGGING IN THE BEGINNING: task_status.txt")
	log_info := "task3:begin:" + file + ":" + strconv.Itoa(chunk_size) + ":"
	sdfsfilename := "task_status.txt"
	conn5, err5 := grpc.Dial(leader, grpc.WithInsecure())
    if err5!= nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err5)
    }
    defer conn5.Close()
    c5 := pb.NewGreeterClient(conn5)
	ctx5, cancel5 := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel5()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", log_info)
	r5, err6 := c5.IntroPut(ctx5)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err6 != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err6)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r5.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(log_info))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = log_info[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r5.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r5.CloseSend()
			fmt.Println("Returned from introput inside task3 - spout - loginfo")	
			feature, err1:= r5.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}
	//file overwriting over


	// leader = introducer_add
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
    if err != nil {
        log.Print("Failed to dail")
        log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewGreeterClient(conn)
    // Set timeout
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
    defer cancel()
    gotfile := 0
    r, err := c.MyGet(ctx, &pb.HelloRequest{Name: file})
    if err!=nil{
            fmt.Println("Error in myget", err)
    } else{
        fmt.Println("Returned from myget")
        run_times := 0
		//receive the streamed data
		for {	
			feature_q, err1:= r.Recv()
			if(feature_q == nil){
				break
			}
			// fmt.Println(feature_q.Message)
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			if len(feature_q.Message)>1000{
				fmt.Println("Output of myget", feature_q.Message[0:1000])
			} else{
				fmt.Println("Output of myget", feature_q.Message)
			}
			fmt.Println(len(feature_q.Message))
			if run_times == 0{
				//want this to run only on the first piece of streaming data
				part := strings.Split(feature_q.Message, "FOUND#")
				file_data += part[1]
				if strings.Contains(part[0], "YES"){
					if len(part[1])>500{
						fmt.Println("Text in file:", part[1][0:500])
					} else{
						fmt.Println("Text in file:", part[1])
					}
					gotfile = 1
				} else{
					fmt.Println(part[0])
					fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
					break
				}
				run_times +=1
			} else{
				run_times +=1
				file_data +=feature_q.Message
			}
		}
	}
	if gotfile == 0{
		var j pb.HelloReply
		j.Message = "Input data source not found"	
		stream.Send(&j)
		return nil
	}
	//Okay, finally received all data


	//Also retrieve the USER DATABASE
	gotfile = 0
	var user_data string
	r, err = c.MyGet(ctx, &pb.HelloRequest{Name: "users.txt"})
    if err!=nil{
            fmt.Println("Error in myget", err)
    } else{
        fmt.Println("Returned from myget")
        run_times := 0
		//receive the streamed data
		for {	
			feature_q, err1:= r.Recv()
			if(feature_q == nil){
				break
			}
			// fmt.Println(feature_q.Message)
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			if len(feature_q.Message)>1000{
				fmt.Println("Output of myget", feature_q.Message[0:1000])
			} else{
				fmt.Println("Output of myget", feature_q.Message)
			}
			fmt.Println(len(feature_q.Message))
			if run_times == 0{
				//want this to run only on the first piece of streaming data
				part := strings.Split(feature_q.Message, "FOUND#")
				user_data += part[1]
				if strings.Contains(part[0], "YES"){
					if len(part[1])>500{
						fmt.Println("Text in file:", part[1][0:500])
					} else{
						fmt.Println("Text in file:", part[1])
					}
					gotfile = 1
				} else{
					fmt.Println(part[0])
					fmt.Println("FILE IS NOT AVAILABLE IN THE SDFS!")
					break
				}
				run_times +=1
			} else{
				run_times +=1
				user_data +=feature_q.Message
			}
		}
	}
	if gotfile == 0{
		var j pb.HelloReply
		j.Message = "User datatbase source not found"	
		stream.Send(&j)
		return nil
	}


	//Iterate through the database and process it for a JOIN
	user_lines := strings.Split(user_data, "\n")
	user_lines = user_lines[0:len(user_lines)-1]
	fmt.Println("len of user_lines ", len(user_lines))
	var topic_users = make(map[string]string)
	for i:=0;i<len(user_lines);i++{
		fmt.Println("Data here: ", user_lines[i])
		split1 := strings.Split(user_lines[i], ":")
		user := split1[0]
		topics := strings.Split(split1[1], ",")
		for j:=0;j<len(topics);j++{
			if val, ok := topic_users[topics[j]]; ok {
			    topic_users[topics[j]] = val + user + ","
			} else{
				topic_users[topics[j]] = user + ","
			}
		}
	}

	var user_data_processed string
	for k, v := range topic_users { 
		user_data_processed += k + ":" + v + "\n"
	}





	all_lines := strings.Split(file_data, "\n")
	line_number := len(all_lines)
	// chunk_size := line_number/num_bolt
	total_calls := int(math.Ceil(float64(len(all_lines))/float64(chunk_size)))
	num_calls := 0
	var j pb.HelloReply

for num_calls<total_calls {
	//Now find out which all nodes can be used as bolts from the membership list
	fmt.Println("Num_calls now ", strconv.Itoa(num_calls))
	try_times :=10
	done := 0
	file_read, err := ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
        fmt.Println(err)
        file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	        done = 1
	    }
    	try_times -=1
	}
	var alive_nodes string
	var neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	var ptr = rand.Intn(len(neighbours))
	var k = 0
	var startptr = ptr
	for j:=0; j < len(neighbours);j++{
		nbr :=neighbours[ptr]
		//[4] is A/F/L
		nbr_info := strings.Split(nbr, ";")
		node_alive := nbr_info[4]
		node_ip_check := nbr_info[2]
		// if( strings.Contains(node_alive, "A") && !strings.Contains(node_ip_check, introducer_add) && !strings.Contains(node_ip_check, leader)){
		// 	alive_nodes += node_ip_check + "#"
		// }
		if( strings.Contains(node_alive, "A")){
			alive_nodes += node_ip_check + "#"
			k+=1
		}
		ptr = (ptr + 1)%len(neighbours)
		if ptr == startptr{
			break
		}
	}
	fmt.Println("Sending to these to k=", strconv.Itoa(k)," bolts: ", alive_nodes)
	alive_split := strings.Split(alive_nodes, "#")
	alive_split = alive_split[0:len(alive_split)-1]

	// k2 := min(total_calls - num_calls, k)
	// k2 := k
	var k2 int
	if total_calls-num_calls <k{
		k2 = total_calls - num_calls
		fmt.Println("We have smaller k2= ", strconv.Itoa(k2))
	} else{
		k2 = k
	}
	if k2>5{
		k2 = 5
		fmt.Println("We have an even smaller k2= ", strconv.Itoa(k2))
	}
	//chunk the file into 4 chunks and send it to the respective bolts
	var wg sync.WaitGroup
	wg.Add(k2)
	var chunkend int
	
	//var chunk_info [3]string
	// chunk_info := make([]string, num_bolt)
	var ok_string string
	fmt.Println("Creating", k2, " chunks for total number of lines=", strconv.Itoa(line_number), " with chunksize=",strconv.Itoa(chunk_size))
	fmt.Println("Total number of reddit lines: ", strconv.Itoa(line_number))
	rem_failures := 0
	for i:=0; i < k2; i++{
		go func(i int) {
			defer wg.Done()

			chunk_i := num_calls + i	
			chunkstart:= chunk_size*chunk_i
			if chunk_i == total_calls-1{
				chunkend = line_number-1 
				fmt.Println("LAST CHUNK OVERALL")
			}else{
				chunkend = chunk_size*(chunk_i+1) - 1
				fmt.Println("INTER CHUNK")
			}
			fmt.Println("THIS CHUNK DETAILS zzz : ival", strconv.Itoa(i)," chunk_i:",strconv.Itoa(chunk_i)," begin:", strconv.Itoa(chunkstart), " end:",strconv.Itoa(chunkend), "total:",strconv.Itoa(line_number))

			// file_chunk := file_data[chunkstart:chunkend]
			var file_chunk string
			for j:=chunkstart; j<=chunkend; j++{
				file_chunk +=all_lines[j] + "\n"
			}
			// chunk_info[i] = file_chunk
			// fmt.Println("Chunk i zz", strconv.Itoa(i), strconv.Itoa(chunkstart), strconv.Itoa(chunkend))
			if len(file_chunk)>500{
				fmt.Println(file_chunk[0:500]," chunk i over")
			} else{
				fmt.Println(file_chunk," chunk i over")
			}
			fmt.Println("THIS CHUNK SIZE qqq : ival", strconv.Itoa(i)," chunk_i:",strconv.Itoa(chunk_i), " size: ",strconv.Itoa(len(file_chunk)))
			fmt.Println("THIS CHUNK DEMO www : ival", strconv.Itoa(i)," chunk_i:",strconv.Itoa(chunk_i), " demolen: ",len(strings.Split(file_chunk, "\n")))
			conn, err := grpc.Dial(alive_split[i], grpc.WithInsecure())
			if err != nil {
				log.Print("Failed to dail")
				log.Fatalf("did not connect: %v", err)
			} else{
				if rem_failures>0{
					rem_failures -=1
				} else{
					defer conn.Close()
					c := pb.NewGreeterClient(conn)
					// Set timeout
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
					defer cancel()
					//file_chunk = strconv.Itoa(i) + "####" + file_chunk
					fmt.Println("Will callf taskthreebolt")
					r, err :=  c.TaskThreeBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})
					fmt.Println("Done callfing taskthreebolt")
					if err!=nil{
						fmt.Println("Error in taskthreespout after taskthreebolt", err)
					} else{
						var send_data pb.HelloRequest
						send_data.Name =  user_data_processed+ "$$$$USER$$$$" + strconv.Itoa(chunk_i) + "####"
						if errq := r.Send(&send_data); errq != nil {
							log.Fatalf("Failed to send a note: %v", errq)
						}
						var len_formout = float64(len(file_chunk))	
						fmt.Println(len_formout)
						//Start server streaming assuming that the chuck size is 2 million characters
						for g:= float64(0); g < len_formout; g = g+2000000{
							var jq pb.HelloRequest
							jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
							//fmt.Println(len(j.Message))							
							if errq := r.Send(&jq); errq != nil {
									fmt.Println(errq)
							}
						}
						r.CloseSend()
						fmt.Println("Returned from taskthreebolt___HHHHHHH")
						file_output, err1 := r.Recv()
						if err1 == io.EOF {
							fmt.Println("EOF Error")
							//break
						} else {
							fmt.Println("Output of taskthreebolt YYYYYYYY", file_output.Message)
							ok_string += strings.Split(file_output.Message, "&&")[0]
							j.Message += file_output.Message
							fmt.Println("AAAA DOne", strconv.Itoa(num_calls), " ",strconv.Itoa(k2))
						}
					}
					//j.Message += "NEWWWWWW " + alive_split[i] + " WW"
				}
			}
		}(i)
		// fmt.Println("22222")
	}
	wg.Wait()
	// fmt.Println("Outside bolt loop. Opening mem again")
	fmt.Println("SSSSS")



	//FAILURE DETECTION
	fmt.Println("FAILURE DETECTION. Opening mem again")
	file_read, err = ioutil.ReadFile("./../membership_list.txt")
	for try_times>0 && (done==0 && err!=nil){
        fmt.Println(err)
        file_read, err = ioutil.ReadFile("./../membership_list.txt")
	    if err != nil {
	        //panic(err)
	        fmt.Println("Errod reading membership list ", err)
	    } else{
	        done = 1
	    }
    	try_times -=1
	}
	neighbours = strings.Split(string(file_read), "\n")
	neighbours = neighbours[0:len(neighbours)-1]
	
	//FAILURE DETECTION
	// ptr = 0 
	for i:=0; i<k2; i++{
		chunk_i := num_calls + i	
		check_string :=  "OK"+strconv.Itoa(chunk_i)
		if strings.Contains(ok_string,check_string){
			fmt.Println("A Success in first go for Chunk ", strconv.Itoa(chunk_i))
			continue
		}
		fmt.Println("FFFFFFAAAAIIILLLLLUUUURREEEE TTHHHEEEERRREEEE")
		chunkstart:= chunk_size*chunk_i
		if chunk_i == total_calls-1{
			chunkend = line_number-1 
		}else{
			chunkend = chunk_size*(chunk_i+1) - 1
		}
		// file_chunk := file_data[chunkstart:chunkend]
		var file_chunk string
		for j:=chunkstart; j<=chunkend; j++{
			file_chunk +=all_lines[j] + "\n"
		}

		for z:=0; z<len(neighbours); z++{
			gotok := 0
			nbr :=neighbours[z]
			nbr_info := strings.Split(nbr, ";")
			node_alive := nbr_info[4]
			node_ip_check := nbr_info[2]
			if( !strings.Contains(node_alive, "A")){
				continue
			}
			// if( strings.Contains(node_alive, "A")){
			
			// fmt.Println("FAIL Chunk i ", strconv.Itoa(chunk_i), file_chunk )
			conn, err := grpc.Dial(node_ip_check, grpc.WithInsecure())
			if err != nil {
				log.Print("Failed to dail")
				log.Fatalf("did not connect: %v", err)
			} else{

				defer conn.Close()
				c := pb.NewGreeterClient(conn)
				// Set timeout
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
				defer cancel()
				// file_chunk = strconv.Itoa(i) + "####" + file_chunk
				r, err :=  c.TaskThreeBolt(ctx)//, &pb.HelloRequest{Name: file_chunk})
				if err!=nil{
					fmt.Println("FAIL Error in tasktwospout before tasktwobolt", err)
				} else{
					fmt.Println("FAIL Returned from tasktwobolt")
					// if err1 == io.EOF {
					// 	fmt.Println("EOF Error")
					// 	//break
					// } else {
						var send_data pb.HelloRequest
						send_data.Name =  user_data_processed + "$$$$USER$$$$" + strconv.Itoa(chunk_i) + "####"
						if errq := r.Send(&send_data); errq != nil {
							log.Fatalf("Failed to send a note: %v", errq)
						}
						var len_formout = float64(len(file_chunk))	
						fmt.Println(len_formout)
						//Start server streaming assuming that the chuck size is 2 million characters
						for g:= float64(0); g < len_formout; g = g+2000000 {
							var jq pb.HelloRequest
							jq.Name = file_chunk[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
							//fmt.Println(len(j.Message))
							if errq := r.Send(&jq); errq != nil {
									fmt.Println(errq)
							}
						}
						r.CloseSend()
						
						fmt.Println("Returned from taskthreebolt___HHHHHHH")
						file_output, err1 := r.Recv()
						if err1 == io.EOF {
							fmt.Println("EOF Error")
							//break
						} else {
							fmt.Println("FAIL Output of taskthreebolt YYYYYYYY", file_output.Message)
							j.Message += file_output.Message
							fmt.Println("AAAA DOne", strconv.Itoa(num_calls), " ",strconv.Itoa(k2))
						
							// ok_failure_string += strings.Split(file_output.Message, "&&")[0]
							j.Message += "HANDLING FAILURE"+file_output.Message
							if strings.Contains(file_output.Message, check_string){
								gotok = 1
							}
						}
						
					}
				}
				j.Message += "FAILNFEWWWWWWWW"
			// }
			if gotok ==1{
				break
			}
		}
	}

	num_calls += k2
	//Now call the sink and pass it the number of chunks along with the file names, so that he sink calls all those files and
	// computes the answer and saves it as the output
	//connect to sink
	fmt.Println("Connect to sink with ", strconv.Itoa(num_calls))
	conn1, err1 := grpc.Dial(leader, grpc.WithInsecure())
	if err1 != nil {
		log.Print("Failed to dail")
		log.Fatalf("did not connect: %v", err1)
	}
	defer conn1.Close()
	c1 := pb.NewGreeterClient(conn1)
	ctx1, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
	defer cancel()
	var ans string
	sink_input := strconv.Itoa(num_calls)
	sdfs_filename_sink := "task3_result_"+sink_input
	r1, err2 := c1.TaskThreeSink(ctx1, &pb.HelloRequest{Name: sdfs_filename_sink+"#"+sink_input})
	if err2!=nil{
		fmt.Println("Error in Task3 Sink", err2)
	} else{
		fmt.Println("Returned from Task3 Sink")
		for {
      		//receive the streamed data
			feature_w, err1:= r1.Recv()
			if(feature_w == nil){
				break
			}
			// fmt.Println(feature_w.Message)	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
				break
			}
			if err1 != nil {
				fmt.Println("ERROR")
			}
			fmt.Println(len(feature_w.Message))
			ans +=feature_w.Message
		}
	}
	// j.Message += ans
	var jj pb.HelloReply
	jj.Message = ans
	if err := stream.Send(&jj); err != nil {
		fmt.Println(err)
		return err
	}
}


//log this information to the file
	log_info = "task3:over:"
	fmt.Println("LOGGING ONCE: final task_status.txt")
	sdfsfilename = "task_status.txt"


	conn9, err9 := grpc.Dial(leader, grpc.WithInsecure())
    if err9 != nil {
         fmt.Print("Failed to dail")
         log.Fatalf("did not connect: %v", err9)
    }
    defer conn9.Close()
    c9:= pb.NewGreeterClient(conn9)
	ctx9, cancel9 := context.WithTimeout(context.Background(), time.Second*10000000)
    defer cancel9()
    fmt.Println("QWQWQWQWQW", sdfsfilename, " DAAATAA ", log_info)
	r9, err9 := c9.IntroPut(ctx9)//, &pb.HelloRequest{Name: "PUT#"+my_add+"#"+sdfsfilename + "#"+string(file_data)})
	if err9 != nil {
		//log.Fatalf("could not greet: %v", err)
		fmt.Println("Could not greet the leader for introput!!",err9)
	} else {	
			var send_data pb.HelloRequest
			send_data.Name = "PUT#!#"+my_add+"#!#"+sdfsfilename + "#!#"
			if errq := r9.Send(&send_data); errq != nil {
				log.Fatalf("Failed to send a note: %v", errq)
			}
			var len_formout = float64(len(log_info))	
			fmt.Println(len_formout)
			//Start server streaming assuming that the chuck size is 2 million characters
			for g:= float64(0); g < len_formout; g = g+2000000 {
				var jq pb.HelloRequest
				jq.Name = log_info[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
				//fmt.Println(len(j.Message))
				if errq := r9.Send(&jq); errq != nil {
						fmt.Println(errq)
				}
			}
			r9.CloseSend()
			fmt.Println("Returned from introput inside task3 at the end")	
			feature, err1:= r9.Recv()	
			if err1 == io.EOF {
				fmt.Println("EOF Error")
			}
			if err1 != nil {
				fmt.Println("ERROR")
			} else{
				fmt.Println("Received nodes to put this file on: \n", feature.Message)
			}
	}



	//file overwriting over



	return nil
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
			if err!=nil{
				fmt.Println("Error in local delete", err)
			} else{
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

        fmt.Println("Inside Fetch Files", in.Name)
        // output_files, err := exec.Command("ls", "./../sdfs/").Output()
        // if err != nil {
        //         fmt.Println("ls error")
        //         log.Println(err)
        // }
        // output := string(output_files)
        // if strings.Contains(output, in.Name) {
                fmt.Println("File name", in.Name)
                file_read_byte, err := ioutil.ReadFile("./../sdfs/"+in.Name)
                
                // fmt.Println("File contains")
                if err != nil {
                      //  panic(err)
                        fmt.Println(err)
                }
                file_read := string(file_read_byte)
                if len(string(file_read))>500{
                	fmt.Println("File contains", string(file_read)[0:500])
                } else{
					fmt.Println("File contains", string(file_read))
               	}
                
                // var j pb.HelloReply
                // j.Message = string(file_read)
                // stream.Send(&j)

                // send it in the form of server streaming

				var len_formout = float64(len(file_read))	
				fmt.Println(len_formout)
				
				//Start server streaming assuming that the chuck size is 2 million characters
				for g:= float64(0); g < len_formout; g = g+2000000 {

					var j pb.HelloReply
					j.Message = file_read[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
					//fmt.Println(len(j.Message))
					
					if err := stream.Send(&j); err != nil {
							fmt.Println(err)
							return err
					}
				}


        // }
        return nil

}

func (s *server) MyGet(in *pb.HelloRequest, stream pb.Greeter_MyGetServer) error {
	var j pb.HelloReply
	// j:= GetLiveNodes(in.Name)
	//Iterate
	file_found := 0
	fmt.Println("In Myget")
	myoutput := "File--"

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
    	received_once := 0
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
					// file_output, err1 := r.Recv()
					// if err1 == io.EOF {
					// 	fmt.Println("EOF Error")
					// 	//break
					// } else {
					// 	fmt.Println("Output of fetch file", file_output.Message)
					// 	 j.Message = file_output.Message
					// 	// file_found = 1
					// 	 myoutput += "InnerlOOP"
					// 	break
					// }

					for {
	      			//receive the streamed data
						file_output, err1:= r.Recv()
						if(file_output == nil){
							break
						}
						if err1 == io.EOF {
							fmt.Println("EOF Error")
							break
						}
						if err1 != nil {
							fmt.Println("ERROR____LAAAVISHA")
							
						}
						fmt.Println(len(file_output.Message))
						if len(file_output.Message)>500{
							fmt.Println("Output of fetch file", file_output.Message[0:500])
						} else{
							fmt.Println("Output of fetch file", file_output.Message)
						}
						j.Message += file_output.Message
						// file_found = 1
						myoutput += "InnerlOOP"
						
					}
					received_once = 1

			}
			if received_once == 1{
				break
			}

	    }
	}
	if file_found ==0{
		j.Message = myoutput+"NOFOUND#"+ string(j.Message)
	} else{
		j.Message = myoutput+"YESFOUND#"+ string(j.Message)
	}

	// stream.Send(&j)
	//Start server streaming assuming that the chuck size is 2 million characters
	
	var len_formout = float64(len(j.Message))	
	for g:= float64(0); g < len_formout; g = g+2000000 {

		var jjj pb.HelloReply
		jjj.Message = j.Message[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
		fmt.Println(len(jjj.Message))
		
		if err := stream.Send(&jjj); err != nil {
				fmt.Println(err)
				return err
		}
	}

    
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

//func (s *server) SayPut(in *pb.HelloRequest, stream pb.Greeter_SayPutServer) error {
func (s *server) SayPut(stream pb.Greeter_SayPutServer) error {

	var j2 pb.HelloRequest
	fmt.Println("Inside sayput. Client streaming here..")
	for {
		input_data, err := stream.Recv()
		if err == io.EOF {
			//return nil
			break
		}
		if err != nil {
			//return err
			fmt.Println("client streaming errir", err)
			break
		}

		j2.Name+=input_data.Name
		if len(input_data.Name)>500{
			fmt.Println("input_data.Name: ",input_data.Name[0:500])
		} else{
			fmt.Println(input_data)
		}
		// key := serialize(in.Location)
  //               ... // look for notes to be sent to client
		// for _, note := range s.routeNotes[key] {
		// 	if err := stream.Send(note); err != nil {
		// 		return err
		// 	}
		// }
	}
	var in pb.HelloRequest
	in.Name = j2.Name


	log.Println("I will create replicas on myself!\n")
	if len(in.Name)>500{
		log.Println("Receiving new file ", in.Name[0:500]+"\n")
	}else{
		log.Println("Receiving new file ", in.Name+"\n")
	}
	newinfo := strings.Split(in.Name, "#!#")
	ip := newinfo[1]
	fname := newinfo[2]
	ftext := newinfo[3]
	fmt.Println("Receiving request from ", ip, " and creating replica of ", fname)

	fpath := "./../sdfs/"+fname
	file, err3 := os.Create(fpath)
	if err3 != nil {
		log.Println("ERROR creating file", err3)
	}
	defer file.Close()

	file1, err4 := os.OpenFile(fpath, os.O_RDWR, 0644)
	if err4 != nil {
		log.Println("ERROR opening file", err4)
	}
	defer file1.Close()

	_, err5 := file1.WriteString(ftext)
	if err5 != nil {
		log.Println("ERROR writing into the file", err5)
	}

	var j pb.HelloReply	
	j.Message = "Created replica! You are all set!"
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
	localfilename := mydata[2]
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
		file_output, err1 := r.Recv()
		if err1 == io.EOF {
			fmt.Println("EOF Error")
		//break
		} else {
			fmt.Println("Output of fetch file", file_output.Message)
			 j.Message += file_output.Message
			// break
		}

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
	}
	    	


	stream.Send(&j)
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
//func (s *server) IntroPut(in *pb.HelloRequest, stream pb.Greeter_IntroPutServer) error {
func (s *server) IntroPut(stream pb.Greeter_IntroPutServer) error {
	var j2 pb.HelloRequest

	for {
		input_data, err := stream.Recv()
		if err == io.EOF {
			//return nil
			break
		}
		if err != nil {
			//return err
			fmt.Println("client streaming errir", err)
			break
		}
		j2.Name+=input_data.Name
		if len(input_data.Name)>500{
			fmt.Println("input_data.Name: ",input_data.Name[0:500])
		} else{
			fmt.Println(input_data)
		}
		// key := serialize(in.Location)
        // ... // look for notes to be sent to client
		// for _, note := range s.routeNotes[key] {
		// 	if err := stream.Send(note); err != nil {
		// 		return err
		// 	}
		// }
	}
	var in pb.HelloRequest
	in.Name = j2.Name
	log.Println("Put function on Leader has been called!\n")
	if len(in.Name)>500{
		log.Println("New incoming node: ", in.Name[0:500]+"\n")
	} else{
		log.Println("New incoming node: ", in.Name+"\n")
	}
	var j pb.HelloReply	
	j.Message = "I am send your file to these nodes .. #"
	

	parts := strings.Split(in.Name, "#!#")
	incoming_nodeid := parts[1]
	fmt.Println(incoming_nodeid, " wants to insert a file")
	sdfsfilename := parts[2]
	filetext :=parts[3]
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

		    mytext = my_add+"#!#"+ sdfsfilename + "_v" + strconv.Itoa(prev_version+1) + "#!#" + filetext
			r2, err3 := c2.SayPut(ctx)//, &pb.HelloRequest{Name: "Please make a replica of this file on yourself#"+mytext})
			if err3 != nil {
				//log.Fatalf("could not greet: %v", err)
				fmt.Println("Could not make a replica!!",err3)
			} else {	
					var send_data pb.HelloRequest
					send_data.Name = "Please make a replica of this file on yourself#!#"
					if errq := r2.Send(&send_data); errq != nil {
						log.Fatalf("Failed to send a note: %v", errq)
					}
					var len_formout = float64(len(mytext))	
					fmt.Println(len_formout)
					//Start server streaming assuming that the chuck size is 2 million characters
					for g:= float64(0); g < len_formout; g = g+2000000 {
						var jq pb.HelloRequest
						jq.Name = mytext[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
						//fmt.Println(len(j.Message))
						if errq := r2.Send(&jq); errq != nil {
								fmt.Println(errq)
						}
					}
					r2.CloseSend()
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

		// file_read, err := ioutil.ReadFile("./../membership_list.txt")
		// if err != nil {
		// 	panic(err)
		// }

		try_times :=10
		done := 0

		file_read, err := ioutil.ReadFile("./../membership_list.txt")
		for try_times>0 && (done==0 && err!=nil){
			fmt.Println(err)
			file_read, err = ioutil.ReadFile("./../membership_list.txt")
		    if err != nil {
		        //panic(err)
		        fmt.Println("Errod reading membership list ", err)
		    } else{
		    	done = 1
		    }
		    try_times -=1	
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

			    mytext = my_add+"#!#"+ sdfsfilename + "_v1" +"#!#" + filetext
				r2, err3 := c2.SayPut(ctx)//, &pb.HelloRequest{Name: "Please make a replica of this file on yourself#"+mytext})
				//r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "VM8;"+my_add+";" })

				if err3 != nil {
					//log.Fatalf("could not greet: %v", err)
					fmt.Println("Could not make a replica!!",err)
				} else {	

					var send_data pb.HelloRequest
					send_data.Name = "Please make a replica of this file on yourself#!#"
					if errq := r2.Send(&send_data); errq != nil {
						log.Fatalf("Failed to send a note: %v", errq)
					}
					var len_formout = float64(len(mytext))	
					fmt.Println(len_formout)
					//Start server streaming assuming that the chuck size is 2 million characters
					for g:= float64(0); g < len_formout; g = g+2000000 {
						var jq pb.HelloRequest
						jq.Name = mytext[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
						//fmt.Println(len(j.Message))
						if errq := r2.Send(&jq); errq != nil {
								fmt.Println(errq)
						}
					}
					r2.CloseSend()
					//If it is able to contact the introducer, then only run its server		   		
					//for {
						//receive the streamed data
						feature, err4:= r2.Recv()
						if err4!=nil{
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
