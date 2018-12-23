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
	"strconv"
	"math"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

// server is used to implement helloworld.GreeterServer.
type server struct{
	
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(in *pb.HelloRequest, stream pb.Greeter_SayHelloServer) error {
	
	var grep_split = strings.Split(in.Name, "#")
	var grep_search = strings.TrimRight(grep_split[0], "\r\n")
	var grep_flag = strings.TrimRight(grep_split[1], "\r\n")
	
	//Find which of the log files exists on current VM
	logfiles_read, err := ioutil.ReadFile("logfiles.txt") // just pass the file name
        if err != nil {
                 fmt.Print(err)
        }
	logfiles := strings.Split(string(logfiles_read), "\n")
        logfiles = logfiles[:len(logfiles)-1]
        
        //To find if the logfiles exist on the machine
        var searchfile string
        for i:=0; i<len(logfiles); i++{
	 	if _, err := os.Stat(logfiles[i]); err == nil {
 	 		 searchfile =logfiles[i]
                         break
		}
	}
	
	//Execute the grep command
	output_bytes, err:= exec.Command("grep", grep_flag, grep_search, searchfile).CombinedOutput()
     	if err != nil {
         	log.Println(err)
      	}
	output := string(output_bytes)
	
	//Append the file name and the line numbers
	output = searchfile + " : " + strings.Replace(output, "\n", "\n" + searchfile + " : ", -1)
	output = strings.TrimRight(output, searchfile + " : ")
	s1 := strings.Split(output, "\n")
	formatted_output := output
	formatted_output = formatted_output + searchfile +":Line count:" + strconv.Itoa(len(s1)-1)
	
	var len_formout = float64(len(formatted_output))	
	fmt.Println(len_formout)
	
	//Start server streaming assuming that the chuck size is 2 million characters
	for g:= float64(0); g < len_formout; g = g+2000000 {

		var j pb.HelloReply
		j.Message = formatted_output[int(g):int(math.Min(float64(len_formout), float64(g+2000000)))]
		fmt.Println(len(j.Message))
		
		if err := stream.Send(&j); err != nil {
				fmt.Println(err)
				return err
		}
	}
	fmt.Println("Ouside the streaming server")
	
	return nil 
}

func main() {

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
