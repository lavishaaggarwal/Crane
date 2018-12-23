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
	//"os/exec"
	"fmt"
	//"strings"
	"os"
	"math/rand"
	"strconv"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

//Generates Random Integer
func randomInt(min, max int) int {
    return min + rand.Intn(max-min)
}

// Generate a random string of A-Z chars with len = l
func randomString(len int) string {
    bytes := make([]byte, len)
    for i := 0; i < len; i++ {
        bytes[i] = byte(randomInt(65, 90))
    }
    return string(bytes)
}


// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {

	 //var grep_search = strings.TrimRight(in.Name, "\r\n")
	 //fmt.Println("After trim", grep_search)
	 
	 var file_name = "machine."+in.Name+".log"
	 
	 /*output, err:= exec.Command("touch", "/home/vandana/go/src/google.golang.org/grpc/examples/helloworld/generatelog_server/"+file_name).CombinedOutput()*/
	 
	 f, err := os.Create("/home/lavisha2/"+file_name)
	 
	 defer f.Close()
	 
	 //n, err := f.WriteString("This is a known line\nThe line is written in this file created in VM "+in.Name+"\nThe following is a random line: ")
	 //n1, err1 := f.WriteString(randomString(100))
	 
	 mynum, errnum := strconv.Atoi(in.Name)
	 if errnum != nil {
                log.Println(errnum)
         }
	 n, err := f.WriteString("One day, "+in.Name+"big boys were walking.\nThen came "+in.Name+"big dinosaurs running around a house.\n"+in.Name+"hours later, "+in.Name+"little birds came flying in the sky.\nThen "+strconv.Itoa(mynum%2)+"elephant came marching on the roof and each of them brough "+strconv.Itoa(mynum%3)+"kids.\nAn hour later the big dinosaurs fell asleep.\nThey all played for "+in.Name+"minutes.\nThen "+in.Name+"boys went tiptoeing home.\n")

	 n1, err1 := f.WriteString(randomString(500))
	 fmt.Printf("Wrote %d known bytes and %d random bytes \n", n, n1)
	 
	 f.Sync()
	
     	 if err != nil {
         	log.Println(err)
      	 }
      	 
      	 if err1 != nil {
         	log.Println(err)
      	 }
	 //fmt.Println(string(output))
	
	 //return &pb.HelloReply{Message: "RESULT: " + string(output)}, nil
	 return &pb.HelloReply{Message: "Generated file: "+file_name}, nil
}

func main() {
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

