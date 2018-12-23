PROJECT TITLE
CS425 MP3 – Distributed File System

BACKGROUND
This project uses Go as the programming language

GETTING STARTED
These instructions help to install the necessary packages and run the client-server for the distributed group membership service

NECESSARY PACKAGES TO BE INSTALLED

Install Protocol Buffers v3
Install the protoc compiler. The simplest way to do this is to download pre-compiled binaries for your platform(protoc--.zip) from here: https://github.com/google/protobuf/releases
Unzip this file.
Update the environment variable PATH to include the path to the protoc binary file.
Next, install the protoc plugin for Go
$ go get -u github.com/golang/protobuf/protoc-gen-go
The compiler plugin, protoc-gen-go, will be installed in $GOBIN, defaulting to $GOPATH/bin. It must be in your $PATH for the protocol compiler, protoc, to find it.
$ export PATH=$PATH:$GOPATH/bin


Install the protoc plugin for Go
$ go get -u github.com/golang/protobuf/protoc-gen-go
The compiler plugin, protoc-gen-go, will be installed in $GOBIN, defaulting to $GOPATH/bin. It must be in your $PATH for the protocol compiler, protoc, to find it.
$ export PATH=$PATH:$GOPATH/bin


RUNNING THE FILES


git clone https://gitlab.engr.illinois.edu/vandana2/425-mp3.git



- To run the server

cd 425-mp-2/greeter_server/

go run main.go





- To run the client:

cd 425-mp-2/greeter_client/

go run main.go 1

1 can be replaced by 2 or 3 as follows:
1 - To join the node
2 - To leave the node
3 - To simulate packet loss (node joins)




- To get self's membership list or self's ID:

cd 425-mp-2/greeter_client/

go run mystatus.go




- In our system VM9 is the introducer hence on VM9 do:

cd 425-mp-2/greeter_server/

go run main.go



and then on another prompt on VM9 do:

cd 425-mp-2/greeter_client/

go run main.go 1


- Then add VM10 which is the original leader in our group
- Then to add any node to the group, for that node run the server in one terminal and then on another terminal run the client:

- To find bandwidth do: 

cd 425-mp-2/greeter_server/ 

go run nettop.go

- Then to run put command, to insert the file into sdfs file system
go run main.go 4 localfilename sdfsfilename

- To run get command, to fetch the file from sdfs file system to local file system
go run main.go get sdfsfilename localfilename 

- To run delete command, to delete all files from all the VMs 
go run main.go delete sdfsfilename

- To run ls command to list all machine (VM) addresses where this file is currently being stored
go run main.go ls sdfsfilename

- To run store, to list all files currently being stored at this machine
go run main.go store

- To run get-versions command, to get the latest version of a file
go run main.go get-versions sdfsfilename numversions localfilename

- To run the different applications, first insert the data source into the sdfs by
go run main.go 4 localfinalname sdfsfilename

- To run TASK 1
go run main.go task1 filename


- To run TASK 2
go run main.go task2 filename

(this requires the static database users.txt to be present in the database hence, put that first using go run main.go 4 users.txt users.txt first)

- To run TASK 3
go run main.go task3 filename

