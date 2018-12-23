PROJECT TITLE
CS425 MP2 – Distributed Group Membership

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


git clone https://gitlab.engr.illinois.edu/vandana2/425-MP-2.git



-To run the server

cd 425-mp-2/greeter_server/

go run main.go





-To run the client:

cd 425-mp-2/greeter_client/

go run main.go 1

1 can be replaced by 2 or 3 as follows:
1 - To join the node
2 - To leave the node
3 - To simulate packet loss (node joins)




-To get self's membership list or self's ID:

cd 425-mp-2/greeter_client/

go run mystatus.go




-In our system VM9 is the introducer hence on VM9 do:

cd 425-mp-2/greeter_server/

go run main.go



and then on another prompt on VM9 do:

cd 425-mp-2/greeter_client/

go run main.go 1



-Then to add any node to the group, for that node run the server in one terminal and then on another terminal run the client:

-To find bandwidth do: 

cd 425-mp-2/greeter_server/ 

go run nettop.go