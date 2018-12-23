PROJECT TITLE

CS425 MP1 - Distribute Log Querier

BACKGROUND

This project uses gRPC as a method of commnication between client and server using Go as the programming language

GETTING STARTED

These instructions helps to install the necessary packages and run the client-server for the grep pattern given by the client

NECESSARY PACKAGES TO BE INSTALLED

1. gRPC requires Go 1.6 higher

    $ go version
    
    For installation instructions, follow this guide: Getting Started - The Go Programming Language using https://golang.org/doc/install
    
2. Install gRPC

    $ go get -u google.golang.org/grpc
    
3. Install Protocol Buffers v3
    
    Install the protoc compiler that is used to generate gRPC service code. The simplest way to do this is to download pre-compiled binaries for your platform(protoc-<version>-<platform>.zip) from here: https://github.com/google/protobuf/releases

    Unzip this file.
    Update the environment variable PATH to include the path to the protoc binary file.
    Next, install the protoc plugin for Go

    $ go get -u github.com/golang/protobuf/protoc-gen-go
    The compiler plugin, protoc-gen-go, will be installed in $GOBIN, defaulting to $GOPATH/bin. It must be in your $PATH for the protocol compiler, protoc, to find it.

    $ export PATH=$PATH:$GOPATH/bin
    
4. Install the protoc plugin for Go

    $ go get -u github.com/golang/protobuf/protoc-gen-go
    The compiler plugin, protoc-gen-go, will be installed in $GOBIN, defaulting to $GOPATH/bin. It must be in your $PATH for the protocol compiler, protoc, to find it.

    $ export PATH=$PATH:$GOPATH/bin

    
RUNNING THE FILES

1. git clone https://gitlab.engr.illinois.edu/vandana2/425-MP-1.git

2. To run the server
    
    cd 425-MP-1/greeter_server/
    
    go run main.go
    
3. To run the client

    cd 425-MP-1/greeter_client/
    
    go run main.go pattern_for_grep grep_flags
    
    Example : go run main.go abcd -ni
    
    If we always want to print the line numbers, do not remove -n from grep_flags.
    
Make the changes with respect to the path of the log files present in 425-MP-1/greeter_server/logfiles.txt and the server IP addresses in 425-MP-1/greeter_client/server_addresses.txt



