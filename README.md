# lazy

broken distributed mapping and RDD implementation.

in one shell:

    $ docker-compose up

And then launch the job:

    $ go run main.go


The main obstacle I've come up with in trying to implement a simple RDD and network of distributed RPC workers is that (at least for the standard library) the `net/rpc` package won't register arbitrary functions
