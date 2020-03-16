# MiniMapReduce

This is the lab01 of the [MIT 6.824](https://pdos.csail.mit.edu/6.824/schedule.html).

## Usage

First, make sure the word-count plugin is freshly built:

```shell
$ go build -buildmode=plugin ../mrapps/wc.go
```

In the `main` directory, run the master.

```shell
$ rm mr-out*
$ go run mrmaster.go pg-*.txt
```

The `pg-*.txt` arguments to `mrmaster.go` are the input files; each file corresponds to one "split", and is the input to one Map task.

In one or more other windows, run some workers:

```shell
$ go run mrworker.go wc.so
```

When the workers and master have finished, look at the output in `mr-out-*`.

> The intermediate output of each map task is stored in the `main/map`, and the output of the reduce tasks are stored in the main directory.

```shell
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```

To run all the test cases one by one:

```shell
$ ./test_mr.sh
```

## References

[MapReduce](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

