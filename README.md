
 🔢 
 ### My HyperLogLog and Presto-style HyperLogLog implementation  
- Source code:
  - [`src/primer/hyperloglog.cpp`](src/primer/hyperloglog.cpp)  
  - [`src/include/primer/hyperloglog.h`](src/include/primer/hyperloglog.h)  
  - [`src/primer/hyperloglogpresto.cpp`](src/primer/hyperloglog_presto.cpp)  
  - [`src/include/primer/hyperloglogpresto.h`](src/include/primer/hyperloglog_presto.h)  

---

📖 This project was inspired by the [CMU 15-445 Database Systems](https://github.com/cmu-db/bustub) course.  
Special thanks to **Prof. Andy Pavlo** — his lectures on [Database Systems (YouTube)](https://www.youtube.com/watch?v=otE2WvX3XdQ&list=PLSE8ODhjZXjYDBpQnSymaectKjxCy6BYq&index=1) motivated me to implement a Presto-style HyperLogLog for scalable cardinality estimation.  

-----------------


BusTub is a relational database management system built at [Carnegie Mellon University](https://db.cs.cmu.edu) for the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course. This system was developed for educational purposes and should not be used in production environments.

BusTub supports basic SQL and comes with an interactive shell. You can get it running after finishing all the course projects.

<img src="logo/sql.png" alt="BusTub SQL" width="400">


### Linux (Recommended) / macOS (Development Only)

To ensure that you have the proper packages on your machine, run the following script to automatically install them:

```console
# Linux
$ sudo build_support/packages.sh
# macOS
$ build_support/packages.sh
```

Then run the following commands to build the system:

```console
$ mkdir build
$ cd build
$ cmake ..
$ make
```

If you want to compile the system in debug mode, pass in the following flag to cmake:
Debug mode:

```console
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make -j`nproc`
```
This enables [AddressSanitizer](https://github.com/google/sanitizers) by default.

If you want to use other sanitizers,

```console
$ cmake -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER=thread ..
$ make -j`nproc`
```

There are some differences between macOS and Linux (i.e., mutex behavior) that might cause test cases
to produce different results in different platforms. We recommend students to use a Linux VM for running
test cases and reproducing errors whenever possible.
