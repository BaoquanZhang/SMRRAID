#ifndef _REPLAY_H
#define _REPLAY_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <aio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <errno.h>
#include <pthread.h>
//#include <signal.h>
//#include <sys/types.h>
//#include <sys/stat.h>

#define SUCCESS 1
#define FAILURE 0
#define BUFSIZE	300
#define RAMSIZE 60 //MB

#define MEM_ALIGN 512  // Memory alignment
#define USE_GLOBAL_BUFF 0 
#define AIO_THREAD_POOL_SIZE    50

#define BYTE_PER_BLOCK		    512     //blk size (bits) 
#define LARGEST_REQUEST_SIZE	1024 * 2 * 3  //1MB Largest request size (blks)
#define BLOCK_PER_DRIVE		    (long long)8*1024*1024*1024*2	//8TB Drive capacity (blks)

#define CHUNK_SIZE 512 //raid: chunk size kb
#define MAX_DISKS 10

struct config_info {
	char device[10][64];
	char traceFileName[1024];
	char logFileName[1024];
        unsigned int exec;
        unsigned int idle;
        unsigned int diskNum;
};

struct req_info {
	double time;
	long long lba;
	unsigned int size;
	unsigned int type;
        long long waitTime;
	struct req_info *next;
        struct req_info *last;
        struct req_info *parent;
        unsigned int waitChild;
        long long slat;
        long long lat;
        unsigned int diskid; /* 11 represents the md device */
};

struct trace_info {
	unsigned int inNum;
	unsigned int outNum;
	long long latencySum;
	FILE *logFile;

	struct req_info *front;
	struct req_info *rear;
};

struct aiocb_info {
	struct aiocb* aiocb;
	struct req_info* req;
	long long beginTime_submit;
	long long beginTime_issue;
	struct trace_info *trace;
};

//replay.c
void replay(char *configName);
void config_read(struct config_info *config,const char *filename);
void trace_read(struct config_info *config,struct trace_info *trace);
long long time_now();
long long time_elapsed(long long begin);
static void handle_aio(sigval_t sigval);
static void submit_aio(int fd, void *buf, struct req_info *req, struct trace_info *trace, long long initTime);
static void init_aio();
//raid ops
void split_req(struct req_info *parent, int diskNum, struct trace_info *subtrace);
void preread(int *op, struct req_info *parent, unsigned long long lba, int diskNum, struct trace_info *subtrace);
int submit_trace(int *fd, void *buf, struct trace_info *subtrace, struct trace_info *trace, long long initTime);

//trace queue ops
void queue_push(struct trace_info *trace, struct req_info *req);
void queue_pop(int from_head, struct trace_info *trace, struct req_info *req);
void queue_print(struct trace_info *trace);
void copy_req(struct req_info *src, struct req_info *des);

#endif
