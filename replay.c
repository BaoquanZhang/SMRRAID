#include "replay.h"

#define DEBUG 0


void main(int argc, char *argv[])
{
	replay(argv[1]);
}

void replay(char *configName)
{
	struct config_info *config;
	struct trace_info *trace;
	struct req_info *req;
	int fd[10];
	char *buf;
	int i,j;
	long long initTime,nowTime,reqTime,waitTime;
        long long execTime;//for bzhang's experiment
        struct trace_info *subtrace;
	
	config=(struct config_info *)malloc(sizeof(struct config_info));
	memset(config,0,sizeof(struct config_info));
	trace=(struct trace_info *)malloc(sizeof(struct trace_info));
	memset(trace,0,sizeof(struct trace_info));
	req=(struct req_info *)malloc(sizeof(struct req_info));
	memset(req,0,sizeof(struct req_info));

        subtrace = (struct trace_info *) malloc(sizeof(struct trace_info));

	config_read(config,configName);
	printf("starting warm up with config %s----\n",configName);
	trace_read(config,trace);
	printf("starting replay IO trace %s----\n",config->traceFileName);

        if (DEBUG == 1) {
                printf("Devices:\n");
                j=0;
                while (j < config->diskNum) {
                        printf("%s\n", config->device[j]);
                        j++;
                }
        }
        for (j = 0; j < config->diskNum; j++) {
                fd[j] = open(config->device[j], O_DIRECT | /*O_SYNC |*/ O_RDWR); 
                if (fd[j] < 0) {
                        fprintf(stderr, "Value of errno: %d\n", errno);
                        printf("Cannot open %d\n", j);
                        exit(-1);
                }
        }
	
	if (posix_memalign((void**)&buf, MEM_ALIGN, LARGEST_REQUEST_SIZE * BYTE_PER_BLOCK)) {
		fprintf(stderr, "Error allocating buffer\n");
		return;
	}
	for(i=0; i<LARGEST_REQUEST_SIZE*BYTE_PER_BLOCK; i++) {
		//Generate random alphabets to write to file
		buf[i] = (char)(rand()%26+65);
	}

	init_aio();

	initTime = time_now();
        execTime = 0;
	//printf("initTime=%lld\n",initTime);
	while (trace->front) {
                /* Initiate a sub request statck */
                memset(subtrace,0, sizeof(struct trace_info));
                queue_pop(1, trace, req);
		reqTime = req->time;
		nowTime = time_elapsed(initTime);
#ifdef  __REPLAY_SLEEP__
                if(nowTime-execTime > config->exec * 1000000)
                {
                        sleep(config->idle);
                        execTime = time_elapsed(initTime);
                }
#endif

		nowTime = time_elapsed(initTime);
		while (nowTime < reqTime) {
			//usleep(waitTime);
			nowTime = time_elapsed(initTime);
		}
		req->waitTime = nowTime-reqTime;
                //printf("wait time =%lld us\n",waitTime);
                split_req(req, config->diskNum, subtrace);

                if (DEBUG)
                        queue_print(subtrace);
                
                submit_trace(fd, buf, subtrace, trace, initTime);
	}

        i = 0;
	while (trace->inNum > trace->outNum) {
		printf("trace->inNum=%d\n",trace->inNum);
		printf("trace->outNum=%d\n",trace->outNum);
		printf("begin sleepping 1 second------\n");
		sleep(1);
	}
	//printf("average latency= %Lf\n",(long double)trace->latencySum/(long double)trace->inNum);
	free(buf);
	free(config);
	fclose(trace->logFile);
	free(trace);
	free(req);
}

static void handle_aio(sigval_t sigval)
{
	struct aiocb_info *cb;
	unsigned long long latency_submit,latency_issue;
	int error;
	int count;
        struct req_info *parent;
        struct req_info *sub_req;

	cb = (struct aiocb_info *)sigval.sival_ptr;
	latency_submit = time_elapsed(cb->beginTime_submit);
	latency_issue = time_elapsed(cb->beginTime_issue);
	//cb->trace->latencySum+=latency;

        sub_req = cb->req;
        if (DEBUG == 1) {
                printf("returned: %d, %lld, %d\n", 
                                sub_req->diskid, sub_req->lba, sub_req->size);
        }
        parent = sub_req->parent;
        sub_req->slat = latency_submit;
        sub_req->lat = latency_issue;

	error = aio_error(cb->aiocb);
	if (error) {
		if (error != ECANCELED) {
			fprintf(stderr,"Error completing i/o:%d\n",error);
		} else {
			printf("---ECANCELED error\n");
		}
		return;
	}
	count = aio_return(cb->aiocb);
	if (count < (int)cb->aiocb->aio_nbytes) {
		fprintf(stderr, "Warning I/O completed:%db but requested:%ldb\n",
			count,cb->aiocb->aio_nbytes);
	}
        
        if (parent) {
                if (parent->lat < sub_req->lat) {
                        parent->lat = sub_req->lat;
                        parent->slat = sub_req->slat;
                }
                parent->waitChild -= 1;
                if (DEBUG) {
                        printf("Has parent! Parent %d is waiting %d child.\n", 
                                parent->size, parent->waitChild);
                }
                if (parent->waitChild == 0) {
                        fprintf(cb->trace->logFile,
                                        "%-16lf %-12lld %-12lld %-5d %-2d %-2lld %lld \n",
                                        parent->time, parent->waitTime, parent->lba, parent->size,
                                        parent->type, parent->slat, parent->lat);
                        fflush(cb->trace->logFile);
                        cb->trace->outNum++;
                        if (cb->trace->outNum % 10000 == 0) {
                                printf("---has replayed %d\n",cb->trace->outNum);
                        }
                        free(parent);
                }
        }

        free(sub_req);
        if (USE_GLOBAL_BUFF == 0)
                free((void *)cb->aiocb->aio_buf);
	free(cb->aiocb);
	free(cb);
}

int submit_trace(int *fd, void *buf, struct trace_info *subtrace, struct trace_info *trace, long long initTime)
{
        struct req_info *req;
        req = (struct req_info *)malloc(sizeof(struct req_info));
        
        if (DEBUG)
                printf("submitting reqs: diskid, lba, type, size\n");
        
        while (subtrace->rear) {
                queue_pop(0, subtrace, req);
                
                if (DEBUG)
                        printf("%d, %lld, %d, %d\n", 
                                req->diskid, req->lba, req->type, req->size);
                submit_aio(fd[req->diskid], buf, req, trace, initTime);
        }

        free(req);
        return 0;
}
static void submit_aio(int fd, void *buf, struct req_info *req,struct trace_info *trace,long long initTime)
{
	struct aiocb_info *cb;
	char *buf_new;
	int error = 0;
	//struct sigaction *sig_act;

	cb = (struct aiocb_info *)malloc(sizeof(struct aiocb_info));
	memset(cb, 0, sizeof(struct aiocb_info));//where to free this?
	cb->aiocb = (struct aiocb *)malloc(sizeof(struct aiocb));
	memset(cb->aiocb, 0, sizeof(struct aiocb));//where to free this?
	cb->req=(struct req_info *)malloc(sizeof(struct req_info));
	memset(cb->req, 0, sizeof(struct req_info));

	cb->aiocb->aio_fildes = fd;
	cb->aiocb->aio_nbytes = req->size;
	cb->aiocb->aio_offset = req->lba;
        cb->aiocb->aio_reqprio = 1;

	cb->aiocb->aio_sigevent.sigev_notify = SIGEV_THREAD;
	cb->aiocb->aio_sigevent.sigev_notify_function = handle_aio;
	cb->aiocb->aio_sigevent.sigev_notify_attributes = NULL;
	cb->aiocb->aio_sigevent.sigev_value.sival_ptr = cb;

	/* error=sigaction(SIGIO,sig_act,NULL);
	 * write and read different buffer
         */
	if (USE_GLOBAL_BUFF != 1) {
		if (posix_memalign((void**)&buf_new, MEM_ALIGN, req->size + 1)) {
			fprintf(stderr, "Error allocating buffer\n");
		}
		cb->aiocb->aio_buf = buf_new;
	} else {
		cb->aiocb->aio_buf = buf;
	}

        copy_req(req, cb->req);

        cb->beginTime_submit = time_now();// latency from the req was submitted
        cb->beginTime_issue = req->time+initTime; //latency from the req was issued 
	
        cb->trace=trace;
        
	if (req->type == 1)
		error = aio_write(cb->aiocb);
	else
		error = aio_read(cb->aiocb);
	
        if (error) {
                fprintf(stderr, "Error performing i/o");
		exit(-1);
	}
}

static void init_aio()
{
	struct aioinit aioParam={0};
	//memset(aioParam,0,sizeof(struct aioinit));
	//two thread for each device is better
	aioParam.aio_threads = AIO_THREAD_POOL_SIZE;
	aioParam.aio_num = 2048;
	aioParam.aio_idle_time = 1;	
	aio_init(&aioParam);
}

void config_read(struct config_info *config,const char *filename)
{
	int name,value;
	char line[BUFSIZE];
	char *ptr;
	FILE *configFile;
        int diskid;
	
	configFile = fopen(filename,"r");
	if(configFile == NULL) {
		printf("error: opening config file\n");
		exit(-1);
	}
	/* read config file */
	memset(line, 0, sizeof(char)*BUFSIZE);
        diskid = 0;
	while (fgets(line, sizeof(line), configFile)) {
		if(line[0] == '#'||line[0] == ' ') {
			continue;
		}
                ptr = strchr(line, '=');
                if (!ptr) {
			continue;
		} 
                name = ptr-line; //the end of name string+1
                value = name+1;	 //the start of value string
                while (line[name-1] == ' ')
			name--;

                line[name] = 0;

		if (strcmp(line, "device") == 0 && diskid < 10)
			sscanf(line + value, "%s", config->device[diskid++]);
		else if (strcmp(line, "trace") ==0)
			sscanf(line + value, "%s", config->traceFileName);
		else if (strcmp(line, "log") == 0)
			sscanf(line + value, "%s", config->logFileName);
		else if (strcmp(line, "exectime") == 0)
			sscanf(line + value, "%d", &config->exec);
		else if(strcmp(line, "idletime") == 0)
			sscanf(line+value, "%d", &config->idle);

		memset(line, 0, sizeof(char) * BUFSIZE);
	}
        config->diskNum = diskid;
	fclose(configFile);
}

void trace_read(struct config_info *config,struct trace_info *trace)
{
	FILE *traceFile;
	char line[BUFSIZE];
	struct req_info* req;

	traceFile = fopen(config->traceFileName,"r");
	if (traceFile == NULL) {
		printf("error: opening trace file\n");
		exit(-1);
	}
	//initialize trace file parameters
	trace->inNum = 0;
	trace->outNum = 0;
	trace->latencySum = 0;
	trace->logFile = fopen(config->logFileName, "w");
        
        req = (struct req_info *)malloc(sizeof(struct req_info));

	while (fgets(line, sizeof(line), traceFile)) {
		if (strlen(line) == 2)
			continue;
		trace->inNum++;	//track the process of IO requests
                //time:ms, lba:sectors, size:sectors, type:1<->write 0<-->read
		sscanf(line, "%lf %lld %d %d", 
                                &req->time, &req->lba, &req->size, &req->type);
		//push into request queue
		req->time = req->time * 1000;	//ms-->us
		req->size = req->size * BYTE_PER_BLOCK;
		req->lba = req->lba * BYTE_PER_BLOCK;
                req->waitTime = 0;
                req->waitChild = 0;
                req->parent = NULL;
		queue_push(trace, req);
	}

        if (DEBUG)
                queue_print(trace);
	
        fclose(traceFile);
        free(req);
}

long long time_now()
{
	struct timeval now;
	gettimeofday(&now, NULL);
	return 1000000 * now.tv_sec + now.tv_usec;	//us
}

long long time_elapsed(long long begin)
{
	return time_now() - begin;	//us
}


void preread(int *op, struct req_info *parent, unsigned long long lba, int diskNum, 
                struct trace_info *subtrace)
{
        int i,count = 0;
        int size = CHUNK_SIZE * 1024;
        int mode; /* 0-rmw; 1-rcw */
        struct req_info *tmp;
        if (lba == -1)
                return;

        for (i = 0; i < diskNum; i++) {
                if (op[i] != 1)
                        count++;
                if (count <= diskNum / 2)
                        mode = 1;
                else
                        mode = 0;
        }
        tmp = (struct req_info *)malloc(sizeof(struct req_info));
        tmp->type = 0;
        tmp->lba = lba;
        tmp->size = size;
        tmp->time = parent->time;
        tmp->waitTime = parent->waitTime;
        tmp->waitChild = 0;
        tmp->parent = parent;

        if (mode == 1) {
                for (i = 0; i < diskNum; i++) {
                        if (op[i] != 1) {
                                tmp->diskid = i;
                                queue_push(subtrace, tmp);
                                parent->waitChild += 1;
                        }
                }
        }
        else if (mode == 0) {
                for (i=0; i < diskNum; i++) {
                        if (op[i] != 0) {
                                tmp->diskid = i;
                                queue_push(subtrace, tmp);
                                parent->waitChild += 1;
                        }
                }
        }

        free(tmp);

}

void split_req(struct req_info *req, int diskNum, struct trace_info *subtrace)
{
        unsigned int chunk_size = CHUNK_SIZE * 1024;
        unsigned int stripe_size = chunk_size * (diskNum - 1);
        int i, len;
        long long req_end;
        unsigned long stripe_id;
        unsigned int data_id;
        unsigned int chunk_offset;
        unsigned int parity_id;
        unsigned int disk_id;
        unsigned long long lba;
        unsigned long long slice;
        int op[MAX_DISKS] = {0};
        
        struct req_info *parity_req = (struct req_info *)malloc(sizeof(struct req_info));
        struct req_info *sub_req = (struct req_info *)malloc(sizeof(struct req_info));
        struct req_info *parent = (struct req_info *)malloc(sizeof(struct req_info));
        
        copy_req(req, parent);

        /* Start to split request */
        stripe_id = 0;
        parity_id = 0;
        lba = -1;
        req_end = parent->lba + parent->size;
        
        if (DEBUG) {
                printf ("########request lba = %lld, size = %d\n", parent->lba, parent->size);
                printf ("Disk num = %d\n", diskNum);
        }
        
        for (slice = parent->lba; ; slice += chunk_size) {
                if (slice + chunk_size <= req_end)
                        len = chunk_size;
                else if (slice + chunk_size > req_end)
                        len = req_end - slice;

                if (DEBUG)
                        printf("len = %d\n", len);

                if (len <= 0)
                        break;

                data_id = (slice % stripe_size) / chunk_size;
                chunk_offset = slice % chunk_size;
                
                if (stripe_id != slice / stripe_size) {
                       
                        memset(op, 0 , sizeof(op)); /* reset op for new stripe */
                        stripe_id = slice / stripe_size;
                        parity_id = stripe_id % diskNum;
                        lba = (long long) (stripe_id * stripe_size) + chunk_offset;
                        
                        if (DEBUG) {
                                printf("stripe id = %ld\n", stripe_id);
                                printf("parity id = %d, lba = %lld\n", parity_id, lba);
                        }
                        
                        parity_req->time = parent->time;
                        parity_req->lba = lba;
                        parity_req->size = len;
                        parity_req->type = parent->type;
                        parity_req->waitTime = parent->waitTime;
                        parity_req->parent = parent;
                        parity_req->waitChild = 0;
                        op[parity_id] = 1;
                        parity_req->diskid = parity_id;
                        
                        if (parent->type == 1) {
                                queue_push(subtrace, parity_req);
                                parent->waitChild += 1;
                        }
                }

                if (data_id < parity_id)
                        disk_id = data_id;
                else
                        disk_id = data_id + 1;

                if (sub_req == NULL) {
                        printf("allocate sub request failed!\n");
                        exit(-1);
                }

                sub_req->time = parent->time;
                sub_req->lba = lba;
                sub_req->size = len;
                sub_req->type = parent->type;
                sub_req->waitTime = parent->waitTime;
                sub_req->parent = parent;
                sub_req->waitChild = 0;
                sub_req->next = NULL;
                if (len >= chunk_size)
                        op[disk_id] = 1;
                else
                        op[disk_id] = 2;
                sub_req->diskid = disk_id;
                queue_push(subtrace, sub_req);

                parent->waitChild += 1;

                if (DEBUG)
                        printf("disk id = %d, lba = %lld\n", disk_id, lba);

        }
        
        /* finish spliting: pre-read before write */
        if (parent->type == 1) 
                preread(op, parent, lba, diskNum, subtrace);
        
        free(parity_req);
        free(sub_req);
}

void copy_req(struct req_info *src_req, struct req_info *des_req)
{
        des_req->time = src_req->time;
        des_req->size = src_req->size;
        des_req->lba = src_req->lba;
        des_req->type = src_req->type;
        des_req->waitTime = src_req->waitTime;
        des_req->next = src_req->next;
        des_req->last = src_req->last;
        des_req->parent = src_req->parent;
        des_req->waitChild = src_req->waitChild;
        des_req->slat = src_req->slat;
        des_req->lat = src_req->lat;
        des_req->diskid = src_req->diskid;
}

void queue_push(struct trace_info *trace, struct req_info *req)
{
	struct req_info* tmp;
	tmp = (struct req_info *)malloc(sizeof(struct req_info));
        
        copy_req(req, tmp);
	
        if (trace->front == NULL && trace->rear == NULL) {
		trace->front = trace->rear = tmp;
                tmp->last = NULL;
                tmp->next = NULL;
	} else {
		trace->rear->next = tmp;
                tmp->last = trace->rear;
		trace->rear = tmp;
                tmp->next = NULL;
	}
}

void queue_pop(int from_head, struct trace_info *trace, struct req_info *req) 
{
	struct req_info* tmp;
	if (trace->front == NULL) {
		printf("Queue is Empty\n");
		return;
	}
        if (from_head)
                tmp = trace->front;
        else
                tmp = trace->rear;

        copy_req(tmp, req);
        
        if (trace->front == trace->rear) {
                trace->front = trace->rear = NULL;
        } else {
                if (from_head)
                        trace->front = trace->front->next;
                else
                        trace->rear = trace->rear->last;
        }

	free(tmp);
}

void queue_print(struct trace_info *trace)
{
	struct req_info* tmp = trace->front;
        printf("request info: timestamp, lba, size, type, diskid \n");
	while (tmp) {
		printf("%lf ", tmp->time);
		printf("%lld ", tmp->lba);
		printf("%d ", tmp->size);
		printf("%d ", tmp->type);
		printf("%d\n", tmp->diskid);
		tmp = tmp->next;
	}
}
