replayer: replay.c
	gcc -g replay.c -o replay -lrt -pthread
clean:
	rm -f replay
