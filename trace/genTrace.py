import sys
from random import randint

filename = sys.argv[1]
total_size = int(sys.argv[2]) * 1024 * 1024 * 1024
sq_lba = 256 * 1024 * 1024 * 2000
line_count = 0
timestamp = 0
upper = (64 * 4 + 1024) * 256 * 1024 * 1024
low = 64 * 4 * 256 * 1024 * 1024
size = 1024 * 4

with open(filename, "w") as trace_f:

    while total_size > 0 :
        lba = randint(low, upper)
        timestep = randint(50, 200)

        if total_size - size * 512 < 0:
            break
        
        total_size -= size * 512
        print total_size

        timestamp +=timestep
        trace_f.write(str(timestamp) + " " + str(lba/512) + " " + str(size) + " 1\n")
        line_count += 1
 
        if line_count == 4:
            timestep = randint(10, 50)
            timestamp += timestep
            trace_f.write(str(timestamp) + " " + str(lba/512) + " " + str(size) + " 0\n")
            line_count += 1
        
        if line_count == 6:
            timestep = randint(10, 50)
            timestamp += timestep
            trace_f.write(str(timestamp) + " " + str(sq_lba/512) + " " + str(size) + " 1\n")
            line_count += 1
        
        if line_count == 8:
            timestep = randint(10, 50)
            timestamp += timestep
            trace_f.write(str(timestamp) + " " + str(sq_lba/512) + " " + str(size) + " 0\n")
            line_count += 1
        
        line_count = line_count % 10
