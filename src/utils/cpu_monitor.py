import psutil
import time
import csv
import sys

try:
    with open('cpu_log.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'cpu1', 'cpu2'])
        
        while True:
            # 0.5s interval to capture peaks
            per_cpu = psutil.cpu_percent(interval=0.5, percpu=True)
            if len(per_cpu) >= 2:
                writer.writerow([time.time(), per_cpu[0], per_cpu[1]])
                f.flush()
except Exception as e:
    print(e)
