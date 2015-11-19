import time 

i = 0

for i in range(0,10):
    print i
    print "This prints once every five seconds."
    i =+ 1
    time.sleep(5)  # Delay for 1 minute (60 seconds)