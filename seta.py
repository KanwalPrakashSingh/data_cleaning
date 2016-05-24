import numbers
import math
import thread
import time
from multiprocessing.pool import Pool
from decimal import *
from settings import * 
### in my laptop the file wont get loaded in memory thats why solving it directly for batch processing 


def check_fields(line,noof_fields):
    return (len(line.split(" ")) == noof_fields)

###
# found at that there are 0 rows which have missing field, so data structure is clear
###
def check_fields_in_data():
    with open(FILE_NAME) as infile:
        count = 0
        for line in infile:
            if not check_fields(line,NO_OF_SERIES + 1):
                count = count + 1
        print count  

def calculate_mean_std(offset):
    """
    params - offset 
    the function reaches the offset and then calculates the mean / standard deviation
    """
    count = 0
    mean = []
    std = []
    squares = []
    dirty_data = []
    ### initialize
    for i in range(0,NO_OF_SERIES):
        mean.append(0)
        std.append(0)
        squares.append(0)
        dirty_data.append(0)
    ### only one line is read at a time, by the time next line is read the older one will be discarded
    with open(FILE_NAME) as infile:
        for line in infile:
            if count - offset == BATCH_SIZE:
                break
            elif count < offset: ## continue till you read the offset
                count = count + 1
                continue

            row = line.split(" ")
            for i in range(len(row)-1):
                value = float(row[i])
                if not math.isnan(value):
                    current_count = count - dirty_data[i] - offset #ignore all the NAN counts for a particular column
                    mean[i] = (mean[i]* current_count + value) / (current_count + 1)
                    squares[i] = (squares[i]*current_count + (value*value)) / (current_count+1)
                else:
                    dirty_data[i] += 1
            count = count + 1

    if PRINT:
        print "mean"
        print mean
        print "standard deviation"
        print squares
        print "unclean data - nan counts across different fields"
        print dirty_data

    return mean,squares,count-offset,dirty_data

def calculate_mean_std_parallel():
    """
    call this function to compute the mean, standard deviation and NaNs for each seies
    the file name, no of jobs can be changed in the settings file 
    """
    start = time.time()
    offsets = []
    instances = (MAX_ROWS/BATCH_SIZE)
    processes = Pool(processes=instances)
    for i in range(instances):
        offsets.append(i*BATCH_SIZE)
    print offsets
    result = processes.map(calculate_mean_std,offsets)
    processes.close()
    processes.join()
    mean = []
    std = []
    squares = []
    dirty_data = []
    #initializing
    for i in range(0,NO_OF_SERIES):
        mean.append(0)
        std.append(0)
        squares.append(0)
        dirty_data.append(0)

    total = 0
    ### here we combine the results from different processes / threads
    for r in result:
        for i in range(len(r[0])): ### update for each time series
            count = (r[2] - r[3][i])  ### actual count - the count with missing value
            mean[i] += r[0][i]*count
            squares[i] += r[1][i]*count
            dirty_data[i] += r[3][i]
        total += r[2]

    for i in range(len(mean)):
        mean[i] = 1.0*(mean[i])/(total - dirty_data[i])
        squares[i] = 1.0*(squares[i]) / (total - dirty_data[i])
        std[i] = math.sqrt(squares[i] - (mean[i]*mean[i]))
    end = time.time()
    print "######### MEAN ######### \n"
    print mean
    print "\n ######### STANDARD DEVIATION ######### \n"
    print std
    print "\n######### NAN ROWS COUNT #########\n"
    print dirty_data
    print "\n######### EXECUTION TIME #########\n"
    print (end-start)

    return mean,std


def get_correlation(obj):
    """
    index 0 - s1 time series 1 
    index 1 - s2 time series 2
    index 2 - mean list of each of the time series
    index 3 - std standard deviation list of each of the time series
    index 4 - offset where  the program should start calculating the correlation
    getting correlation between series s1 and s2
    """
    s1,s2,mean,std,offset = obj
    pearson_corr = 0
    count = 0
    with open(FILE_NAME) as infile:
        for line in infile:
            if count - offset == BATCH_SIZE:
                break
            elif count < offset: ## continue till you read the offset
                count = count + 1
                continue
            row = line.split(" ")
            val1 = float(row[s1])
            val2 = float(row[s2])
            if math.isnan(val1) or math.isnan(val2): #skip if any one of the two values has missing / nan values
                continue
            current_count = count - offset 
            pearson_corr = 1.0 * ( pearson_corr*current_count + ((val1 - mean[s1])*(val2 - mean[s2]))) / (current_count+1)
            count = count + 1

    pearson_corr = pearson_corr*1.0 / (std[s1]*std[s2])
    return pearson_corr,count

def get_correlation_parallel(s1,s2):
    """
    params s1 - series 1
    params s2 - series 2 
    NOTE : series are number 0 to 24 when giving in arguments
    returns the correlation between series
    """
    start = time.time()
    offsets = [] #this will be the arguments to all the parallel jobs
    instances = (MAX_ROWS/BATCH_SIZE)
    mean,std = calculate_mean_std_parallel()
    processes = Pool(processes=instances)
    for i in range(instances):
        offsets.append((s1,s2,mean,std,i*BATCH_SIZE))
    results = processes.map(get_correlation,offsets)
    processes.close()
    processes.join()
    pearson_corr = 0
    total = 0
    for result in results:
        pearson_corr += result[0]*result[1]
        total += result[1]
    pearson_corr = 1.0*pearson_corr / total
    #t_value = pearson_corr*math.sqrt( (total - 2) / ( 1 - (pearson_corr*pearson_corr)))
    print "\n ######### CORRELATION BETWEEN SERIES ",s1+1," AND SERIES ",s2+1, " is ",pearson_corr ,"######### \n" 
    end = time.time()
    print "EXECUTION TIME : ", end-start , " sec"
    return pearson_corr

def get_max_min(filename):
    """
    returns the max and min values for each of the series
    """
    series_max = []
    series_min = []
    for i in range(0,NO_OF_SERIES):
        series_min.append(0)
        series_max.append(0)
    ### only one line is read at a time, by the time next line is read the older one will be discarded
    with open(filename) as infile:
        for line in infile:
            row = line.split(" ")
            for i in range(len(row) - 1):
                val = float(row[i])
                if val > series_max[i]:
                    series_max[i] = val
                if val < series_min[i]:
                    series_min[i] = val

    return series_max,series_min

#print get_max_min(FILE_NAME)
#calculate_mean_std_parallel()
get_correlation_parallel(1,2) # series 1 is series 2 , index starts from 0 



