

def timer_decorator(f):
    import time
    import datetime as dt
    import logging

    def timer(*args, **kwargs):
        t = time.time()
        logging.debug("Timer Started for Function: {},{} :{}".format(__name__, f.__name__, dt.datetime.now()))
        f(*args, **kwargs)
        time_delta = time.time() - t
        logging.debug('{} {} Took: {} Seconds'.format(__name__, f.__name__, time_delta))
        return f(*args, **kwargs)
    return timer


def print_array_matrix(array, limit_rows=None, header_sizes=None, justify="r", fill_string=" "):
    for row in array:
        line = ""
        jj = 0
        for column in row:
            if justify == "r":
                line += (str(column).rjust(header_sizes[jj] + 1, fill_string))
            elif justify == "c":
                line += (str(column).center(header_sizes[jj] + 1, fill_string))
            else:
                line += (str(column).ljust(header_sizes[jj] + 1, fill_string))

            jj += 1
        print(line)

    # this funtion will iterate through a matrix up the limit
    # and return the max/min size for each column


def size_array_matrix(array, limit_rows=None, min_max="MAX"):
    header = []
    headermax = []
    headermin = []
    #[25, 8, 1, 8, 9, 26, 26]
    ii = 0
    jj = 0
    y = 0
    x = 0
    for row in array:
        ii += 1

        if ii == 1:

            for column in row:
                y = len(str(column))
                header.append(y)
            headermax = header.copy()
            headermin = header.copy()

        else:
            jj = 0

            for column in row:
                x_max = headermax[jj]
                x_min = headermin[jj]
                y = len(str(column))

                if y > x_max:
                    headermax[jj] = y

                if y < x_min:
                    headermin[jj] = y
                jj += 1

    if min_max == "MAX":
        header = headermax.copy()
    else:
        header = headermin.copy()
    return header
