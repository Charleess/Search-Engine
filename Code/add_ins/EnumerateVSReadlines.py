import time

def with_enumerate():
    t = time.time()
    with open('tuples_iteration_0.csv', 'r') as f:
        for i, line in enumerate(f):
            if i == 20:
                t1 = time.time()
                print("Enumerate took {}".format(t1 - t))
                return line

def with_readlines():
    t = time.time()
    with open('tuples_iteration_0.csv', 'r') as f:
        lines = f.readlines()
        t1 = time.time()
        print("Readlines took {}".format(t1 - t))
        return lines[20]

if __name__ == '__main__':
    with_enumerate()
    with_readlines()

"""
Enumerate is the real shit here
"""